import os
import hashlib
import re
import signal
import sqlite3
import subprocess
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urlparse
from urllib.request import Request as UrlRequest, urlopen

from fastapi import FastAPI, HTTPException, UploadFile

from app.models.schemas import RestartJobRequest, RestreamStartRequest


APP_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = Path(os.getenv("RESTREAM_DATA_DIR", "/data"))
UPLOAD_DIR = Path(os.getenv("RESTREAM_UPLOAD_DIR", str(BASE_DIR / "videos")))
UPLOAD_STAGING_DIR = UPLOAD_DIR / ".uploads"
FRAME_DIR = Path(os.getenv("RESTREAM_FRAME_DIR", str(BASE_DIR / "frames")))
DB_PATH = Path(os.getenv("RESTREAM_DB_PATH", str(BASE_DIR / "restream.db")))
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.getenv("FFPROBE_BIN", "ffprobe")
LIBRARY_DIRS = [
    Path(item.strip())
    for item in os.getenv("RESTREAM_LIBRARY_DIRS", "").split(",")
    if item.strip()
]

DEFAULT_PUBLISH_SCHEME = os.getenv("RESTREAM_PUBLISH_SCHEME", "rtsp")
DEFAULT_PUBLISH_HOST = os.getenv("RESTREAM_PUBLISH_HOST", "localhost")
DEFAULT_PUBLISH_PORT = int(os.getenv("RESTREAM_PUBLISH_PORT", "18554"))
DEFAULT_VIEW_SCHEME = os.getenv("RESTREAM_VIEW_SCHEME", DEFAULT_PUBLISH_SCHEME)
DEFAULT_VIEW_HOST = os.getenv("RESTREAM_VIEW_HOST", DEFAULT_PUBLISH_HOST)
DEFAULT_VIEW_PORT = int(os.getenv("RESTREAM_VIEW_PORT", str(DEFAULT_PUBLISH_PORT)))
DEFAULT_USERNAME = os.getenv("RESTREAM_USERNAME", "")
DEFAULT_PASSWORD = os.getenv("RESTREAM_PASSWORD", "")
STALL_TIMEOUT_SECONDS = int(os.getenv("RESTREAM_STALL_TIMEOUT_SECONDS", "120"))
AUTO_RETRY_DELAY_SECONDS = int(os.getenv("RESTREAM_AUTO_RETRY_DELAY_SECONDS", "10"))
AUTO_RETRY_MAX_RETRIES = int(os.getenv("RESTREAM_AUTO_RETRY_MAX_RETRIES", "0"))

PROFILE_COPY = "copy"
PROFILE_H264_LOW_LATENCY = "h264_low_latency"
ENCODING_PROFILES = {PROFILE_COPY, PROFILE_H264_LOW_LATENCY}
H264_PRESETS = {
    "ultrafast", "superfast", "veryfast", "faster", "fast", "medium",
    "slow", "slower", "veryslow",
}
DEFAULT_H264_CRF = 23
DEFAULT_H264_PRESET = "veryfast"
DEFAULT_H264_GOP = 30

SAFE_PATH_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,254}$")
TIME_PATTERN = re.compile(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)")
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov", ".avi", ".ts", ".m4v", ".webm"}
ACTIVE_STATUSES = ("PENDING", "RUNNING", "STOPPING")
RESUMABLE_STATUSES = ("PENDING", "RUNNING")


class RuntimeProcess:
    def __init__(self, process: subprocess.Popen):
        self.process = process


runtime_processes: dict[str, RuntimeProcess] = {}
runtime_lock = threading.Lock()
job_action_lock = threading.Lock()
video_registry_lock = threading.Lock()
download_lock = threading.Lock()
upload_lock = threading.Lock()
shutdown_event = threading.Event()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_storage() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    FRAME_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS videos (
                id TEXT PRIMARY KEY,
                original_name TEXT NOT NULL,
                path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                duration_seconds REAL,
                content_hash TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                video_id TEXT NOT NULL,
                camera_code TEXT NOT NULL,
                camera_name TEXT,
                camera_ip TEXT,
                rtsp_path TEXT NOT NULL,
                publish_url TEXT NOT NULL,
                view_url TEXT NOT NULL,
                duration_seconds REAL,
                out_time_seconds REAL DEFAULT 0,
                progress_percent REAL DEFAULT 0,
                status TEXT NOT NULL,
                exit_code INTEGER,
                last_error TEXT,
                rtsp_transport TEXT DEFAULT 'tcp',
                loop INTEGER DEFAULT 0,
                auto_retry INTEGER DEFAULT 1,
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 0,
                encoding_profile TEXT DEFAULT 'copy',
                h264_crf INTEGER DEFAULT 23,
                h264_preset TEXT DEFAULT 'veryfast',
                h264_gop INTEGER DEFAULT 30,
                source_url TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                ended_at TEXT
            )
            """
        )
        existing_job_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        if "rtsp_transport" not in existing_job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN rtsp_transport TEXT DEFAULT 'tcp'")
        if "loop" not in existing_job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN loop INTEGER DEFAULT 0")
        if "auto_retry" not in existing_job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN auto_retry INTEGER DEFAULT 1")
        if "retry_count" not in existing_job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN retry_count INTEGER DEFAULT 0")
        if "max_retries" not in existing_job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN max_retries INTEGER DEFAULT 0")
        if "encoding_profile" not in existing_job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN encoding_profile TEXT DEFAULT 'copy'")
        if "h264_crf" not in existing_job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN h264_crf INTEGER DEFAULT 23")
        if "h264_preset" not in existing_job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN h264_preset TEXT DEFAULT 'veryfast'")
        if "h264_gop" not in existing_job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN h264_gop INTEGER DEFAULT 30")
        if "source_url" not in existing_job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN source_url TEXT")
        existing_video_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(videos)").fetchall()
        }
        if "content_hash" not in existing_video_columns:
            conn.execute("ALTER TABLE videos ADD COLUMN content_hash TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_content_hash ON videos(content_hash)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS video_file_cache (
                path TEXT PRIMARY KEY,
                size_bytes INTEGER NOT NULL,
                modified_at_ns INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                action TEXT NOT NULL,
                message TEXT NOT NULL,
                out_time_seconds REAL,
                progress_percent REAL,
                status TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS downloads (
                id TEXT PRIMARY KEY,
                source_url TEXT NOT NULL,
                original_name TEXT NOT NULL,
                status TEXT NOT NULL,
                total_bytes INTEGER,
                downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                video_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_created_at ON downloads(created_at DESC)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS uploads (
                id TEXT PRIMARY KEY,
                client_key TEXT NOT NULL UNIQUE,
                original_name TEXT NOT NULL,
                target_path TEXT NOT NULL,
                total_bytes INTEGER NOT NULL,
                received_bytes INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                video_id TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT
            )
            """
        )


def is_video_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS


def file_content_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def cached_file_content_hash(path: Path) -> str:
    stat = path.stat()
    cache = db_fetchone("SELECT * FROM video_file_cache WHERE path = ?", (str(path),))
    if (
        cache
        and cache["size_bytes"] == stat.st_size
        and cache["modified_at_ns"] == stat.st_mtime_ns
    ):
        return cache["content_hash"]
    content_hash = file_content_hash(path)
    db_execute(
        """
        INSERT INTO video_file_cache(path, size_bytes, modified_at_ns, content_hash, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            size_bytes = excluded.size_bytes,
            modified_at_ns = excluded.modified_at_ns,
            content_hash = excluded.content_hash,
            updated_at = excluded.updated_at
        """,
        (str(path), stat.st_size, stat.st_mtime_ns, content_hash, utc_now()),
    )
    return content_hash


def backfill_video_hash(row: sqlite3.Row) -> Optional[str]:
    if row["content_hash"]:
        return row["content_hash"]
    path = Path(row["path"])
    if not path.exists():
        return None
    content_hash = cached_file_content_hash(path)
    db_execute("UPDATE videos SET content_hash = ? WHERE id = ?", (content_hash, row["id"]))
    return content_hash


def merge_duplicate_video_records() -> int:
    merged = 0
    candidates = db_fetchall(
        """
        SELECT original_name, size_bytes
        FROM videos
        GROUP BY original_name, size_bytes
        HAVING COUNT(*) > 1
        """
    )
    for candidate in candidates:
        rows = db_fetchall(
            """
            SELECT * FROM videos
            WHERE original_name = ? AND size_bytes = ?
            ORDER BY created_at ASC
            """,
            (candidate["original_name"], candidate["size_bytes"]),
        )
        by_hash: dict[str, sqlite3.Row] = {}
        for row in rows:
            content_hash = backfill_video_hash(row)
            if not content_hash:
                continue
            canonical = by_hash.get(content_hash)
            if not canonical:
                by_hash[content_hash] = row
                continue
            db_execute("UPDATE jobs SET video_id = ? WHERE video_id = ?", (canonical["id"], row["id"]))
            db_execute("DELETE FROM videos WHERE id = ?", (row["id"],))
            merged += 1
    return merged


def scan_video_libraries() -> int:
    imported = 0
    candidates = [UPLOAD_DIR, *LIBRARY_DIRS]
    for directory in candidates:
        if not directory.exists() or not directory.is_dir():
            continue
        for path in directory.rglob("*"):
            if not is_video_file(path):
                continue
            absolute_path = path.resolve()
            existing = db_fetchone("SELECT id FROM videos WHERE path = ?", (str(absolute_path),))
            if existing:
                continue
            content_hash = cached_file_content_hash(absolute_path)
            duplicate = db_fetchone("SELECT id FROM videos WHERE content_hash = ?", (content_hash,))
            if duplicate:
                continue
            video_id = str(uuid.uuid5(uuid.NAMESPACE_URL, str(absolute_path)))
            duration = get_duration_seconds(absolute_path)
            db_execute(
                """
                INSERT OR IGNORE INTO videos(
                    id, original_name, path, size_bytes, duration_seconds, content_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    video_id,
                    absolute_path.name,
                    str(absolute_path),
                    absolute_path.stat().st_size,
                    duration,
                    content_hash,
                    utc_now(),
                ),
            )
            imported += 1
    return imported


def prune_missing_videos() -> int:
    pruned = 0
    rows = db_fetchall(
        """
        SELECT videos.id, videos.path, COUNT(jobs.id) AS job_count
        FROM videos
        LEFT JOIN jobs ON jobs.video_id = videos.id
        GROUP BY videos.id
        """
    )
    for row in rows:
        if row["job_count"] > 0:
            continue
        if is_remote_source(row["path"]) or row["path"].startswith("s3://"):
            continue
        if Path(row["path"]).exists():
            continue
        db_execute("DELETE FROM videos WHERE id = ?", (row["id"],))
        pruned += 1
    return pruned


def db_execute(query: str, params: tuple = ()) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(query, params)


def db_fetchone(query: str, params: tuple = ()) -> Optional[sqlite3.Row]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(query, params).fetchone()


def db_fetchall(query: str, params: tuple = ()) -> list[sqlite3.Row]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(query, params).fetchall()


def row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row)


def video_row_to_dict(row: sqlite3.Row) -> dict:
    item = dict(row)
    if is_remote_source(item["path"]) or item["path"].startswith("s3://"):
        item["source_type"] = "s3"
        item["path"] = None
        item["modified_at"] = item["created_at"]
        return item
    try:
        modified_at = datetime.fromtimestamp(Path(item["path"]).stat().st_mtime, timezone.utc).isoformat()
    except Exception:
        modified_at = item["created_at"]
    item["modified_at"] = modified_at
    return item


def job_row_to_dict(row: sqlite3.Row) -> dict:
    item = dict(row)
    source_url = item.pop("source_url", None)
    video = {
        "id": item.pop("video_id_joined", None),
        "original_name": item.pop("video_original_name", None),
        "path": item.pop("video_path", None),
        "size_bytes": item.pop("video_size_bytes", None),
        "duration_seconds": item.pop("video_duration_seconds", None),
        "created_at": item.pop("video_created_at", None),
    }
    if source_url:
        video["path"] = None
    item["source_type"] = "s3" if source_url else "local"
    item["video"] = video if video["id"] else None
    return item


def jobs_with_video_query(where_clause: str = "") -> str:
    return f"""
    SELECT
        jobs.*,
        videos.id AS video_id_joined,
        videos.original_name AS video_original_name,
        videos.path AS video_path,
        videos.size_bytes AS video_size_bytes,
        videos.duration_seconds AS video_duration_seconds,
        videos.created_at AS video_created_at
    FROM jobs
    LEFT JOIN videos ON videos.id = jobs.video_id
    {where_clause}
    """


def get_active_job_for_video(video_id: str) -> Optional[sqlite3.Row]:
    placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
    return db_fetchone(
        f"""
        SELECT id, status, rtsp_path, view_url
        FROM jobs
        WHERE video_id = ? AND status IN ({placeholders})
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (video_id, *ACTIVE_STATUSES),
    )


def assert_video_has_no_active_job(video_id: str) -> None:
    active_job = get_active_job_for_video(video_id)
    if active_job:
        raise HTTPException(
            status_code=409,
            detail=(
                "This video already has an active restream job "
                f"({active_job['status']}): {active_job['id']}. Stop it before starting another process."
            ),
        )


def assert_s3_source_has_no_active_job(source_url: str) -> None:
    placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
    active_job = db_fetchone(
        f"""
        SELECT id, status, rtsp_path
        FROM jobs
        WHERE source_url = ? AND status IN ({placeholders})
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (source_url, *ACTIVE_STATUSES),
    )
    if active_job:
        raise HTTPException(
            status_code=409,
            detail=(
                "This S3 source already has an active restream job "
                f"({active_job['status']}): {active_job['id']}. Stop it before starting another process."
            ),
        )


def resume_active_jobs() -> None:
    placeholders = ",".join("?" for _ in RESUMABLE_STATUSES)
    rows = db_fetchall(
        f"""
        SELECT jobs.*, videos.path AS video_path
        FROM jobs
        LEFT JOIN videos ON videos.id = jobs.video_id
        WHERE jobs.status IN ({placeholders})
        ORDER BY jobs.created_at ASC
        """,
        RESUMABLE_STATUSES,
    )
    for row in rows:
        source_ref = row["source_url"] or row["video_path"] or ""
        if not row["source_url"] and not Path(source_ref).exists():
            db_execute(
                "UPDATE jobs SET status = ?, last_error = ?, ended_at = ? WHERE id = ?",
                ("FAILED", "Cannot resume because video file is missing", utc_now(), row["id"]),
            )
            add_log(row["id"], "SYSTEM", "Cannot resume because video file is missing")
            continue
        offset = float(row["out_time_seconds"] or 0)
        transport = row["rtsp_transport"] or "tcp"
        loop = bool(row["loop"])
        if loop and row["duration_seconds"]:
            offset %= float(row["duration_seconds"])
        add_log(row["id"], "SYSTEM", f"Resuming restream from {offset:.2f}s after application startup")
        try:
            start_ffmpeg_job(
                row["id"],
                resolve_s3_source(source_ref) if row["source_url"] else Path(source_ref),
                row["publish_url"],
                transport,
                loop,
                row["duration_seconds"],
                start_offset_seconds=offset,
                encoding_profile=row["encoding_profile"] or PROFILE_COPY,
                h264_crf=int(row["h264_crf"] or DEFAULT_H264_CRF),
                h264_preset=row["h264_preset"] or DEFAULT_H264_PRESET,
                h264_gop=int(row["h264_gop"] or DEFAULT_H264_GOP),
            )
        except FileNotFoundError:
            db_execute(
                "UPDATE jobs SET status = ?, last_error = ?, ended_at = ? WHERE id = ?",
                ("FAILED", f"{FFMPEG_BIN} not found", utc_now(), row["id"]),
            )
            add_log(row["id"], "SYSTEM", f"Cannot resume because {FFMPEG_BIN} was not found")
        except HTTPException as error:
            db_execute(
                "UPDATE jobs SET status = ?, last_error = ?, ended_at = ? WHERE id = ?",
                ("FAILED", str(error.detail), utc_now(), row["id"]),
            )
            add_event(row["id"], "RESUME_FAILED", str(error.detail), offset)


async def save_uploaded_video(file: UploadFile) -> dict:
    if not file.filename:
        raise HTTPException(status_code=400, detail="filename is required")
    video_id = str(uuid.uuid4())
    safe_parts = [
        part.replace("/", "_").replace("\\", "_")
        for part in Path(file.filename.replace("\\", "/")).parts
        if part not in {"", ".", ".."}
    ]
    safe_name = Path(*safe_parts).as_posix() if safe_parts else f"{video_id}.video"
    target_path = UPLOAD_DIR / f"{video_id}_{safe_name}"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    size = 0
    with target_path.open("wb") as output:
        while chunk := await file.read(1024 * 1024):
            size += len(chunk)
            output.write(chunk)
    duration = get_duration_seconds(target_path)
    content_hash = file_content_hash(target_path)
    with video_registry_lock:
        duplicate = db_fetchone("SELECT * FROM videos WHERE content_hash = ?", (content_hash,))
        if duplicate:
            target_path.unlink(missing_ok=True)
            item = video_row_to_dict(duplicate)
            item["duplicate"] = True
            return item
        db_execute(
            """
            INSERT INTO videos(id, original_name, path, size_bytes, duration_seconds, content_hash, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (video_id, safe_name, str(target_path), size, duration, content_hash, utc_now()),
        )
    return {
        "id": video_id,
        "original_name": safe_name,
        "size_bytes": size,
        "duration_seconds": duration,
        "duplicate": False,
    }


def safe_upload_name(original_name: str, fallback: str) -> str:
    parts = [
        part.replace("/", "_").replace("\\", "_")
        for part in Path(original_name.replace("\\", "/")).parts
        if part not in {"", ".", ".."}
    ]
    return Path(*parts).as_posix() if parts else fallback


def upload_row_to_dict(row: sqlite3.Row) -> dict:
    item = dict(row)
    item.pop("target_path", None)
    item.pop("client_key", None)
    return item


def init_resumable_upload(original_name: str, total_bytes: int, client_key: str) -> dict:
    with upload_lock:
        existing = db_fetchone("SELECT * FROM uploads WHERE client_key = ?", (client_key,))
        if existing:
            if existing["total_bytes"] != total_bytes or existing["original_name"] != original_name:
                raise HTTPException(status_code=409, detail="Upload key conflicts with a different file")
            return upload_row_to_dict(existing)
        upload_id = str(uuid.uuid4())
        safe_name = safe_upload_name(original_name, f"{upload_id}.video")
        target_path = UPLOAD_STAGING_DIR / f"{upload_id}_{safe_name}.part"
        target_path.parent.mkdir(parents=True, exist_ok=True)
        now = utc_now()
        db_execute(
            """
            INSERT INTO uploads(
                id, client_key, original_name, target_path, total_bytes, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'UPLOADING', ?, ?)
            """,
            (upload_id, client_key, original_name, str(target_path), total_bytes, now, now),
        )
    row = db_fetchone("SELECT * FROM uploads WHERE id = ?", (upload_id,))
    return upload_row_to_dict(row)


def append_upload_chunk(upload_id: str, offset: int, payload: bytes) -> dict:
    if not payload:
        raise HTTPException(status_code=400, detail="Upload chunk is empty")
    with upload_lock:
        row = db_fetchone("SELECT * FROM uploads WHERE id = ?", (upload_id,))
        if not row:
            raise HTTPException(status_code=404, detail="upload not found")
        if row["status"] == "COMPLETED":
            return upload_row_to_dict(row)
        if row["status"] != "UPLOADING":
            raise HTTPException(status_code=409, detail=f"upload is {row['status'].lower()}")
        path = Path(row["target_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        actual_offset = path.stat().st_size if path.exists() else 0
        if actual_offset != offset:
            db_execute("UPDATE uploads SET received_bytes = ?, updated_at = ? WHERE id = ?", (actual_offset, utc_now(), upload_id))
            raise HTTPException(status_code=409, detail=f"Expected chunk offset {actual_offset}")
        next_offset = actual_offset + len(payload)
        if next_offset > row["total_bytes"]:
            raise HTTPException(status_code=400, detail="Upload chunk exceeds declared file size")
        with path.open("ab") as output:
            output.write(payload)
        db_execute(
            "UPDATE uploads SET received_bytes = ?, updated_at = ? WHERE id = ?",
            (next_offset, utc_now(), upload_id),
        )
    updated = db_fetchone("SELECT * FROM uploads WHERE id = ?", (upload_id,))
    return upload_row_to_dict(updated)


def complete_resumable_upload(upload_id: str) -> dict:
    with upload_lock:
        row = db_fetchone("SELECT * FROM uploads WHERE id = ?", (upload_id,))
        if not row:
            raise HTTPException(status_code=404, detail="upload not found")
        if row["status"] == "COMPLETED":
            return upload_row_to_dict(row)
        path = Path(row["target_path"])
        actual_size = path.stat().st_size if path.exists() else 0
        if actual_size != row["total_bytes"]:
            raise HTTPException(status_code=409, detail=f"Upload incomplete: {actual_size}/{row['total_bytes']} bytes")
        video_id = str(uuid.uuid4())
        safe_name = safe_upload_name(row["original_name"], f"{video_id}.video")
        target_path = UPLOAD_DIR / f"{video_id}_{safe_name}"
        target_path.parent.mkdir(parents=True, exist_ok=True)
        path.replace(target_path)
        try:
            duration = get_duration_seconds(target_path)
            content_hash = file_content_hash(target_path)
            with video_registry_lock:
                duplicate = db_fetchone("SELECT * FROM videos WHERE content_hash = ?", (content_hash,))
                if duplicate:
                    target_path.unlink(missing_ok=True)
                    video_id = duplicate["id"]
                else:
                    db_execute(
                        """
                        INSERT INTO videos(id, original_name, path, size_bytes, duration_seconds, content_hash, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (video_id, safe_name, str(target_path), actual_size, duration, content_hash, utc_now()),
                    )
            db_execute(
                """
                UPDATE uploads
                SET status = 'COMPLETED', received_bytes = ?, video_id = ?, completed_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (actual_size, video_id, utc_now(), utc_now(), upload_id),
            )
        except Exception as error:
            db_execute(
                "UPDATE uploads SET status = 'FAILED', error = ?, updated_at = ? WHERE id = ?",
                (str(error)[-1000:], utc_now(), upload_id),
            )
            raise HTTPException(status_code=500, detail="Failed to finalize uploaded video")
    completed = db_fetchone("SELECT * FROM uploads WHERE id = ?", (upload_id,))
    return upload_row_to_dict(completed)


def add_log(job_id: str, level: str, message: str) -> None:
    trimmed = re.sub(r"https?://[^\s\"']+", "[remote-url-hidden]", message.strip())
    if not trimmed:
        return
    db_execute(
        "INSERT INTO logs(job_id, created_at, level, message) VALUES (?, ?, ?, ?)",
        (job_id, utc_now(), level, trimmed[-4000:]),
    )


def add_event(
    job_id: str,
    action: str,
    message: str,
    out_time_seconds: Optional[float] = None,
) -> None:
    trimmed = message.strip()
    if not trimmed:
        return
    job = db_fetchone("SELECT out_time_seconds, progress_percent, status FROM jobs WHERE id = ?", (job_id,))
    event_out_time = out_time_seconds
    progress_percent = None
    status = None
    if job:
        if event_out_time is None:
            event_out_time = job["out_time_seconds"]
        progress_percent = job["progress_percent"]
        status = job["status"]
    db_execute(
        """
        INSERT INTO event_logs(job_id, created_at, action, message, out_time_seconds, progress_percent, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (job_id, utc_now(), action, trimmed[-4000:], event_out_time, progress_percent, status),
    )
    add_log(job_id, "EVENT", f"{action}: {trimmed}")


def get_duration_seconds(path: Path) -> Optional[float]:
    try:
        result = subprocess.run(
            [
                FFPROBE_BIN,
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=True,
        )
        duration = float(result.stdout.strip())
        return duration if duration > 0 else None
    except Exception:
        return None


def is_remote_source(value: str) -> bool:
    return value.startswith("https://") or value.startswith("http://")


def resolve_s3_source(source: str) -> str:
    normalized = source.strip()
    if is_remote_source(normalized):
        return normalized
    parsed = urlparse(normalized)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise HTTPException(status_code=400, detail="S3 source must be s3://bucket/key or an HTTPS presigned URL")
    try:
        import boto3

        return boto3.client("s3").generate_presigned_url(
            "get_object",
            Params={"Bucket": parsed.netloc, "Key": parsed.path.lstrip("/")},
            ExpiresIn=86400,
        )
    except Exception as error:
        raise HTTPException(status_code=400, detail=f"Cannot create S3 download URL: {error}")


def source_display_name(source: str) -> str:
    parsed = urlparse(source)
    name = Path(parsed.path).name
    return name or "S3 video"


def download_row_to_dict(row: sqlite3.Row) -> dict:
    item = dict(row)
    item.pop("source_url", None)
    return item


def get_download(download_id: str) -> Optional[dict]:
    row = db_fetchone("SELECT * FROM downloads WHERE id = ?", (download_id,))
    return download_row_to_dict(row) if row else None


def list_downloads() -> list[dict]:
    rows = db_fetchall("SELECT * FROM downloads ORDER BY created_at DESC LIMIT 50")
    return [download_row_to_dict(row) for row in rows]


def update_download(download_id: str, **changes: object) -> None:
    if not changes:
        return
    changes["updated_at"] = utc_now()
    fields = ", ".join(f"{key} = ?" for key in changes)
    db_execute(
        f"UPDATE downloads SET {fields} WHERE id = ?",
        (*changes.values(), download_id),
    )


def download_source_to_library(download_id: str, source_url: str, original_name: str) -> None:
    target_path = UPLOAD_DIR / f"{download_id}_{original_name}"
    partial_path = target_path.with_suffix(target_path.suffix + ".part")
    try:
        if target_path.exists():
            update_download(download_id, status="PROCESSING", downloaded_bytes=target_path.stat().st_size)
            finalize_downloaded_video(download_id, target_path, original_name)
            return
        resolved_url = resolve_s3_source(source_url)
        resume_at = partial_path.stat().st_size if partial_path.exists() else 0
        headers = {"User-Agent": "RTSP-Restream-Tool/1.0"}
        if resume_at:
            headers["Range"] = f"bytes={resume_at}-"
        request = UrlRequest(resolved_url, headers=headers)
        update_download(download_id, status="DOWNLOADING")
        with urlopen(request, timeout=30) as response:
            if resume_at and getattr(response, "status", None) != 206:
                resume_at = 0
            mode = "ab" if resume_at else "wb"
            with partial_path.open(mode) as output:
                total_header = response.headers.get("Content-Length")
                try:
                    response_bytes = int(total_header) if total_header else None
                except ValueError:
                    response_bytes = None
                total_bytes = (resume_at + response_bytes) if response_bytes is not None else None
                update_download(download_id, total_bytes=total_bytes, downloaded_bytes=resume_at)
                downloaded_bytes = resume_at
                last_reported_at = 0.0
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
                    downloaded_bytes += len(chunk)
                    now = time.monotonic()
                    if now - last_reported_at >= 0.5:
                        update_download(download_id, downloaded_bytes=downloaded_bytes)
                        last_reported_at = now
        update_download(download_id, status="PROCESSING", downloaded_bytes=downloaded_bytes)
        partial_path.replace(target_path)
        finalize_downloaded_video(download_id, target_path, original_name)
    except Exception as error:
        update_download(download_id, status="FAILED", error=str(error)[-1000:])


def finalize_downloaded_video(download_id: str, target_path: Path, original_name: str) -> None:
    try:
        downloaded_bytes = target_path.stat().st_size
        duration = get_duration_seconds(target_path)
        content_hash = file_content_hash(target_path)
        with video_registry_lock:
            duplicate = db_fetchone("SELECT * FROM videos WHERE content_hash = ?", (content_hash,))
            if duplicate:
                target_path.unlink(missing_ok=True)
                update_download(
                    download_id,
                    status="COMPLETED",
                    video_id=duplicate["id"],
                    completed_at=utc_now(),
                    downloaded_bytes=downloaded_bytes,
                )
                return
            video_id = str(uuid.uuid4())
            db_execute(
                """
                INSERT INTO videos(id, original_name, path, size_bytes, duration_seconds, content_hash, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (video_id, original_name, str(target_path), downloaded_bytes, duration, content_hash, utc_now()),
            )
        update_download(
            download_id,
            status="COMPLETED",
            video_id=video_id,
            completed_at=utc_now(),
            downloaded_bytes=downloaded_bytes,
        )
    except Exception as error:
        update_download(download_id, status="FAILED", error=str(error)[-1000:])


def start_s3_download(source: str) -> dict:
    source_url = source.strip()
    if not source_url:
        raise HTTPException(status_code=400, detail="S3 source is required")
    if not is_remote_source(source_url) and not source_url.startswith("s3://"):
        raise HTTPException(status_code=400, detail="S3 source must be s3://bucket/key or an HTTPS URL")
    with download_lock:
        existing = db_fetchone(
            """
            SELECT * FROM downloads
            WHERE source_url = ? AND status IN ('PENDING', 'DOWNLOADING', 'PROCESSING')
            ORDER BY created_at DESC LIMIT 1
            """,
            (source_url,),
        )
        if existing:
            return download_row_to_dict(existing)
        download_id = str(uuid.uuid4())
        original_name = source_display_name(source_url)
        now = utc_now()
        db_execute(
            """
            INSERT INTO downloads(id, source_url, original_name, status, created_at, updated_at)
            VALUES (?, ?, ?, 'PENDING', ?, ?)
            """,
            (download_id, source_url, original_name, now, now),
        )
    return get_download(download_id) or {"id": download_id, "status": "PENDING"}


def resume_download(download_id: str) -> dict:
    with download_lock:
        row = db_fetchone("SELECT * FROM downloads WHERE id = ?", (download_id,))
        if not row:
            raise HTTPException(status_code=404, detail="download not found")
        if row["status"] == "COMPLETED":
            return download_row_to_dict(row)
        if row["status"] in {"PENDING", "DOWNLOADING", "PROCESSING"}:
            return download_row_to_dict(row)
        update_download(download_id, status="PENDING", error=None)
    return get_download(download_id) or {"id": download_id, "status": "PENDING"}


def recover_download_queue() -> None:
    db_execute(
        """
        UPDATE downloads
        SET status = 'PENDING', error = NULL, updated_at = ?
        WHERE status IN ('DOWNLOADING', 'PROCESSING')
           OR (status = 'FAILED' AND error = 'Container restarted before download finished')
        """,
        (utc_now(),),
    )


def claim_next_download() -> Optional[sqlite3.Row]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM downloads WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        updated = conn.execute(
            "UPDATE downloads SET status = 'DOWNLOADING', error = NULL, updated_at = ? WHERE id = ? AND status = 'PENDING'",
            (utc_now(), row["id"]),
        )
        return row if updated.rowcount == 1 else None


def run_download_worker(poll_seconds: float = 2.0) -> None:
    init_storage()
    recover_download_queue()
    while True:
        download = claim_next_download()
        if not download:
            time.sleep(poll_seconds)
            continue
        download_source_to_library(download["id"], download["source_url"], download["original_name"])


def capture_job_frame(job_id: str) -> Path:
    job = db_fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    video = db_fetchone("SELECT * FROM videos WHERE id = ?", (job["video_id"],))
    if not video:
        raise HTTPException(status_code=404, detail="video not found")
    input_source = resolve_s3_source(job["source_url"]) if job["source_url"] else video["path"]
    if not is_remote_source(input_source) and not Path(input_source).exists():
        raise HTTPException(status_code=404, detail="video file is missing on disk")
    offset = max(0.0, float(job["out_time_seconds"] or 0))
    frame_path = FRAME_DIR / f"{job_id}.jpg"
    try:
        subprocess.run(
            [
                FFMPEG_BIN,
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                f"{offset:.3f}",
                "-i",
                input_source,
                "-frames:v",
                "1",
                "-q:v",
                "2",
                "-y",
                str(frame_path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"{FFMPEG_BIN} not found")
    except subprocess.CalledProcessError as error:
        message = error.stderr.strip() or error.stdout.strip() or "failed to capture frame"
        add_event(job_id, "FRAME_CAPTURE_FAILED", message, offset)
        raise HTTPException(status_code=500, detail=message)
    except subprocess.TimeoutExpired:
        message = "frame capture timed out"
        add_event(job_id, "FRAME_CAPTURE_FAILED", message, offset)
        raise HTTPException(status_code=504, detail=message)
    if not frame_path.exists():
        raise HTTPException(status_code=500, detail="frame file was not created")
    return frame_path


def validate_rtsp_path(rtsp_path: str) -> str:
    normalized = rtsp_path.strip().strip("/")
    if not SAFE_PATH_PATTERN.match(normalized):
        raise HTTPException(
            status_code=400,
            detail="rtsp_path only supports letters, numbers, dot, dash, underscore and slash",
        )
    if ".." in normalized.split("/"):
        raise HTTPException(status_code=400, detail="rtsp_path cannot contain '..'")
    return normalized


def build_rtsp_url(scheme: str, host: str, port: int, path: str, username: Optional[str], password: Optional[str]) -> str:
    auth = ""
    if username:
        auth = quote(username, safe="")
        if password:
            auth += f":{quote(password, safe='')}"
        auth += "@"
    return f"{scheme}://{auth}{host}:{port}/{path}"


def parse_ffmpeg_time(line: str) -> Optional[float]:
    match = TIME_PATTERN.search(line)
    if not match:
        return None
    hours, minutes, seconds = match.groups()
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def normalize_profile(profile: str, preset: str, crf: int, gop: int) -> tuple[str, str, int, int]:
    normalized_profile = profile.strip().lower()
    normalized_preset = preset.strip().lower()
    if normalized_profile not in ENCODING_PROFILES:
        raise HTTPException(status_code=400, detail="Unknown encoding profile")
    if normalized_preset not in H264_PRESETS:
        raise HTTPException(status_code=400, detail="Invalid H.264 preset")
    if not 0 <= crf <= 51:
        raise HTTPException(status_code=400, detail="H.264 CRF must be between 0 and 51")
    if not 1 <= gop <= 1000:
        raise HTTPException(status_code=400, detail="H.264 GOP must be between 1 and 1000")
    return normalized_profile, normalized_preset, crf, gop


def update_progress(
    job_id: str,
    out_time_seconds: float,
    duration_seconds: Optional[float],
    loop: bool = False,
) -> None:
    if not loop and duration_seconds and duration_seconds > 0:
        percent = min(100.0, max(0.0, out_time_seconds / duration_seconds * 100))
    else:
        percent = 0.0
    db_execute(
        "UPDATE jobs SET out_time_seconds = ?, progress_percent = ? WHERE id = ?",
        (out_time_seconds, percent, job_id),
    )


def terminate_stalled_process(job_id: str, process: subprocess.Popen, out_time_seconds: float) -> None:
    if process.poll() is not None:
        return
    message = (
        f"FFmpeg stalled: output timestamp stayed at {out_time_seconds:.2f}s "
        f"for {STALL_TIMEOUT_SECONDS}s"
    )
    db_execute(
        "UPDATE jobs SET status = ?, last_error = ? WHERE id = ?",
        ("FAILED", message, job_id),
    )
    add_event(job_id, "STALL_DETECTED", message, out_time_seconds)
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass


def read_stdout_progress(
    job_id: str,
    process: subprocess.Popen,
    duration_seconds: Optional[float],
    progress_base_seconds: float = 0,
    loop: bool = False,
) -> None:
    assert process.stdout is not None
    last_out_time: Optional[float] = None
    last_out_time_change = time.monotonic()
    for raw_line in process.stdout:
        line = raw_line.strip()
        if not line:
            continue
        current_out_time: Optional[float] = None
        if line.startswith("out_time_ms="):
            try:
                current_out_time = progress_base_seconds + int(line.split("=", 1)[1]) / 1_000_000
                update_progress(job_id, current_out_time, duration_seconds, loop)
            except ValueError:
                pass
        elif line.startswith("out_time="):
            parsed = parse_ffmpeg_time("time=" + line.split("=", 1)[1])
            if parsed is not None:
                current_out_time = progress_base_seconds + parsed
                update_progress(job_id, current_out_time, duration_seconds, loop)
        elif line == "progress=end":
            add_log(job_id, "PROGRESS", line)

        if current_out_time is None or STALL_TIMEOUT_SECONDS <= 0:
            continue
        if last_out_time is None or current_out_time > last_out_time + 0.001:
            last_out_time = current_out_time
            last_out_time_change = time.monotonic()
            continue
        if time.monotonic() - last_out_time_change >= STALL_TIMEOUT_SECONDS:
            terminate_stalled_process(job_id, process, current_out_time)
            return


def read_stderr_logs(
    job_id: str,
    process: subprocess.Popen,
    duration_seconds: Optional[float],
    progress_base_seconds: float = 0,
    loop: bool = False,
) -> None:
    assert process.stderr is not None
    last_logged_video_time = -60.0
    for raw_line in process.stderr:
        line = raw_line.strip()
        if not line:
            continue
        parsed = parse_ffmpeg_time(line)
        if parsed is not None:
            current_time = progress_base_seconds + parsed
            update_progress(job_id, current_time, duration_seconds, loop)
            if current_time - last_logged_video_time >= 60:
                add_log(job_id, "FFMPEG", line)
                last_logged_video_time = current_time
        else:
            add_log(job_id, "FFMPEG", line)


def monitor_process(
    job_id: str,
    process: subprocess.Popen,
    duration_seconds: Optional[float],
    progress_base_seconds: float = 0,
    loop: bool = False,
) -> None:
    stdout_thread = threading.Thread(
        target=read_stdout_progress,
        args=(job_id, process, duration_seconds, progress_base_seconds, loop),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=read_stderr_logs,
        args=(job_id, process, duration_seconds, progress_base_seconds, loop),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()
    exit_code = process.wait()
    stdout_thread.join(timeout=2)
    stderr_thread.join(timeout=2)

    row = db_fetchone("SELECT status FROM jobs WHERE id = ?", (job_id,))
    current_status = row["status"] if row else "UNKNOWN"
    if shutdown_event.is_set() and current_status in {"PENDING", "RUNNING"}:
        db_execute(
            "UPDATE jobs SET last_error = ? WHERE id = ?",
            ("Application shutting down; job will resume on next startup", job_id),
        )
        add_log(job_id, "SYSTEM", "Application is shutting down; keeping job resumable")
        with runtime_lock:
            runtime_processes.pop(job_id, None)
        return
    if current_status in {"STOPPED", "STOPPING"}:
        final_status = "STOPPED"
    elif exit_code == 0 and not loop:
        final_status = "COMPLETED"
        db_execute("UPDATE jobs SET progress_percent = 100 WHERE id = ?", (job_id,))
    elif exit_code == 0:
        final_status = "STOPPED"
    else:
        final_status = "FAILED"

    db_execute(
        "UPDATE jobs SET status = ?, exit_code = ?, ended_at = ? WHERE id = ?",
        (final_status, exit_code, utc_now(), job_id),
    )
    add_log(job_id, "SYSTEM", f"Restream finished with status={final_status}, exit_code={exit_code}")
    if final_status == "COMPLETED":
        add_event(job_id, "COMPLETED", "Restream completed")
    elif final_status == "FAILED":
        add_event(job_id, "FAILED", f"FFmpeg exited before completion with exit_code={exit_code}")
    elif final_status == "STOPPED":
        add_event(job_id, "STOPPED", "Restream stopped")
    with runtime_lock:
        runtime_processes.pop(job_id, None)
    if final_status == "FAILED":
        maybe_auto_retry_job(job_id)


def start_ffmpeg_job(
    job_id: str,
    video_path: str | Path,
    publish_url: str,
    transport: str,
    loop: bool,
    duration: Optional[float],
    start_offset_seconds: float = 0,
    encoding_profile: str = PROFILE_COPY,
    h264_crf: int = DEFAULT_H264_CRF,
    h264_preset: str = DEFAULT_H264_PRESET,
    h264_gop: int = DEFAULT_H264_GOP,
) -> None:
    encoding_profile, h264_preset, h264_crf, h264_gop = normalize_profile(
        encoding_profile, h264_preset, h264_crf, h264_gop
    )
    args = [FFMPEG_BIN, "-hide_banner", "-loglevel", "warning", "-nostdin", "-re"]
    if loop:
        args.extend(["-stream_loop", "-1"])
    if start_offset_seconds > 0:
        args.extend(["-ss", f"{start_offset_seconds:.3f}"])
    args.extend(["-i", str(video_path)])
    if encoding_profile == PROFILE_H264_LOW_LATENCY:
        args.extend(
            [
                "-c:v", "libx264",
                "-preset", h264_preset,
                "-tune", "zerolatency",
                "-crf", str(h264_crf),
                "-pix_fmt", "yuv420p",
                "-g", str(h264_gop),
                "-keyint_min", str(h264_gop),
                "-x264-params",
                f"keyint={h264_gop}:min-keyint={h264_gop}:scenecut=0:repeat-headers=1",
                "-an",
            ]
        )
    else:
        args.extend(["-c:v", "copy", "-c:a", "copy"])
    args.extend(["-f", "rtsp", "-rtsp_transport", transport, "-progress", "pipe:1", publish_url])
    process = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    with runtime_lock:
        runtime_processes[job_id] = RuntimeProcess(process)
    db_execute(
        "UPDATE jobs SET status = ?, started_at = ?, ended_at = NULL, exit_code = NULL, last_error = NULL WHERE id = ?",
        ("RUNNING", utc_now(), job_id),
    )
    logged_args = list(args[:-1])
    input_index = logged_args.index("-i") + 1
    if is_remote_source(str(video_path)):
        logged_args[input_index] = "[remote-source-hidden]"
    prefix = f"Started from offset {start_offset_seconds:.2f}s: " if start_offset_seconds > 0 else "Started: "
    add_log(job_id, "SYSTEM", prefix + " ".join(logged_args) + " [rtsp-url-hidden]")
    threading.Thread(
        target=monitor_process,
        args=(job_id, process, duration, start_offset_seconds, loop),
        daemon=True,
    ).start()


def terminate_runtime_process(job_id: str, timeout_seconds: int = 10) -> None:
    with runtime_lock:
        runtime = runtime_processes.get(job_id)
    if not runtime or runtime.process.poll() is not None:
        return
    try:
        os.killpg(runtime.process.pid, signal.SIGTERM)
        runtime.process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        os.killpg(runtime.process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def maybe_auto_retry_job(job_id: str) -> None:
    if shutdown_event.is_set():
        return
    job = db_fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))
    if not job or not job["auto_retry"]:
        return
    if job["status"] != "FAILED":
        return
    if not job["loop"] and job["progress_percent"] is not None and float(job["progress_percent"]) >= 100:
        return
    max_retries = int(job["max_retries"] or 0)
    retry_count = int(job["retry_count"] or 0)
    if max_retries > 0 and retry_count >= max_retries:
        add_event(job_id, "AUTO_RESUME_SKIPPED", f"Retry limit was reached ({retry_count}/{max_retries})")
        return
    video = db_fetchone("SELECT * FROM videos WHERE id = ?", (job["video_id"],))
    if not video:
        add_event(job_id, "AUTO_RESUME_SKIPPED", "Video metadata is missing")
        return
    source_ref = job["source_url"] or video["path"]
    if not job["source_url"] and not Path(source_ref).exists():
        db_execute(
            "UPDATE jobs SET last_error = ? WHERE id = ?",
            ("Auto resume failed because video file is missing", job_id),
        )
        add_event(job_id, "AUTO_RESUME_FAILED", "Video file is missing")
        return

    offset = float(job["out_time_seconds"] or 0)
    if job["loop"] and job["duration_seconds"]:
        offset %= float(job["duration_seconds"])
    if not job["loop"] and job["duration_seconds"] and offset >= float(job["duration_seconds"]):
        add_event(job_id, "AUTO_RESUME_SKIPPED", "Job is already at the end of the video", offset)
        return

    next_retry_count = retry_count + 1
    db_execute(
        """
        UPDATE jobs
        SET status = ?, retry_count = ?, ended_at = NULL, exit_code = NULL, last_error = NULL
        WHERE id = ?
        """,
        ("PENDING", next_retry_count, job_id),
    )
    add_event(
        job_id,
        "AUTO_RESUME_SCHEDULED",
        f"Auto resume scheduled in {AUTO_RETRY_DELAY_SECONDS}s from {offset:.2f}s "
        f"(retry {next_retry_count}{'' if max_retries == 0 else '/' + str(max_retries)})",
        offset,
    )
    if AUTO_RETRY_DELAY_SECONDS > 0:
        time.sleep(AUTO_RETRY_DELAY_SECONDS)
    if shutdown_event.is_set():
        return
    current = db_fetchone("SELECT status FROM jobs WHERE id = ?", (job_id,))
    if not current or current["status"] != "PENDING":
        add_event(job_id, "AUTO_RESUME_CANCELED", "Job status changed", offset)
        return
    add_event(job_id, "AUTO_RESUME_STARTING", f"Auto resume starting from {offset:.2f}s", offset)
    try:
        start_ffmpeg_job(
            job_id,
            resolve_s3_source(source_ref) if job["source_url"] else Path(source_ref),
            job["publish_url"],
            job["rtsp_transport"] or "tcp",
            bool(job["loop"]),
            job["duration_seconds"] or video["duration_seconds"],
            start_offset_seconds=offset,
            encoding_profile=job["encoding_profile"] or PROFILE_COPY,
            h264_crf=int(job["h264_crf"] or DEFAULT_H264_CRF),
            h264_preset=job["h264_preset"] or DEFAULT_H264_PRESET,
            h264_gop=int(job["h264_gop"] or DEFAULT_H264_GOP),
        )
    except FileNotFoundError:
        db_execute(
            "UPDATE jobs SET status = ?, last_error = ?, ended_at = ? WHERE id = ?",
            ("FAILED", f"{FFMPEG_BIN} not found", utc_now(), job_id),
        )
        add_event(job_id, "AUTO_RESUME_FAILED", f"{FFMPEG_BIN} was not found", offset)
    except HTTPException as error:
        db_execute("UPDATE jobs SET status = ?, last_error = ? WHERE id = ?", ("FAILED", str(error.detail), job_id))
        add_event(job_id, "AUTO_RESUME_FAILED", str(error.detail), offset)


@asynccontextmanager
async def lifespan(_: FastAPI):
    shutdown_event.clear()
    init_storage()
    scan_video_libraries()
    resume_active_jobs()
    yield
    shutdown_event.set()
    with runtime_lock:
        processes = list(runtime_processes.items())
    for job_id, runtime in processes:
        try:
            runtime.process.terminate()
            add_log(job_id, "SYSTEM", "Process terminated during application shutdown")
        except Exception:
            pass

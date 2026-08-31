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

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


APP_ROOT = Path(__file__).resolve().parent
BASE_DIR = Path(os.getenv("RESTREAM_DATA_DIR", "/data"))
UPLOAD_DIR = Path(os.getenv("RESTREAM_UPLOAD_DIR", str(BASE_DIR / "videos")))
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


class RestreamStartRequest(BaseModel):
    video_id: Optional[str] = None
    s3_source: Optional[str] = None
    camera_code: Optional[str] = None
    camera_name: Optional[str] = None
    camera_ip: Optional[str] = None
    rtsp_path: str = Field(min_length=1)
    username: Optional[str] = None
    password: Optional[str] = None
    publish_scheme: Optional[str] = None
    publish_host: Optional[str] = None
    publish_port: Optional[int] = None
    view_scheme: Optional[str] = None
    view_host: Optional[str] = None
    view_port: Optional[int] = None
    rtsp_transport: str = "tcp"
    loop: bool = False
    auto_retry: bool = True
    encoding_profile: str = PROFILE_COPY
    h264_crf: int = Field(default=DEFAULT_H264_CRF, ge=0, le=51)
    h264_preset: str = DEFAULT_H264_PRESET
    h264_gop: int = Field(default=DEFAULT_H264_GOP, ge=1, le=1000)


class RestartJobRequest(BaseModel):
    start_offset_seconds: float = Field(default=0, ge=0)


class RuntimeProcess:
    def __init__(self, process: subprocess.Popen):
        self.process = process


runtime_processes: dict[str, RuntimeProcess] = {}
runtime_lock = threading.Lock()
job_action_lock = threading.Lock()
video_registry_lock = threading.Lock()
shutdown_event = threading.Event()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_storage() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
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


app = FastAPI(title="RTSP Restream Tool", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=APP_ROOT / "static"), name="static")


@app.get("/")
def index():
    return FileResponse(APP_ROOT / "static" / "index.html")


@app.get("/api/config")
def config():
    return {
        "publish_scheme": DEFAULT_PUBLISH_SCHEME,
        "publish_host": DEFAULT_PUBLISH_HOST,
        "publish_port": DEFAULT_PUBLISH_PORT,
        "view_scheme": DEFAULT_VIEW_SCHEME,
        "view_host": DEFAULT_VIEW_HOST,
        "view_port": DEFAULT_VIEW_PORT,
        "username": DEFAULT_USERNAME,
        "password": DEFAULT_PASSWORD,
        "library_dirs": [str(path) for path in LIBRARY_DIRS],
        "encoding_profiles": [
            {"id": PROFILE_COPY, "label": "Copy original (no re-encode)"},
            {"id": PROFILE_H264_LOW_LATENCY, "label": "H.264 low latency (loop, no audio)"},
        ],
        "h264_defaults": {
            "crf": DEFAULT_H264_CRF,
            "preset": DEFAULT_H264_PRESET,
            "gop": DEFAULT_H264_GOP,
        },
    }


@app.post("/api/videos")
async def upload_video(file: UploadFile = File(...)):
    return await save_uploaded_video(file)


@app.post("/api/videos/batch")
async def upload_videos(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="files are required")
    uploaded = []
    for file in files:
        uploaded.append(await save_uploaded_video(file))
    return {"count": len(uploaded), "items": uploaded}


@app.get("/api/videos")
def list_video():
    with video_registry_lock:
        scan_video_libraries()
        merge_duplicate_video_records()
        prune_missing_videos()
    rows = db_fetchall("SELECT * FROM videos ORDER BY created_at DESC")
    return [video_row_to_dict(row) for row in rows]


@app.post("/api/videos/scan")
def scan_videos():
    with video_registry_lock:
        imported = scan_video_libraries()
        deduplicated = merge_duplicate_video_records()
        pruned = prune_missing_videos()
    return {"imported": imported, "deduplicated": deduplicated, "pruned": pruned}


@app.post("/api/jobs")
def start_job(request: RestreamStartRequest):
    with job_action_lock:
        source_url = None
        if request.s3_source and request.s3_source.strip():
            source_url = request.s3_source.strip()
            assert_s3_source_has_no_active_job(source_url)
            input_source = resolve_s3_source(source_url)
            video_id = str(uuid.uuid4())
            video = {
                "id": video_id,
                "path": source_url,
                "duration_seconds": None,
            }
            db_execute(
                """
                INSERT INTO videos(id, original_name, path, size_bytes, duration_seconds, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (video_id, source_display_name(request.s3_source), source_url, 0, None, utc_now()),
            )
        else:
            if not request.video_id:
                raise HTTPException(status_code=400, detail="Select a local video or provide an S3 source")
            video = db_fetchone("SELECT * FROM videos WHERE id = ?", (request.video_id,))
            if not video:
                raise HTTPException(status_code=404, detail="video not found")
            assert_video_has_no_active_job(request.video_id)
            if not Path(video["path"]).exists():
                raise HTTPException(status_code=404, detail="video file is missing on disk")
            input_source = video["path"]
        rtsp_path = validate_rtsp_path(request.rtsp_path)
        camera_code = request.camera_code or rtsp_path
        transport = request.rtsp_transport.lower()
        if transport not in {"tcp", "udp"}:
            raise HTTPException(status_code=400, detail="rtsp_transport must be tcp or udp")
        encoding_profile, h264_preset, h264_crf, h264_gop = normalize_profile(
            request.encoding_profile,
            request.h264_preset,
            request.h264_crf,
            request.h264_gop,
        )
        loop = request.loop or encoding_profile == PROFILE_H264_LOW_LATENCY

        username = request.username if request.username is not None else DEFAULT_USERNAME
        password = request.password if request.password is not None else DEFAULT_PASSWORD
        publish_url = build_rtsp_url(
            request.publish_scheme or DEFAULT_PUBLISH_SCHEME,
            request.publish_host or DEFAULT_PUBLISH_HOST,
            request.publish_port or DEFAULT_PUBLISH_PORT,
            rtsp_path,
            username,
            password,
        )
        view_url = build_rtsp_url(
            request.view_scheme or DEFAULT_VIEW_SCHEME,
            request.view_host or DEFAULT_VIEW_HOST,
            request.view_port or DEFAULT_VIEW_PORT,
            rtsp_path,
            username,
            password,
        )
        duration = video["duration_seconds"]
        job_id = str(uuid.uuid4())
        db_execute(
            """
            INSERT INTO jobs(
                id, video_id, camera_code, camera_name, camera_ip, rtsp_path, publish_url, view_url,
                duration_seconds, rtsp_transport, loop, auto_retry, max_retries,
                encoding_profile, h264_crf, h264_preset, h264_gop, source_url, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                video["id"],
                camera_code,
                request.camera_name,
                request.camera_ip,
                rtsp_path,
                publish_url,
                view_url,
                duration,
                transport,
                1 if loop else 0,
                1 if request.auto_retry else 0,
                AUTO_RETRY_MAX_RETRIES,
                encoding_profile,
                h264_crf,
                h264_preset,
                h264_gop,
                source_url,
                "PENDING",
                utc_now(),
            ),
        )
        profile_summary = (
            f"H.264 low latency: CRF={h264_crf}, preset={h264_preset}, GOP={h264_gop}, audio=off"
            if encoding_profile == PROFILE_H264_LOW_LATENCY
            else "Copy original streams"
        )
        add_event(job_id, "SESSION_CREATED", f"Profile selected: {profile_summary}")
    try:
        start_ffmpeg_job(
            job_id,
            input_source,
            publish_url,
            transport,
            loop,
            duration,
            encoding_profile=encoding_profile,
            h264_crf=h264_crf,
            h264_preset=h264_preset,
            h264_gop=h264_gop,
        )
    except FileNotFoundError:
        db_execute(
            "UPDATE jobs SET status = ?, last_error = ?, ended_at = ? WHERE id = ?",
            ("FAILED", f"{FFMPEG_BIN} not found", utc_now(), job_id),
        )
        raise HTTPException(status_code=500, detail=f"{FFMPEG_BIN} not found")
    except HTTPException as error:
        db_execute(
            "UPDATE jobs SET status = ?, last_error = ?, ended_at = ? WHERE id = ?",
            ("FAILED", str(error.detail), utc_now(), job_id),
        )
        add_event(job_id, "SESSION_START_FAILED", str(error.detail))
        raise
    return get_job(job_id)


@app.get("/api/jobs")
def list_jobs():
    rows = db_fetchall(jobs_with_video_query("ORDER BY jobs.created_at DESC"))
    return [job_row_to_dict(row) for row in rows]


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    row = db_fetchone(jobs_with_video_query("WHERE jobs.id = ?"), (job_id,))
    if not row:
        raise HTTPException(status_code=404, detail="job not found")
    return job_row_to_dict(row)


@app.post("/api/jobs/{job_id}/stop")
def stop_job(job_id: str):
    row = db_fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))
    if not row:
        raise HTTPException(status_code=404, detail="job not found")
    with runtime_lock:
        runtime = runtime_processes.get(job_id)
    if not runtime or runtime.process.poll() is not None:
        db_execute("UPDATE jobs SET status = ?, ended_at = ? WHERE id = ?", ("STOPPED", utc_now(), job_id))
        add_event(job_id, "STOP_REQUESTED", "Stop requested but process is not running")
        return get_job(job_id)
    db_execute("UPDATE jobs SET status = ? WHERE id = ?", ("STOPPING", job_id))
    add_event(job_id, "STOP_REQUESTED", "Stop requested")
    terminate_runtime_process(job_id)
    db_execute("UPDATE jobs SET status = ?, ended_at = ? WHERE id = ?", ("STOPPED", utc_now(), job_id))
    return get_job(job_id)


@app.post("/api/jobs/{job_id}/resume")
def resume_job(job_id: str):
    job = db_fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    if job["status"] in RESUMABLE_STATUSES:
        return get_job(job_id)
    assert_video_has_no_active_job(job["video_id"])
    video = db_fetchone("SELECT * FROM videos WHERE id = ?", (job["video_id"],))
    if not video:
        raise HTTPException(status_code=404, detail="video not found")
    source_ref = job["source_url"] or video["path"]
    if not job["source_url"] and not Path(source_ref).exists():
        raise HTTPException(status_code=404, detail="video file is missing on disk")
    offset = float(job["out_time_seconds"] or 0)
    if job["loop"] and job["duration_seconds"]:
        offset %= float(job["duration_seconds"])
    elif job["duration_seconds"] and offset >= float(job["duration_seconds"]):
        offset = 0
    db_execute(
        "UPDATE jobs SET status = ?, ended_at = NULL, exit_code = NULL, last_error = NULL WHERE id = ?",
        ("PENDING", job_id),
    )
    add_event(job_id, "MANUAL_RESUME_REQUESTED", f"Manual resume requested from {offset:.2f}s", offset)
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
        raise HTTPException(status_code=500, detail=f"{FFMPEG_BIN} not found")
    except HTTPException as error:
        db_execute(
            "UPDATE jobs SET status = ?, last_error = ?, ended_at = ? WHERE id = ?",
            ("FAILED", str(error.detail), utc_now(), job_id),
        )
        add_event(job_id, "MANUAL_RESUME_FAILED", str(error.detail), offset)
        raise
    return get_job(job_id)


@app.post("/api/jobs/{job_id}/auto-retry")
def toggle_auto_retry(job_id: str):
    job = db_fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    enable_auto_retry = not bool(job["auto_retry"])
    db_execute(
        "UPDATE jobs SET auto_retry = ?, max_retries = ? WHERE id = ?",
        (1 if enable_auto_retry else 0, AUTO_RETRY_MAX_RETRIES, job_id),
    )
    add_event(
        job_id,
        "AUTO_COMPLETE_ENABLED" if enable_auto_retry else "AUTO_COMPLETE_DISABLED",
        "Auto complete enabled" if enable_auto_retry else "Auto complete disabled",
    )

    if enable_auto_retry:
        refreshed = db_fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))
        is_active = refreshed["status"] in ACTIVE_STATUSES
        is_complete = refreshed["progress_percent"] is not None and float(refreshed["progress_percent"]) >= 100
        if not is_active and not is_complete:
            add_event(job_id, "AUTO_COMPLETE_RESUME_REQUESTED", "Auto complete action requested resume")
            return resume_job(job_id)
    return get_job(job_id)


@app.post("/api/jobs/{job_id}/restart")
def restart_job(job_id: str, request: Optional[RestartJobRequest] = None):
    job = db_fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    video = db_fetchone("SELECT * FROM videos WHERE id = ?", (job["video_id"],))
    if not video:
        raise HTTPException(status_code=404, detail="video not found")
    source_ref = job["source_url"] or video["path"]
    if not job["source_url"] and not Path(source_ref).exists():
        raise HTTPException(status_code=404, detail="video file is missing on disk")
    if job["status"] in ACTIVE_STATUSES:
        add_event(job_id, "RESTART_REQUESTED", "Restart requested; stopping current process first")
        db_execute("UPDATE jobs SET status = ? WHERE id = ?", ("STOPPING", job_id))
        terminate_runtime_process(job_id)
    duration = video["duration_seconds"] or job["duration_seconds"]
    start_offset = request.start_offset_seconds if request else 0
    if job["loop"] and duration:
        start_offset %= float(duration)
    elif duration and start_offset >= float(duration):
        start_offset = 0
    start_offset = max(0, float(start_offset or 0))
    if duration and start_offset > 0 and not job["loop"]:
        progress_percent = min(100.0, start_offset / float(duration) * 100)
    else:
        progress_percent = 0
    db_execute(
        """
        UPDATE jobs
        SET
            status = ?,
            out_time_seconds = ?,
            progress_percent = ?,
            exit_code = NULL,
            last_error = NULL,
            ended_at = NULL,
            retry_count = 0,
            duration_seconds = ?
        WHERE id = ?
        """,
        ("PENDING", start_offset, progress_percent, duration, job_id),
    )
    add_event(job_id, "RUN_AGAIN", f"Run again requested in the same session from {start_offset:.2f}s", start_offset)
    try:
        start_ffmpeg_job(
            job_id,
            resolve_s3_source(source_ref) if job["source_url"] else Path(source_ref),
            job["publish_url"],
            job["rtsp_transport"] or "tcp",
            bool(job["loop"]),
            duration,
            start_offset_seconds=start_offset,
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
        raise HTTPException(status_code=500, detail=f"{FFMPEG_BIN} not found")
    except HTTPException as error:
        db_execute(
            "UPDATE jobs SET status = ?, last_error = ?, ended_at = ? WHERE id = ?",
            ("FAILED", str(error.detail), utc_now(), job_id),
        )
        add_event(job_id, "RUN_AGAIN_FAILED", str(error.detail), start_offset)
        raise
    return get_job(job_id)


@app.delete("/api/jobs/{job_id}")
def delete_job(job_id: str):
    job = db_fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    if job["status"] in ACTIVE_STATUSES:
        add_event(job_id, "DELETE_REQUESTED", "Delete requested; stopping current process first")
        db_execute("UPDATE jobs SET status = ? WHERE id = ?", ("STOPPING", job_id))
        terminate_runtime_process(job_id)
    add_event(job_id, "DELETED", "Session deleted")
    db_execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    return {"deleted": True, "id": job_id}


@app.get("/api/jobs/{job_id}/frame")
def get_job_frame(job_id: str):
    frame_path = capture_job_frame(job_id)
    return FileResponse(
        frame_path,
        media_type="image/jpeg",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/jobs/{job_id}/logs")
def get_logs(job_id: str, limit: int = 300, include_progress: bool = False):
    if not db_fetchone("SELECT id FROM jobs WHERE id = ?", (job_id,)):
        raise HTTPException(status_code=404, detail="job not found")
    capped_limit = min(max(limit, 1), 2000)
    if include_progress:
        query = "SELECT created_at, level, message FROM logs WHERE job_id = ? ORDER BY id DESC LIMIT ?"
        params = (job_id, capped_limit)
    else:
        query = """
        SELECT created_at, level, message
        FROM logs
        WHERE job_id = ? AND level != 'PROGRESS'
        ORDER BY id DESC
        LIMIT ?
        """
        params = (job_id, capped_limit)
    rows = db_fetchall(
        query,
        params,
    )
    return list(reversed([row_to_dict(row) for row in rows]))


@app.get("/api/events")
def get_events(limit: int = 300):
    capped_limit = min(max(limit, 1), 2000)
    rows = db_fetchall(
        """
        SELECT
            event_logs.*,
            jobs.camera_name,
            jobs.rtsp_path,
            videos.original_name AS video_name
        FROM event_logs
        LEFT JOIN jobs ON jobs.id = event_logs.job_id
        LEFT JOIN videos ON videos.id = jobs.video_id
        ORDER BY event_logs.id DESC
        LIMIT ?
        """,
        (capped_limit,),
    )
    return list(reversed([row_to_dict(row) for row in rows]))


@app.get("/api/jobs/{job_id}/events")
def get_job_events(job_id: str, limit: int = 300):
    if not db_fetchone("SELECT id FROM jobs WHERE id = ?", (job_id,)):
        raise HTTPException(status_code=404, detail="job not found")
    capped_limit = min(max(limit, 1), 2000)
    rows = db_fetchall(
        """
        SELECT *
        FROM event_logs
        WHERE job_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (job_id, capped_limit),
    )
    return list(reversed([row_to_dict(row) for row in rows]))

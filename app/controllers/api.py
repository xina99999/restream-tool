from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

from app.models.schemas import RestartJobRequest, RestreamStartRequest, S3DownloadRequest
from app.services.restream_service import *


router = APIRouter()


@router.get("/")
def index():
    return FileResponse(APP_ROOT / "static" / "index.html")


@router.get("/api/config")
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


@router.post("/api/videos")
async def upload_video(file: UploadFile = File(...)):
    return await save_uploaded_video(file)


@router.post("/api/videos/batch")
async def upload_videos(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="files are required")
    uploaded = []
    for file in files:
        uploaded.append(await save_uploaded_video(file))
    return {"count": len(uploaded), "items": uploaded}


@router.get("/api/videos")
def list_video():
    with video_registry_lock:
        scan_video_libraries()
        merge_duplicate_video_records()
        prune_missing_videos()
    rows = db_fetchall("SELECT * FROM videos ORDER BY created_at DESC")
    return [video_row_to_dict(row) for row in rows]


@router.post("/api/videos/scan")
def scan_videos():
    with video_registry_lock:
        imported = scan_video_libraries()
        deduplicated = merge_duplicate_video_records()
        pruned = prune_missing_videos()
    return {"imported": imported, "deduplicated": deduplicated, "pruned": pruned}


@router.get("/api/downloads")
def get_downloads():
    return list_downloads()


@router.post("/api/downloads")
def download_from_s3(request: S3DownloadRequest):
    return start_s3_download(request.source)


@router.post("/api/downloads/{download_id}/resume")
def resume_s3_download(download_id: str):
    return resume_download(download_id)


@router.post("/api/jobs")
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


@router.get("/api/jobs")
def list_jobs():
    rows = db_fetchall(jobs_with_video_query("ORDER BY jobs.created_at DESC"))
    return [job_row_to_dict(row) for row in rows]


@router.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    row = db_fetchone(jobs_with_video_query("WHERE jobs.id = ?"), (job_id,))
    if not row:
        raise HTTPException(status_code=404, detail="job not found")
    return job_row_to_dict(row)


@router.post("/api/jobs/{job_id}/stop")
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


@router.post("/api/jobs/{job_id}/resume")
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


@router.post("/api/jobs/{job_id}/auto-retry")
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


@router.post("/api/jobs/{job_id}/restart")
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


@router.delete("/api/jobs/{job_id}")
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


@router.get("/api/jobs/{job_id}/frame")
def get_job_frame(job_id: str):
    frame_path = capture_job_frame(job_id)
    return FileResponse(
        frame_path,
        media_type="image/jpeg",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/api/jobs/{job_id}/logs")
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


@router.get("/api/events")
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


@router.get("/api/jobs/{job_id}/events")
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

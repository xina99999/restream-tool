# RTSP Restream Tool

Standalone tool for uploading a video and restreaming it to MediaMTX with ffmpeg.

## Project structure

```text
app/
  controllers/api.py          HTTP routes and response handling
  models/schemas.py           Pydantic request models
  services/restream_service.py SQLite, S3, FFmpeg, and job runtime logic
  main.py                     FastAPI application composition
  static/                     Browser UI
```

## Features

- Upload video files to the server with browser-side upload progress.
- Upload multiple files or a browser-selected folder.
- Scan existing video folders mounted from the host.
- Show clear UI notifications when an action fails or succeeds.
- Search and sort videos by newest, oldest, name or duration.
- Enter an RTSP path and connection settings.
- Restream directly from an S3 presigned HTTPS URL or `s3://bucket/key`.
- Choose copy mode or an H.264 low-latency profile, with per-session CRF, preset and GOP settings.
- Start/stop ffmpeg restream jobs.
- Allow only one active ffmpeg process per video.
- Resume active jobs automatically after container restart.
- Resume a stopped/failed job from its last saved video timestamp.
- Mark a job failed if ffmpeg output time stalls for too long.
- Auto-resume failed/stalled jobs until the video completes.
- Track progress percent from ffmpeg output.
- Show the RTSP view URL.
- Persist job history and concise ffmpeg logs in SQLite, with raw progress logs available on demand.
- Persist structured action event logs in `event_logs`.
- Run as a Docker container.

## Run

```bash
cd restream-tool
cp .env.example .env
docker compose up --build
```

If Docker Hub returns an authorization error while pulling `python:3.12-slim`, the compose file already builds from the ECR Public mirror:

```yaml
build:
  context: .
  args:
    PYTHON_IMAGE: public.ecr.aws/docker/library/python:3.12-slim
```

Open:

```text
http://localhost:8090
```

## S3 Restream

In the **Restream** panel, tick **Restream from S3**, then paste either:

```text
s3://your-bucket/path/to/video.mp4
```

or an HTTPS presigned URL. For `s3://` sources, set `AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` in `.env`, then rebuild with `docker compose up -d --build`. An HTTPS presigned URL needs no AWS credentials in the container.

The tool reads the remote video directly with FFmpeg; it does not download a full local copy first. Resume, Run Again, auto-retry, frame capture, and container-start recovery re-create the temporary URL for an `s3://` source. A presigned HTTPS URL must remain valid for the whole restream session.

To keep a local reusable copy, paste the same source into **Download S3** in the Video panel. The download runs in the background, reports downloaded bytes in the UI, scans duration after completion, and adds the resulting file to the video library. Public S3 HTTPS URLs require no AWS credentials.

## Retry behavior

By default, new jobs enable `Auto resume until complete`. If ffmpeg fails or stalls before the video ends, the same job is resumed from its last saved timestamp.

```yaml
RESTREAM_STALL_TIMEOUT_SECONDS: 120
RESTREAM_AUTO_RETRY_DELAY_SECONDS: 10
RESTREAM_AUTO_RETRY_MAX_RETRIES: 0
```

`RESTREAM_AUTO_RETRY_MAX_RETRIES: 0` means unlimited retries until completion.

## Existing videos

To use videos that already exist on the host, mount the folder into the container:

```yaml
environment:
  RESTREAM_LIBRARY_DIRS: /library/videos
volumes:
  - /absolute/path/to/videos:/library/videos:ro
```

The UI scans this folder on startup and whenever you open the video list. You can also click `Scan Existing Videos`.

Do not copy files to `/data` on the host unless `/data` is explicitly mounted into the container. In the default compose file, `/data` is a Docker named volume, so the host path `/data` is not the same storage location.

On Linux, keep this line if MediaMTX runs on the host:

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

## Large video transfers with tmux

For large files, prefer downloading or copying them into the mounted host folder instead of uploading through the browser. Use `tmux` so the transfer keeps running if SSH disconnects.

Start a tmux session:

```bash
tmux new -s restream-download
```

Download or copy videos into the mounted folder:

```bash
cd "/home/mic-742/Data_12h/Data 24h_260626/260626"
wget -c "VIDEO_URL"
```

Detach from tmux:

```text
Ctrl-b d
```

Attach again:

```bash
tmux attach -t restream-download
```

Check progress from another shell:

```bash
ls -lh "/home/mic-742/Data_12h/Data 24h_260626/260626"
du -h "/home/mic-742/Data_12h/Data 24h_260626/260626"/*
```

After the file is complete, click `Scan Existing` in the web UI.

## MediaMTX host

If MediaMTX runs on the host and the container must publish to the host machine, set:

```yaml
RESTREAM_PUBLISH_HOST: host.docker.internal
RESTREAM_PUBLISH_PORT: 18554
```

If MediaMTX runs as another container in the same Docker network, set:

```yaml
RESTREAM_PUBLISH_HOST: mediamtx
RESTREAM_PUBLISH_PORT: 8554
```

Then attach this tool to the same Docker network as MediaMTX.

The view URL can stay as the address your operators use, for example:

```yaml
RESTREAM_VIEW_HOST: localhost
RESTREAM_VIEW_PORT: 18554
```

## Encoding profiles

`Copy original (no re-encode)` preserves the source video/audio streams:

```bash
ffmpeg -re -i "video-file" -c:v copy -c:a copy -f rtsp -rtsp_transport tcp "rtsp://USERNAME:PASSWORD@localhost:18554/test1_linfox_camera"
```

`H.264 low latency (loop, no audio)` creates a continuous H.264 stream. It forces video looping and uses `-an`, matching the desired no-audio publishing behavior. Default settings are CRF 23, preset `veryfast`, and GOP 30:

```bash
ffmpeg -hide_banner -loglevel warning -nostdin \
  -re -stream_loop -1 -i "video-file" \
  -c:v libx264 -preset veryfast -tune zerolatency \
  -crf 23 -pix_fmt yuv420p \
  -g 30 -keyint_min 30 \
  -x264-params "keyint=30:min-keyint=30:scenecut=0:repeat-headers=1" \
  -an -f rtsp -rtsp_transport tcp "rtsp://USERNAME:PASSWORD@localhost:18554/test-path"
```

The same GOP value is supplied through `-g`, `-keyint_min`, and `x264-params` for encoder compatibility. Profile settings are saved with each job, so Resume, Run Again, auto retry, and container startup recovery keep the original settings. Looping sessions show elapsed stream time rather than completion percentage.

The app uses list arguments instead of shell string concatenation.

## API

Upload:

```bash
curl -F "file=@/path/video.mp4" http://localhost:8090/api/videos
```

Start:

```bash
curl -X POST http://localhost:8090/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "video_id": "VIDEO_ID",
    "rtsp_path": "test1_linfox_DCVSIP-192_168_253_8"
  }'
```

Status:

```bash
curl http://localhost:8090/api/jobs/JOB_ID
```

Logs:

```bash
curl http://localhost:8090/api/jobs/JOB_ID/logs
```

Stop:

```bash
curl -X POST http://localhost:8090/api/jobs/JOB_ID/stop
```

Run the same session again from the beginning:

```bash
curl -X POST http://localhost:8090/api/jobs/JOB_ID/restart
```

Run the same session again from a selected timestamp:

```bash
curl -X POST http://localhost:8090/api/jobs/JOB_ID/restart \
  -H "Content-Type: application/json" \
  -d '{"start_offset_seconds": 3600}'
```

Resume the same job from its last saved timestamp:

```bash
curl -X POST http://localhost:8090/api/jobs/JOB_ID/resume
```

Toggle auto-complete for an existing job:

```bash
curl -X POST http://localhost:8090/api/jobs/JOB_ID/auto-retry
```

Delete an unused session:

```bash
curl -X DELETE http://localhost:8090/api/jobs/JOB_ID
```

View structured event logs:

```bash
curl http://localhost:8090/api/jobs/JOB_ID/events
curl http://localhost:8090/api/events
```

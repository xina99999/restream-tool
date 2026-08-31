from typing import Optional

from pydantic import BaseModel, Field


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
    encoding_profile: str = "copy"
    h264_crf: int = Field(default=23, ge=0, le=51)
    h264_preset: str = "veryfast"
    h264_gop: int = Field(default=30, ge=1, le=1000)


class RestartJobRequest(BaseModel):
    start_offset_seconds: float = Field(default=0, ge=0)


class S3DownloadRequest(BaseModel):
    source: str = Field(min_length=1, max_length=4096)


class UploadInitRequest(BaseModel):
    original_name: str = Field(min_length=1, max_length=1024)
    total_bytes: int = Field(gt=0)
    client_key: str = Field(min_length=1, max_length=512)

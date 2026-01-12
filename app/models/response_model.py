from enum import Enum
from pydantic import BaseModel

from app.utils import TaskStatus


class UploadResponse(BaseModel):
    guid: str
    name: str
    status: str
    upscale: str

class StatusResponse(BaseModel):
    status: TaskStatus
    error: str | None = None

class DownloadResponse(BaseModel):
    file_path: str
    filename: str
from pydantic import BaseModel

from app.utils import TaskStatus


class UploadResponse(BaseModel):
    task_id: str

class StatusResponse(BaseModel):
    status: TaskStatus
    error: str | None = None

class DownloadResponse(BaseModel):
    file_path: str
    filename: str
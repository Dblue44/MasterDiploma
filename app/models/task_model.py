# app/models/task_model.py
from pydantic import BaseModel


class PhotoTask(BaseModel):
    task_id: str
    filename: str
    scale: int

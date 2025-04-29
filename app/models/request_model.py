# app/models/request_model.py
from pydantic import BaseModel, Field


class UploadRequest(BaseModel):
    scale: int = Field(..., ge=1, le=4, description="Уровень увеличения от 2 до 4")

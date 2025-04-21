import os
import json
import uuid
from PIL import Image
from typing import Annotated
from fastapi import APIRouter, UploadFile, HTTPException, File
from fastapi.responses import FileResponse
from app.core import settings
from app.logger import logger
from app.services import state
from app.services.kafka import PhotoTask

upscale_v1 = APIRouter()


@upscale_v1.post("/upload")
async def upload_photo(file: UploadFile = File(...), scale: int = 2):
    contents = await file.read()

    with Image.open(file.file) as img:
        if img.width > 1080 or img.height > 920:
            raise HTTPException(status_code=400, detail="Image dimensions too large")

    task_id = str(uuid.uuid4())
    filename = f"{task_id}_{file.filename}"
    save_path = settings.UPLOAD_DIR / filename

    with open(save_path, "wb") as f:
        f.write(contents)

    task = PhotoTask(task_id=task_id, filename=filename, scale=scale)

    await state.kafka_producer.send_and_wait(settings.KAFKA_TOPIC, json.dumps(task.model_dump()).encode("utf-8"))

    await state.redis_client.hset(task_id, mapping={"status": "queued"})

    return {"task_id": task_id}

@upscale_v1.get("/status/{task_id}")
async def get_status(task_id: str):
    if not await state.redis_client.exists(task_id):
        logger.info(f"Task id: {task_id}. Task not found")
        raise HTTPException(status_code=404, detail="Task not found")
    return await state.redis_client.hgetall(task_id)

@upscale_v1.get("/download/{task_id}")
def download(task_id: str):
    filename = next((f for f in os.listdir(settings.RESULT_DIR) if f.startswith(task_id)), None)
    if not filename:
        logger.info(f"Task id: {task_id}. File not found")
        raise HTTPException(status_code=404, detail="Result not found")
    return FileResponse(path=settings.RESULT_DIR / filename, media_type='image/jpeg', filename=filename)
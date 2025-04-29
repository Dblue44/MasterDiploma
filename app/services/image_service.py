# app/services/image_service.py
import uuid
from pathlib import Path
from PIL import Image
from fastapi import UploadFile, HTTPException
from app.core import settings
from app.logger import logger
from app.services import get_filename_by_task_id
from app.utils import TaskStatus
from app.models import PhotoTask
from app.services.kafka_service import KafkaService
from app.services.redis_service import RedisService


class ImageService:
    def __init__(self, kafka_service: KafkaService, redis_service: RedisService):
        self.kafka = kafka_service
        self.redis = redis_service

    async def upload_photo(self, file: UploadFile, scale: int) -> dict:
        logger.info(f"Uploading photo: {file.filename}, scale: {scale}")
        with Image.open(file.file) as img:
            if img.width > 1080 or img.height > 920:
                logger.warning("Image size too large")
                raise HTTPException(status_code=400, detail="Image dimensions too large")

        task_id = str(uuid.uuid4())
        filename = f"{task_id}_{file.filename}"
        save_path = settings.UPLOAD_DIR / filename

        logger.debug(f"Saving uploaded file to: {save_path}")
        with open(save_path, "wb") as out_file:
            while chunk := await file.read(1024 * 1024):  # 1MB
                out_file.write(chunk)
        await file.close()

        task = PhotoTask(task_id=task_id, filename=filename, scale=scale)

        try:
            logger.info(f"Trying to send task {task_id} to Kafka")
            await self.kafka.send_task(settings.KAFKA_TOPIC, task.model_dump())
            logger.info(f"Task {task_id} successfully sent to Kafka")
            await self.redis.set_task_status(task_id, {"status": TaskStatus.QUEUED})
        except Exception as e:
            logger.error(f"Failed to send task {task_id} to Kafka: {e}")
            await self.redis.set_task_status(task_id, {
                "status": TaskStatus.QUEUED,
                "pending_kafka": True,
                "error": str(e)
            })

        return {"task_id": task_id}

    async def get_status(self, task_id: str) -> dict:
        logger.debug(f"Getting status for task: {task_id}")
        if not await self.redis.task_exists(task_id):
            logger.warning(f"Task not found: {task_id}")
            raise HTTPException(status_code=404, detail="Task not found")
        return await self.redis.get_task_status(task_id)

    @staticmethod
    async def download_image(task_id: str) -> tuple[Path, str]:
        logger.debug(f"Downloading image for task: {task_id}")
        filename = get_filename_by_task_id(task_id)
        if not filename:
            logger.warning(f"File not found for task: {task_id}")
            raise HTTPException(status_code=404, detail="Result not found")
        return settings.RESULT_DIR / filename, filename
# app/services/image_service.py
import uuid
from io import BytesIO
from pathlib import Path
import cv2
import numpy as np
from fastapi import UploadFile, HTTPException
from fastapi.responses import FileResponse
from app.core import settings
from app.logger import logger
from app.models import UploadResponse, PhotoTask
from .kafka_service import KafkaService
from .redis_service import RedisService
from .helper import get_filename_by_task_id
from app.utils import TaskStatus


class ImageService:
    def __init__(self, kafka_service: KafkaService, redis_service: RedisService):
        self.kafka = kafka_service
        self.redis = redis_service

    async def upload_photo(self, file: UploadFile, scale: int, model_name: str, model_version: str) -> UploadResponse:
        logger.info(f"Uploading photo: {file.filename}, scale: {scale}")

        # file_bytes = await file.read()
        #
        # nparr = np.frombuffer(file_bytes, np.uint8)
        # img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        # if img is None:
        #     logger.error("Cannot decode uploaded image via OpenCV")
        #     raise HTTPException(status_code=400, detail="Invalid image file")
        # height, width = img.shape[:2]
        #
        # if width > 1080 or height > 920:
        #     logger.warning("Image size too large")
        #     raise HTTPException(status_code=400, detail="Image dimensions too large")

        task_id = str(uuid.uuid4())
        filename = f"{task_id}_{file.filename}"
        # save_path = settings.UPLOAD_DIR / filename
        #
        # logger.debug(f"Saving uploaded file to: {save_path}")
        # with open(save_path, "wb") as out_file:
        #     out_file.write(file_bytes)
        # await file.close()
        #
        task = PhotoTask(task_id=task_id, filename=filename, scale=scale, model_name=model_name, model_version=model_version)
        #
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

        return UploadResponse(guid=task_id, name=file.filename, status=TaskStatus.QUEUED, upscale=f"x{scale}")

    async def get_status(self, task_id: str) -> dict:
        logger.debug(f"Getting status for task: {task_id}")
        if not await self.redis.task_exists(task_id):
            logger.warning(f"Task not found: {task_id}")
            raise HTTPException(status_code=404, detail="Task not found")
        return await self.redis.get_task_status(task_id)

    @staticmethod
    async def get_image(task_id: str) -> tuple[Path, str]:
        logger.debug(f"Get image info for task: {task_id}")
        filename = get_filename_by_task_id(task_id)
        if not filename:
            logger.warning(f"File not found for task: {task_id}")
            raise HTTPException(status_code=404, detail="Result not found")
        return settings.RESULT_DIR / filename, filename

    @staticmethod
    async def download_image(file_path: str, filename: str) -> FileResponse:
        logger.debug(f"Get image {filename} from patch: {file_path}")
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type="application/octet-stream"
        )
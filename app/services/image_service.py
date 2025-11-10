# app/services/image_service.py
import uuid
import os
import mimetypes
import tempfile
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import List

import cv2
import numpy as np
from fastapi import UploadFile, HTTPException
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

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

        file_bytes = await file.read()
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
        save_path = settings.UPLOAD_DIR / filename
        #
        logger.debug(f"Saving uploaded file to: {save_path}")
        with open(save_path, "wb") as out_file:
            out_file.write(file_bytes)
        await file.close()
        #
        task = PhotoTask(task_id=task_id, filename=filename, scale=scale, model_name=model_name, model_version=model_version)
        #
        try:
            # logger.info(f"Trying to send task {task_id} to Kafka")
            # await self.kafka.send_task(settings.KAFKA_TOPIC, task.model_dump())
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


    async def download_images(self, guids: List[str]) -> FileResponse:
        """
        Проверяет существование всех файлов по GUID'ам, упаковывает их в ZIP и возвращает архив.
        Если какого-то файла нет — 404 с перечислением отсутствующих GUID.
        """
        logger.debug(f"[Download] Multiple images: {guids}")

        files_to_zip: list[tuple[Path, str]] = []
        missing: list[str] = []

        for guid in guids:
            filename = get_filename_by_task_id(guid)
            if not filename:
                missing.append(guid)
                continue
            file_path = settings.RESULT_DIR / filename
            if not file_path.exists():
                missing.append(guid)
                continue
            files_to_zip.append((file_path, filename))

        if missing:
            logger.warning(f"[Download] Missing results for tasks: {missing}")
            raise HTTPException(
                status_code=404,
                detail={"message": "Some results not found", "missing": missing}
            )

        if not files_to_zip:
            raise HTTPException(status_code=404, detail="No files to archive")

        tmp_fd, tmp_zip_path = tempfile.mkstemp(suffix=".zip")
        os.close(tmp_fd)
        try:
            with zipfile.ZipFile(tmp_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                for file_path, arcname in files_to_zip:
                    zf.write(str(file_path), arcname=arcname)

            archive_name = f"images_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.zip"

            def _cleanup(path: str) -> None:
                try:
                    os.remove(path)
                except Exception:
                    pass

            return FileResponse(
                path=tmp_zip_path,
                filename=archive_name,
                media_type="application/zip",
                background=BackgroundTask(_cleanup, tmp_zip_path)
            )
        except Exception:
            try:
                os.remove(tmp_zip_path)
            except Exception:
                pass
            raise


    async def download_image(self, task_id: str) -> FileResponse:
        """
        Ищет в settings.RESULT_DIR файл, соответствующий GUID (через get_filename_by_task_id),
        и отдает его как файл (image/*).
        """
        logger.debug(f"[Download] Single image by task_id={task_id}")

        filename = get_filename_by_task_id(task_id)
        if not filename:
            logger.warning(f"[Download] Result not found for task {task_id}")
            raise HTTPException(status_code=404, detail="Result not found")

        file_path = settings.RESULT_DIR / filename
        if not file_path.exists():
            logger.warning(f"[Download] File missing on disk: {file_path}")
            raise HTTPException(status_code=404, detail="File not found")

        mime, _ = mimetypes.guess_type(str(file_path))
        mime = mime or "application/octet-stream"

        return FileResponse(
            path=str(file_path),
            filename=filename,
            media_type=mime
        )


    @staticmethod
    async def get_image_file(task_id: str) -> tuple[Path, str]:
        logger.debug(f"Get image info for task: {task_id}")
        filename = get_filename_by_task_id(task_id)
        if not filename:
            logger.warning(f"File not found for task: {task_id}")
            raise HTTPException(status_code=404, detail="Result not found")
        return settings.RESULT_DIR / filename, filename
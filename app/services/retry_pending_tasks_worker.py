# app/services/retry_pending_tasks_worker.py
import asyncio
from typing import Optional
from app.logger import logger
from app.services.kafka_service import KafkaService
from app.services.redis_service import RedisService
from app.utils import TaskStatus
from app.core import settings

async def retry_pending_tasks(kafka_service: KafkaService, redis_service: RedisService) -> None:
    logger.info("Starting retry pending tasks worker")
    while True:
        await asyncio.sleep(settings.REDIS_CHECK_INTERVAL_SECONDS)
        try:
            await process_pending_tasks(kafka_service, redis_service)
        except Exception as e:
            logger.exception(f"Unexpected error while processing pending tasks: {e}")

async def process_pending_tasks(kafka_service: KafkaService, redis_service: RedisService) -> None:
    if not kafka_service.connected:
        logger.warning("Kafka still not connected.")
        return

    pending_tasks = await redis_service.get_all_pending_kafka_tasks()
    logger.debug(f"Found {len(pending_tasks)} keys in Redis to check for pending tasks.")

    for task_id, task_data in pending_tasks:
        try:
            task_payload = await build_task_payload(task_id, task_data)
            if not task_payload:
                continue
            await kafka_service.send_task(settings.KAFKA_TOPIC, task_payload)
            logger.info(f"Successfully resent task {task_id} to Kafka.")
            await redis_service.set_task_status(task_id, {
                "status": task_data.get("status", TaskStatus.QUEUED),
                "pending_kafka": False
            })
        except Exception as e:
            logger.error(f"Failed to resend task {task_id}: {e}")

async def build_task_payload(task_id: str, task_data: dict) -> Optional[dict]:
    filename = task_data.get("filename")
    scale = task_data.get("scale")

    if not filename or not scale:
        logger.error(f"Incomplete task data for {task_id}: {task_data}")
        return None

    return {
        "task_id": task_id,
        "filename": filename,
        "scale": int(scale)
    }

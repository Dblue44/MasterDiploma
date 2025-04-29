import asyncio
import json
from PIL import Image
from aiokafka.errors import KafkaConnectionError
from pydantic import ValidationError
from aiokafka import AIOKafkaConsumer
from app.core import settings
from app.logger import logger
from app.services.redis_service import RedisService
from app.utils import TaskStatus
from app.models import PhotoTask


async def consume_photos(redis_service: RedisService):
    while True:
        consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="photo-workers"
        )
        try:
            logger.info(f"Trying connect to Kafka.")
            await consumer.start()
            logger.info("Kafka consumer started for topic: %s", settings.KAFKA_TOPIC)

            try:
                async for msg in consumer:
                    await handle_message(msg.value, redis_service)
            except Exception as e:
                logger.exception(f"Unexpected error while consuming messages: {e}")
            finally:
                await consumer.stop()
                logger.info("Kafka consumer stopped.")
                break
        except KafkaConnectionError as e:
            logger.warning(f"Kafka unavailable: {e}")
            await asyncio.sleep(settings.KAFKA_CONNECTION_DELAY)
        except Exception as e:
            logger.exception(f"Unexpected error starting Kafka consumer: {e}")
            await asyncio.sleep(settings.KAFKA_CONNECTION_DELAY)


async def handle_message(value: bytes, redis_service: RedisService) -> None:
    try:
        data = json.loads(value.decode("utf-8"))
        task = PhotoTask(**data)
        logger.info(f"Received task: {task.task_id}")
        await process_task(task, redis_service)
    except (json.JSONDecodeError, ValidationError) as e:
        logger.exception(f"Failed to parse incoming message: {e}")


async def process_task(task: PhotoTask, redis_service: RedisService) -> None:
    input_path = settings.UPLOAD_DIR / task.filename
    output_path = settings.RESULT_DIR / task.filename

    logger.info(f"Start processing task: {task.task_id}")
    try:
        await redis_service.set_task_status(task.task_id, {"status": TaskStatus.RUNNING})

        await asyncio.sleep(60)

        with Image.open(input_path) as img:
            new_size = (img.width * task.scale, img.height * task.scale)
            result = img.resize(new_size)
            result.save(output_path)

        await redis_service.set_task_status(task.task_id, {"status": TaskStatus.COMPLETED})
        logger.info(f"Task {task.task_id} completed successfully.")
    except Exception as e:
        await redis_service.set_task_status(task.task_id, {
            "status": TaskStatus.FAILED,
            "error": str(e)
        })
        logger.exception(f"Failed to process task {task.task_id}: {e}")

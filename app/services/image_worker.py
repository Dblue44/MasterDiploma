import asyncio
import json
from PIL import Image
from pydantic import BaseModel, ValidationError
from aiokafka import AIOKafkaConsumer
from app.core import settings
from app.services import state
from app.logger import logger

class PhotoTask(BaseModel):
    task_id: str
    filename: str
    scale: int

status_store = {}

async def consume_photos():
    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="photo-workers"
    )
    await consumer.start()

    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value.decode("utf-8"))
                task = PhotoTask(**data)
                await process_task(task)
            except (json.JSONDecodeError, ValidationError) as e:
                logger.error(f"Failed to parse task: {str(e)}")
                continue
    finally:
        await consumer.stop()

async def process_task(task: PhotoTask):
    logger.info(f"Start process image. Task id:{task.task_id}")
    input_path = settings.UPLOAD_DIR / task.filename
    output_path = settings.RESULT_DIR / task.filename

    try:
        await state.redis_client.hset(task.task_id, mapping={"status": "working"})

        await asyncio.sleep(60)

        with Image.open(input_path) as img:
            new_size = (img.width * task.scale, img.height * task.scale)
            result = img.resize(new_size)
            result.save(output_path)

        await state.redis_client.hset(task.task_id, mapping={"status": "done"})
        logger.info(f"Finish process image. Task id:{task.task_id}")
    except Exception as e:
        await state.redis_client.hset(task.task_id, mapping={
            "status": "failed",
            "error": str(e)
        })
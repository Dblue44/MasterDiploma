# app/services/kafka_service.py
import asyncio
import json
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from app.logger import logger
from app.core import settings


class KafkaService:
    def __init__(self):
        self.producer = None
        self.connected = False
        self._lock = asyncio.Lock()

    async def start(self):
        async with self._lock:
            if self.connected:
                return
            try:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
                )
                await self.producer.start()
                self.connected = True
                logger.info("Kafka producer started successfully.")
            except KafkaConnectionError as e:
                await self.stop()
                logger.warning(f"Kafka unavailable: {e}")
                self.producer = None
                self.connected = False

    async def try_reconnect_loop(self):
        logger.warning("Kafka not connected on startup. Will retry in background.")
        while not self.connected:
            await asyncio.sleep(settings.KAFKA_CONNECTION_DELAY)
            logger.info("Kafka unavailable. Retrying Kafka connection.")
            await self.start()

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            self.connected = False

    async def send_task(self, topic: str, task: dict):
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")
        await self.producer.send_and_wait(
            topic,
            json.dumps(task).encode("utf-8")
        )

# app/services/redis_service.py
from redis.asyncio import Redis
from app.logger import logger
from app.core import settings


class RedisService:
    def __init__(self):
        self.client = None

    async def start(self):
        try:
            self.client = Redis(
                host=settings.REDIS_SERVER,
                port=settings.REDIS_PORT,
                decode_responses=True
            )
            await self.client.ping()
            logger.info("Redis service started successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def stop(self):
        if self.client:
            await self.client.close()

    async def set_task_status(self, task_id: str, status: dict):
        if not self.client:
            raise RuntimeError("Redis client not initialized")
        await self.client.hset(task_id, mapping=status)

    async def get_task_status(self, task_id: str) -> dict:
        if not self.client:
            raise RuntimeError("Redis client not initialized")
        return await self.client.hgetall(task_id)

    async def task_exists(self, task_id: str) -> bool:
        if not self.client:
            raise RuntimeError("Redis client not initialized")
        return await self.client.exists(task_id)

    async def get_all_pending_kafka_tasks(self) -> list[tuple[str, dict]]:
        if not self.client:
            raise RuntimeError("Redis client not initialized")

        keys = await self.client.keys("*")
        pending_tasks = []

        for key in keys:
            data = await self.client.hgetall(key)
            if data.get("pending_kafka") == "True" or data.get("pending_kafka") is True:
                pending_tasks.append((key, data))

        return pending_tasks

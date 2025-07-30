import json

from app.services.redis_service import RedisService
from app.services.socket_manager import ConnectionManager


async def redis_pubsub_listener(redis_service: RedisService, socket_manager: ConnectionManager):
    pubsub = redis_service.client.pubsub()
    await pubsub.psubscribe("task_update:*")
    async for msg in pubsub.listen():
        if msg["type"] == "pmessage":
            channel = msg["channel"]
            guid = channel.split("task_update:")[1]
            payload = json.loads(msg["data"])
            out = json.dumps({"guid": guid, **payload})
            await socket_manager.broadcast(guid, out)
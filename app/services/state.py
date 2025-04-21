from aiokafka import AIOKafkaProducer
from redis.asyncio import Redis

kafka_producer: AIOKafkaProducer | None = None
redis_client: Redis | None = None
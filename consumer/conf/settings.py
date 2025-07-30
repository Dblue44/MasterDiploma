from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Middleware
    MIDDLEWARE_CORS: bool = True

    # Triton
    TRITON_SERVER_HOST: str = 'localhost'
    TRITON_SERVER_PORT: int = 8100
    TRITON_SERVER_URL: str = f"{TRITON_SERVER_HOST}:{TRITON_SERVER_PORT}"
    TRITON_BATCH_SIZE: int = 4
    TRITON_OVERLAP_SIZE: int = 32

    # Redis
    REDIS_SERVER: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_CHECK_INTERVAL_SECONDS: int = 60

    # Kafka
    KAFKA_TOPIC: str = "photo-tasks"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    UPLOAD_DIR: Path = Path("uploads")
    RESULT_DIR: Path = Path("results")
    KAFKA_CONNECTION_DELAY: int = 60

    UPLOAD_DIR.mkdir(exist_ok=True)
    RESULT_DIR.mkdir(exist_ok=True)


@lru_cache
def get_settings():
    return Settings()


settings = get_settings()

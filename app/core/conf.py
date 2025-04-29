from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Uvicorn
    UVICORN_HOST: str = 'localhost'
    UVICORN_PORT: int = 8085
    UVICORN_RELOAD: bool = True

    # FastAPI
    TITLE: str = 'FastAPI'
    VERSION: str = '1.0.0'
    DESCRIPTION: str = 'FastAPITelegramBot'
    DOCS_URL: str | None = f'/docs'
    REDOCS_URL: str | None = f'/redocs'
    OPENAPI_URL: str | None = f'/openapi'

    # Middleware
    MIDDLEWARE_CORS: bool = True

    # Redis
    REDIS_SERVER: str = "localhost"
    REDIS_PORT: int = 6379
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

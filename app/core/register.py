import typing
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core import settings
from app.api import api, ws
from app.services import ImageService, KafkaService, RedisService, ConnectionManager
from app.utils import ensure_unique_route_names, simplify_operation_ids, redis_pubsub_listener


@asynccontextmanager
async def lifespan(app: FastAPI) -> typing.AsyncGenerator[None, None]:
    kafka_service = KafkaService()
    redis_service = RedisService()

    await redis_service.start()
    await kafka_service.start()
    if not kafka_service.connected:
       asyncio.create_task(kafka_service.try_reconnect_loop())

    image_service = ImageService(kafka_service, redis_service)
    socket_manager = ConnectionManager()

    app.state.socket_manager = socket_manager
    app.state.kafka_service = kafka_service
    app.state.redis_service = redis_service
    app.state.image_service = image_service

    # asyncio.create_task(retry_pending_tasks(kafka_service, redis_service))

    asyncio.create_task(redis_pubsub_listener(redis_service, socket_manager))

    yield

    await kafka_service.stop()
    await redis_service.stop()


def register_app():
    # FastAPI
    app = FastAPI(
        lifespan=lifespan,
        title=settings.TITLE,
        version=settings.VERSION,
        description=settings.DESCRIPTION,
        docs_url=settings.DOCS_URL,
        redoc_url=settings.REDOCS_URL,
        openapi_url=settings.OPENAPI_URL,
    )

    # Middlewares
    register_middleware(app)

    # Routers
    register_router(app)

    return app


def register_middleware(app: FastAPI):
    """
    Add Middewares, the execution order is from bottom to top

    :param app:
    :return:
    """

    # CORS: Always at the end
    if settings.MIDDLEWARE_CORS:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=['*'],
            allow_credentials=True,
            allow_methods=['*'],
            allow_headers=['*'],
        )


def register_router(app: FastAPI):
    """
    Routing

    :param app: FastAPI
    :return:
    """

    # API
    app.include_router(api)
    app.include_router(ws)

    # Extra
    ensure_unique_route_names(app)
    simplify_operation_ids(app)

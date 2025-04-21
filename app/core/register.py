import typing
import asyncio
from redis.asyncio import Redis
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.core import settings
from app.api import api
from fastapi.middleware.cors import CORSMiddleware
from app.services.kafka import consume_photos
from app.utils import ensure_unique_route_names, simplify_operation_ids

@asynccontextmanager
async def lifespan(app: FastAPI) -> typing.AsyncGenerator[None, None]:
    from app.services import state

    state.kafka_producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    await state.kafka_producer.start()

    state.redis_client = Redis(host=settings.REDIS_SERVER, port=settings.REDIS_PORT, decode_responses=True)
    await state.redis_client.ping()

    asyncio.create_task(consume_photos())
    yield
    if state.kafka_producer:
        await state.kafka_producer.stop()

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

    # Extra
    ensure_unique_route_names(app)
    simplify_operation_ids(app)

from fastapi import APIRouter
from app.core import settings
from app.api.v1 import upscale_v1

v1 = APIRouter(prefix=settings.API_V1_STR)

v1.include_router(upscale_v1, prefix='/react', tags=['React'])

from fastapi import APIRouter
from app.api.v1 import upscale_v1

v1 = APIRouter()

v1.include_router(upscale_v1, prefix='/react', tags=['React'])

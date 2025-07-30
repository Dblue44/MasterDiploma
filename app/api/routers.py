from fastapi import APIRouter
from app.api.v1 import upscale_v1
from app.api.ws import ws_router

v1 = APIRouter()

v1.include_router(upscale_v1, prefix='/v1', tags=['React'])

ws = APIRouter()

ws.include_router(ws_router, prefix='/ws', tags=['WebSocket'])

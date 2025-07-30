import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

ws_router = APIRouter()

@ws_router.websocket("/status")
async def websocket_endpoint(websocket: WebSocket):
    manager = websocket.app.state.socket_manager
    await manager.connect(websocket)

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            if msg.get("type") == "subscribe" and isinstance(msg.get("guids"), list):
                manager.subscribe(websocket, set(msg["guids"]))
    except WebSocketDisconnect:
        manager.disconnect(websocket)
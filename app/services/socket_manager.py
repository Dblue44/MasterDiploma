# app/services/socket_manager.py
from typing import Dict, Set
from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.subscriptions: Dict[WebSocket, Set[str]] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        self.subscriptions[websocket] = set()

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        self.subscriptions.pop(websocket, None)

    def subscribe(self, websocket: WebSocket, guids: Set[str]):
        self.subscriptions[websocket] = guids

    async def broadcast(self, task_id: str, message: str):
        dead = []
        for ws in self.active_connections:
            if task_id in self.subscriptions.get(ws, ()):
                try:
                    await ws.send_text(message)
                except:
                    dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

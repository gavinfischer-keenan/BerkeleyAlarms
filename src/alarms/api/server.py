"""FastAPI REST + WebSocket API for BerkeleyAlarms.

Endpoints:
  GET  /               — Alarm dashboard UI (serves index.html)
  GET  /alarms         — Current active alarms (JSON)
  GET  /alarms/history — Resolved alarm history (JSON)
  POST /alarms/{id}/ack     — Acknowledge an active alarm (stops Alexa repeat)
  POST /alarms/{id}/resolve — Manually resolve an alarm
  WS   /ws/alarms      — Live WebSocket feed; pushes state on every change
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import structlog
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from alarms import __version__

log = structlog.get_logger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(
    title="Berkeley Alarm Service",
    version=__version__,
    docs_url="/docs",
)

# Injected by main.py after startup
_manager = None
_store = None

# Connected WebSocket clients
_ws_clients: set[WebSocket] = set()
_ws_lock = asyncio.Lock()


def init(manager, store) -> None:
    """Called by main.py to inject the manager and store."""
    global _manager, _store
    _manager = manager
    _store = store
    # Register WebSocket broadcaster as a state-change listener
    _manager.add_listener(_broadcast_sync)


# ── WebSocket broadcaster ────────────────────────────────────────────────────

def _broadcast_sync(active_alarms) -> None:
    """Synchronous listener called from AlarmManager; schedules async broadcast."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.call_soon_threadsafe(
                lambda: asyncio.create_task(_broadcast(active_alarms))
            )
    except RuntimeError:
        pass


async def _broadcast(active_alarms) -> None:
    """Push updated alarm state to all connected WebSocket clients."""
    if not _ws_clients:
        return
    payload = json.dumps({
        "event": "state_update",
        "alarms": [a.to_dict() for a in active_alarms],
        "count": len(active_alarms),
    })
    dead: set[WebSocket] = set()
    async with _ws_lock:
        for ws in _ws_clients:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.add(ws)
        for ws in dead:
            _ws_clients.discard(ws)


# ── REST routes ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def ui() -> HTMLResponse:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html)


@app.get("/alarms", summary="Get active alarms")
async def get_active() -> dict[str, Any]:
    alarms = _manager.get_active() if _manager else []
    return {
        "alarms": [a.to_dict() for a in alarms],
        "count": len(alarms),
    }


@app.get("/alarms/history", summary="Get resolved alarm history")
async def get_history(limit: int = 50, key: str | None = None) -> dict[str, Any]:
    rows = _store.history(limit=limit, definition_key=key) if _store else []
    return {"history": rows, "count": len(rows)}


@app.get("/definitions", summary="Get configured alarm definitions")
async def get_definitions() -> dict[str, Any]:
    if not _manager:
        return {"definitions": {}}
    defs = _manager.get_definitions()
    return {
        "definitions": {
            k: {
                "name": d.name,
                "trigger_topic": d.trigger_topic,
                "severity": d.severity.value,
                "tts_template": d.tts_template,
                "repeat_interval_sec": d.repeat_interval_sec,
            }
            for k, d in defs.items()
        }
    }


@app.post("/alarms/{alarm_id}/ack", summary="Acknowledge an alarm")
async def ack_alarm(alarm_id: str) -> dict[str, str]:
    if not _manager:
        raise HTTPException(status_code=503, detail="Manager not initialized")
    ok = _manager.ack(alarm_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Alarm {alarm_id!r} not found or not active")
    return {"status": "acked", "alarm_id": alarm_id}


@app.post("/alarms/{alarm_id}/resolve", summary="Manually resolve an alarm")
async def resolve_alarm(alarm_id: str, reason: str = "manual") -> dict[str, str]:
    if not _manager:
        raise HTTPException(status_code=503, detail="Manager not initialized")
    ok = _manager.resolve(alarm_id, reason=reason)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Alarm {alarm_id!r} not found")
    return {"status": "resolved", "alarm_id": alarm_id}


@app.get("/health", summary="Health check")
async def health() -> dict[str, Any]:
    alarms = _manager.get_active() if _manager else []
    return {
        "status": "ok",
        "version": __version__,
        "active_alarms": len(alarms),
    }


# ── WebSocket endpoint ───────────────────────────────────────────────────────

@app.websocket("/ws/alarms")
async def ws_alarms(websocket: WebSocket) -> None:
    await websocket.accept()
    async with _ws_lock:
        _ws_clients.add(websocket)
    log.debug("ws.client_connected", total=len(_ws_clients))

    # Send current state immediately on connect
    if _manager:
        alarms = _manager.get_active()
        await websocket.send_text(json.dumps({
            "event": "initial_state",
            "alarms": [a.to_dict() for a in alarms],
            "count": len(alarms),
        }))

    try:
        while True:
            # Keep connection alive; client sends pings
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with _ws_lock:
            _ws_clients.discard(websocket)
        log.debug("ws.client_disconnected", remaining=len(_ws_clients))

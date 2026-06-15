"""Main entry point — wires all components and starts the service."""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import threading
import time

import structlog
import uvicorn

from alarms.config import settings


def _configure_logging() -> None:
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.log_level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


log = structlog.get_logger(__name__)

_start_time = time.time()


async def _run() -> None:
    from alarms.handlers.alexa import AlexaHandler
    from alarms.handlers.display import DisplayHandler
    from alarms.handlers.dispatcher import ChannelDispatcher
    from alarms.handlers.push import CommandPanelHandler, PushHandler
    from alarms.handlers.rotating_screen import RotatingScreenHandler
    from alarms.manager import AlarmManager
    from alarms.models import NotificationChannel
    from alarms.mqtt_client import MQTTClient
    from alarms.store import AlarmStore
    from alarms.api.server import app, init as init_api

    # ── Storage ─────────────────────────────────────────────────────────
    store = AlarmStore(settings.alarm_db_path)

    # ── MQTT client ──────────────────────────────────────────────────────
    # manager is referenced by closure — defined after dispatcher
    mqtt: MQTTClient | None = None  # forward ref; set below

    def _route(topic: str, payload: dict) -> None:
        if manager:
            manager.on_mqtt_message(topic, payload)

    mqtt = MQTTClient(on_message=_route)

    # ── Handlers ─────────────────────────────────────────────────────────
    alexa   = AlexaHandler(mqtt_publish_fn=mqtt.publish)
    display = DisplayHandler(mqtt_publish_fn=mqtt.publish)
    rotating = RotatingScreenHandler(mqtt_publish_fn=mqtt.publish)
    push    = PushHandler(mqtt_publish_fn=mqtt.publish)
    panel   = CommandPanelHandler(mqtt_publish_fn=mqtt.publish)

    # ── Channel Dispatcher ────────────────────────────────────────────────
    dispatcher = ChannelDispatcher()
    dispatcher.register(NotificationChannel.ALEXA,           alexa)
    dispatcher.register(NotificationChannel.DISPLAY_BANNER,  display)
    dispatcher.register(NotificationChannel.ROTATING_SCREEN, rotating)
    dispatcher.register(NotificationChannel.PUSH,            push)
    dispatcher.register(NotificationChannel.COMMAND_PANEL,   panel)

    # ── Manager ──────────────────────────────────────────────────────────
    manager = AlarmManager(
        config_path=settings.alarms_config_path,
        store=store,
        dispatcher=dispatcher,
    )

    # Broadcast full state on every change
    manager.add_listener(lambda alarms: display.broadcast_state(alarms))

    # ── API ───────────────────────────────────────────────────────────────
    init_api(manager, store)

    # ── Start components ─────────────────────────────────────────────────
    mqtt.start()
    await manager.start()

    log.info(
        "main.started",
        api_port=settings.alarm_api_port,
        mqtt_broker=settings.mqtt_broker,
        config=str(settings.alarms_config_path),
    )

    # ── FastAPI in background thread ──────────────────────────────────────
    api_config = uvicorn.Config(
        app,
        host=settings.alarm_api_host,
        port=settings.alarm_api_port,
        log_level=settings.log_level.lower(),
        access_log=False,
    )
    api_server = uvicorn.Server(api_config)
    api_thread = threading.Thread(target=api_server.run, daemon=True, name="api-server")
    api_thread.start()

    # ── Heartbeat task ────────────────────────────────────────────────────
    async def heartbeat() -> None:
        while True:
            await asyncio.sleep(60)
            uptime = time.time() - _start_time
            mqtt.publish_heartbeat(
                active_count=len(manager.get_active()),
                uptime_sec=uptime,
            )

    hb_task = asyncio.create_task(heartbeat(), name="heartbeat")

    # ── Wait for shutdown ─────────────────────────────────────────────────
    stop_event = asyncio.Event()

    def _signal_handler(sig: int, frame) -> None:
        log.info("main.signal_received", signal=sig)
        stop_event.set()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    await stop_event.wait()

    # ── Graceful shutdown ─────────────────────────────────────────────────
    log.info("main.shutting_down")
    hb_task.cancel()
    await manager.stop()
    mqtt.stop()
    log.info("main.stopped")


def main() -> None:
    _configure_logging()
    from alarms import __version__
    log.info("main.starting", version=__version__)
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("main.interrupted")
    sys.exit(0)


if __name__ == "__main__":
    main()

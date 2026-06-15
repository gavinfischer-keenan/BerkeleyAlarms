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
    from alarms.manager import AlarmManager
    from alarms.mqtt_client import MQTTClient
    from alarms.store import AlarmStore
    from alarms.api.server import app, init as init_api

    # ── Storage ─────────────────────────────────────────────────────────
    store = AlarmStore(settings.alarm_db_path)

    # ── MQTT client (start before manager so publish fn is ready) ───────
    mqtt = MQTTClient(on_message=lambda topic, payload: manager.on_mqtt_message(topic, payload))

    # ── Handlers ─────────────────────────────────────────────────────────
    alexa = AlexaHandler(mqtt_publish_fn=mqtt.publish)
    display = DisplayHandler(mqtt_publish_fn=mqtt.publish)

    # ── Manager ──────────────────────────────────────────────────────────
    manager = AlarmManager(
        config_path=settings.alarms_config_path,
        store=store,
        alexa_handler=alexa,
        display_handler=display,
    )

    # Register display state broadcaster as a listener
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

    # ── Start FastAPI in a background thread ──────────────────────────────
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

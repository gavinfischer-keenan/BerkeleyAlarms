"""MQTT client for BerkeleyAlarms.

Subscribes to all home/alerts/# and home/events/# topics.
Publishes to home/commands/alexa-say, home/commands/display,
home/alarms/active, and home/status/alarm-service.
"""
from __future__ import annotations

import json
import time
from typing import Any, Callable

import paho.mqtt.client as mqtt
import structlog

from alarms.config import settings
from alarms import __version__

log = structlog.get_logger(__name__)

# Topics we subscribe to
SUBSCRIBE_TOPICS = [
    ("home/alerts/#", 1),    # all alert triggers
    ("home/events/#", 1),    # auto-resolve signals (e.g. confirmed earthquake)
    ("home/commands/alarm/ack", 1),  # UI ack commands (future use)
]

STATUS_TOPIC = "home/status/alarm-service"
_connected_at: float = 0.0


class MQTTClient:
    """Wraps paho-mqtt with a simple publish interface used by handlers."""

    def __init__(self, on_message: Callable[[str, dict[str, Any]], None]) -> None:
        self._on_message_cb = on_message
        self._client = mqtt.Client(
            client_id=settings.mqtt_client_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        # LWT: mark offline on unexpected disconnect
        self._client.will_set(
            STATUS_TOPIC,
            payload=json.dumps({"status": "offline", "service": "alarm-service"}),
            qos=1,
            retain=True,
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    # ── lifecycle ───────────────────────────────────────────────────────

    def start(self) -> None:
        global _connected_at
        self._client.connect(settings.mqtt_broker, settings.mqtt_port, keepalive=60)
        self._client.loop_start()
        _connected_at = time.time()
        log.info("mqtt_client.connecting", broker=settings.mqtt_broker, port=settings.mqtt_port)

    def stop(self) -> None:
        self._publish_status("offline")
        self._client.loop_stop()
        self._client.disconnect()
        log.info("mqtt_client.stopped")

    # ── publish interface (used by handlers) ────────────────────────────

    def publish(self, topic: str, payload: dict[str, Any], qos: int = 0, retain: bool = False) -> None:
        self._client.publish(topic, json.dumps(payload, default=str), qos=qos, retain=retain)

    # ── paho callbacks ──────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        if reason_code == 0:
            log.info("mqtt_client.connected")
            for topic, qos in SUBSCRIBE_TOPICS:
                client.subscribe(topic, qos)
                log.debug("mqtt_client.subscribed", topic=topic)
            self._publish_status("online")
        else:
            log.error("mqtt_client.connect_failed", reason=reason_code)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None) -> None:
        log.warning("mqtt_client.disconnected", reason=reason_code)

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            log.warning("mqtt_client.bad_payload", topic=topic)
            return

        log.debug("mqtt_client.message", topic=topic)
        try:
            self._on_message_cb(topic, payload)
        except Exception:
            log.exception("mqtt_client.handler_error", topic=topic)

    # ── status heartbeat ────────────────────────────────────────────────

    def _publish_status(self, status: str, extra: dict | None = None) -> None:
        payload: dict[str, Any] = {
            "status": status,
            "service": "alarm-service",
            "version": __version__,
        }
        if extra:
            payload.update(extra)
        self._client.publish(STATUS_TOPIC, json.dumps(payload), qos=1, retain=True)

    def publish_heartbeat(self, active_count: int, uptime_sec: float) -> None:
        """Publish a heartbeat with current alarm count."""
        self._publish_status("online", {
            "active_alarms": active_count,
            "uptime_sec": round(uptime_sec),
        })

"""Alexa TTS output handler.

Publishes to home/commands/alexa-say.
The text payload instructs the Alexa integration (via Home Assistant) to
make a spoken announcement on all Echo devices in the home.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from alarms.models import ActiveAlarm

log = structlog.get_logger(__name__)

ALEXA_SAY_TOPIC = "home/commands/alexa-say"


class AlexaHandler:
    """Formats and publishes Alexa TTS commands."""

    def __init__(self, mqtt_publish_fn) -> None:
        """
        Args:
            mqtt_publish_fn: Callable(topic: str, payload: dict, qos: int) → None
        """
        self._publish = mqtt_publish_fn

    def announce(self, alarm: "ActiveAlarm") -> None:
        """Publish the alarm's TTS text to the Alexa command topic."""
        payload = {
            "command": "say",
            "text": alarm.tts_text,
            "alarm_id": alarm.alarm_id,
            "severity": alarm.severity.value,
        }
        self._publish(ALEXA_SAY_TOPIC, payload, qos=1)
        log.info(
            "alexa_handler.announced",
            alarm_id=alarm.alarm_id,
            text=alarm.tts_text,
            repeat=alarm.repeat_count,
        )

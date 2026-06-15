"""Rotating screen output handler (stub).

Publishes soft informational cards to home/commands/rotating-display.
These are low-urgency items that surface on the home info display
(e.g., a TV or dedicated screen cycling through metrics, camera feeds,
weather, etc.) without interrupting the household via voice.

When the rotating display service is implemented, it subscribes to
home/commands/rotating-display and renders cards according to the
command payload.

For time_critical and high-priority alarms this handler also raises
a persistent card that stays in rotation until the alarm is resolved.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import structlog

if TYPE_CHECKING:
    from alarms.models import ActiveAlarm

log = structlog.get_logger(__name__)

ROTATING_DISPLAY_TOPIC = "home/commands/rotating-display"


class RotatingScreenHandler:
    """Publishes alarm cards to the rotating info display."""

    def __init__(self, mqtt_publish_fn: Callable) -> None:
        self._publish = mqtt_publish_fn

    def announce(self, alarm: "ActiveAlarm") -> None:
        """Publish/refresh an alarm card on the rotating display."""
        payload = {
            "command": "alarm_card",
            "alarm_id": alarm.alarm_id,
            "severity": alarm.severity.value,
            "priority": alarm.priority.value,
            "title": alarm.name,
            "message": alarm.tts_text,
            "location": alarm.location,
            "state": alarm.state.value,
            "repeat_count": alarm.repeat_count,
        }
        self._publish(ROTATING_DISPLAY_TOPIC, payload, qos=0)
        log.debug("rotating_screen.card_updated", alarm_id=alarm.alarm_id)

    def raise_banner(self, alarm: "ActiveAlarm") -> None:
        """Pin a card to the display until resolved."""
        self._publish(ROTATING_DISPLAY_TOPIC, {
            "command": "pin_card",
            "alarm_id": alarm.alarm_id,
            "severity": alarm.severity.value,
            "title": alarm.name,
            "message": alarm.tts_text,
            "location": alarm.location,
        }, qos=0)

    def clear_banner(self, alarm: "ActiveAlarm") -> None:
        """Remove the pinned card when alarm is resolved."""
        self._publish(ROTATING_DISPLAY_TOPIC, {
            "command": "unpin_card",
            "alarm_id": alarm.alarm_id,
        }, qos=0)

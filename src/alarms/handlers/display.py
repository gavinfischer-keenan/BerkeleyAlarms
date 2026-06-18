"""Dashboard display output handler.

Publishes alarm banners to home/commands/display so the BerkeleyHouse
4K TV dashboard shows a visible alert overlay.
Also publishes the full current alarm state to home/alarms/active
so any subscriber (other dashboards, future mobile app) stays in sync.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Callable

import structlog

if TYPE_CHECKING:
    from alarms.models import ActiveAlarm

log = structlog.get_logger(__name__)

DISPLAY_TOPIC = "home/commands/display"
ACTIVE_STATE_TOPIC = "home/alarms/active"


class DisplayHandler:
    """Formats and publishes dashboard display commands."""

    def __init__(self, mqtt_publish_fn: Callable) -> None:
        self._publish = mqtt_publish_fn

    def raise_banner(self, alarm: "ActiveAlarm") -> None:
        """Tell the dashboard to show an alert banner for this alarm."""
        severity_map = {
            "critical": "CRITICAL",
            "warning": "WARNING",
            "advisory": "ADVISORY",
            "info": "INFO",
        }
        payload = {
            "command": "alarm_banner",
            "alarm_id": alarm.alarm_id,
            "severity": severity_map.get(alarm.severity.value, "WARNING"),
            "title": alarm.name.upper(),
            "message": alarm.tts_text,
            "location": alarm.location,
        }
        self._publish(DISPLAY_TOPIC, payload, qos=1)
        log.debug("display_handler.banner_raised", alarm_id=alarm.alarm_id)

    def clear_banner(self, alarm: "ActiveAlarm") -> None:
        """Tell the dashboard to clear this alarm's banner."""
        payload = {
            "command": "alarm_clear",
            "alarm_id": alarm.alarm_id,
        }
        self._publish(DISPLAY_TOPIC, payload, qos=1)
        log.debug("display_handler.banner_cleared", alarm_id=alarm.alarm_id)

    def broadcast_state(self, active_alarms: list["ActiveAlarm"]) -> None:
        """Publish the full current alarm state to home/alarms/active (retained)."""
        payload = {
            "alarms": [a.to_dict() for a in active_alarms],
            "count": len(active_alarms),
        }
        self._publish(ACTIVE_STATE_TOPIC, payload, qos=0, retain=True)

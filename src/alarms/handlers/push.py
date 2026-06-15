"""Push notification handler (stub).

Publishes push notification requests to home/commands/push.
A future push service (Pushover, Gotify, or similar) will subscribe
to this topic and relay notifications to mobile devices.

Priority mapping:
  time_critical → Pushover emergency priority (retries until acknowledged)
  high          → Pushover high priority
  normal        → Normal priority
  low           → Quiet / no notification sound
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import structlog

if TYPE_CHECKING:
    from alarms.models import ActiveAlarm
from alarms.models import AlarmPriority

log = structlog.get_logger(__name__)

PUSH_TOPIC = "home/commands/push"

# Map our AlarmPriority to Pushover-style numeric priority
# (-2=silent, -1=quiet, 0=normal, 1=high, 2=emergency/retry)
_PRIORITY_MAP = {
    AlarmPriority.TIME_CRITICAL: 2,   # Emergency — retries until confirmed
    AlarmPriority.HIGH: 1,            # High — bypasses quiet hours
    AlarmPriority.NORMAL: 0,          # Normal
    AlarmPriority.LOW: -1,            # Quiet — no sound
}


class PushHandler:
    """Publishes mobile push notification requests."""

    def __init__(self, mqtt_publish_fn: Callable) -> None:
        self._publish = mqtt_publish_fn

    def announce(self, alarm: "ActiveAlarm") -> None:
        """Send a push notification for the alarm."""
        push_priority = _PRIORITY_MAP.get(alarm.priority, 0)
        payload = {
            "command": "notify",
            "alarm_id": alarm.alarm_id,
            "title": alarm.name,
            "message": alarm.tts_text,
            "severity": alarm.severity.value,
            "priority": alarm.priority.value,
            "push_priority": push_priority,   # Pushover-compatible numeric priority
            "location": alarm.location,
            "sound": "siren" if push_priority >= 1 else "default",
        }
        self._publish(PUSH_TOPIC, payload, qos=1)
        log.debug("push_handler.sent", alarm_id=alarm.alarm_id, push_priority=push_priority)


class CommandPanelHandler:
    """Publishes alarm events to a future touch command panel.

    The command panel is envisioned as a dedicated touchscreen (tablet
    or in-wall panel) showing system state and accepting inputs.
    Until the panel service is built, this handler stubs the MQTT
    messages it would consume.
    """

    PANEL_TOPIC = "home/commands/panel"

    def __init__(self, mqtt_publish_fn: Callable) -> None:
        self._publish = mqtt_publish_fn

    def announce(self, alarm: "ActiveAlarm") -> None:
        self._publish(self.PANEL_TOPIC, {
            "command": "alarm_notify",
            "alarm_id": alarm.alarm_id,
            "severity": alarm.severity.value,
            "priority": alarm.priority.value,
            "title": alarm.name,
            "message": alarm.tts_text,
            "location": alarm.location,
            "state": alarm.state.value,
        }, qos=0)

    def raise_banner(self, alarm: "ActiveAlarm") -> None:
        self._publish(self.PANEL_TOPIC, {
            "command": "alarm_active",
            "alarm_id": alarm.alarm_id,
            "severity": alarm.severity.value,
            "title": alarm.name,
            "message": alarm.tts_text,
        }, qos=0)

    def clear_banner(self, alarm: "ActiveAlarm") -> None:
        self._publish(self.PANEL_TOPIC, {
            "command": "alarm_resolved",
            "alarm_id": alarm.alarm_id,
        }, qos=0)

"""Channel dispatcher — routes alarm announcements to the correct output handlers.

The dispatcher decouples the AlarmManager from specific output mechanisms.
Each alarm definition declares which channels it wants, and the dispatcher
routes to the registered handler for each channel.

Handlers not yet wired up (rotating_screen, push, command_panel) are
registered as stubs that publish to their MQTT topics — the infrastructure
subscribers for those topics don't exist yet but the messages will flow
once they do.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

from alarms.models import NotificationChannel

if TYPE_CHECKING:
    from alarms.models import ActiveAlarm

log = structlog.get_logger(__name__)


class ChannelDispatcher:
    """Routes alarm.announce() calls to the appropriate registered handlers.

    Usage
    -----
    dispatcher = ChannelDispatcher()
    dispatcher.register(NotificationChannel.ALEXA, alexa_handler)
    dispatcher.register(NotificationChannel.DISPLAY_BANNER, display_handler)
    ...

    # When an alarm fires:
    dispatcher.announce(alarm)   # routes to all channels in alarm.channels
    dispatcher.raise_banner(alarm)
    dispatcher.clear_banner(alarm)
    """

    def __init__(self) -> None:
        self._handlers: dict[NotificationChannel, Any] = {}

    def register(self, channel: NotificationChannel, handler: Any) -> None:
        """Register a handler for a notification channel."""
        self._handlers[channel] = handler
        log.debug("dispatcher.registered", channel=channel.value)

    def announce(self, alarm: "ActiveAlarm") -> None:
        """Fire the initial/repeat announcement across all of the alarm's channels."""
        for channel in alarm.channels:
            handler = self._handlers.get(channel)
            if handler is None:
                log.debug("dispatcher.no_handler", channel=channel.value)
                continue
            try:
                handler.announce(alarm)
            except Exception:
                log.exception("dispatcher.announce_error", channel=channel.value, alarm_id=alarm.alarm_id)

    def raise_banner(self, alarm: "ActiveAlarm") -> None:
        """Raise a visible alert on all applicable display channels."""
        for channel in alarm.channels:
            handler = self._handlers.get(channel)
            if handler and hasattr(handler, "raise_banner"):
                try:
                    handler.raise_banner(alarm)
                except Exception:
                    log.exception("dispatcher.banner_error", channel=channel.value)

    def clear_banner(self, alarm: "ActiveAlarm") -> None:
        """Clear alert from all display channels on resolve."""
        for channel in alarm.channels:
            handler = self._handlers.get(channel)
            if handler and hasattr(handler, "clear_banner"):
                try:
                    handler.clear_banner(alarm)
                except Exception:
                    log.exception("dispatcher.clear_error", channel=channel.value)

    def broadcast_state(self, active_alarms: list["ActiveAlarm"]) -> None:
        """Broadcast full alarm state to all handlers that support it."""
        for handler in self._handlers.values():
            if hasattr(handler, "broadcast_state"):
                try:
                    handler.broadcast_state(active_alarms)
                except Exception:
                    log.exception("dispatcher.broadcast_error")

"""Core alarm state machine and repeat scheduler.

AlarmManager is the heart of the service. It:
  - Loads alarm definitions from alarms.yml
  - Maintains in-memory active alarm state (thread-safe dict)
  - Fires the first announcement immediately on trigger
  - Runs an asyncio background task that drives repeat announcements
  - Handles auto-resolve (topic-based and timeout-based)
  - Exposes ack() and resolve() for the REST API
  - Notifies registered listeners (WebSocket broadcaster) on state change
  - Routes all output through the ChannelDispatcher
"""
from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
import yaml

from alarms.models import (
    SEVERITY_IGNORE,
    ActiveAlarm,
    AlarmDefinition,
    AlarmPriority,
    AlarmSeverity,
    AlarmState,
    AutoResolveConfig,
    NotificationChannel,
)
from alarms.store import AlarmStore

log = structlog.get_logger(__name__)

# Type alias for state-change listeners (e.g. WebSocket broadcaster)
StateListener = Callable[[list[ActiveAlarm]], None]


def _deep_get(data: dict, dotted_path: str) -> str:
    """Safely extract a value from a nested dict using a dotted key path."""
    parts = dotted_path.split(".")
    current: Any = data
    for part in parts:
        if not isinstance(current, dict):
            return ""
        current = current.get(part, "")
    return str(current) if current else ""


def _topic_matches(pattern: str, topic: str) -> bool:
    """Check if an MQTT topic string matches a pattern (with # and + wildcards)."""
    regex = re.escape(pattern).replace(r"\#", ".*").replace(r"\+", "[^/]+")
    return bool(re.fullmatch(regex, topic))


class AlarmManager:
    """Central alarm state machine."""

    def __init__(
        self,
        config_path: Path,
        store: AlarmStore,
        dispatcher,  # ChannelDispatcher
    ) -> None:
        self._definitions: dict[str, AlarmDefinition] = {}
        self._active: dict[str, ActiveAlarm] = {}   # alarm_id → ActiveAlarm
        # One active alarm per definition_key (dedup)
        self._active_by_key: dict[str, str] = {}    # definition_key → alarm_id

        self._store = store
        self._dispatcher = dispatcher
        self._listeners: list[StateListener] = []
        self._scheduler_task: asyncio.Task | None = None

        self._load_config(config_path)

    # ── config ─────────────────────────────────────────────────────────

    def _load_config(self, config_path: Path) -> None:
        with config_path.open() as f:
            raw = yaml.safe_load(f)
        for key, cfg in raw.get("alarms", {}).items():
            ar_raw = cfg.get("auto_resolve", {})
            ar = AutoResolveConfig(
                topic=ar_raw.get("topic"),
                timeout_sec=ar_raw.get("timeout_sec"),
                sensor_clear=ar_raw.get("sensor_clear", False),
            )
            # Parse channels list (default varies by priority)
            raw_channels = cfg.get("channels", [])
            channels = []
            for ch in raw_channels:
                try:
                    channels.append(NotificationChannel(ch))
                except ValueError:
                    log.warning("alarm_manager.unknown_channel", channel=ch, key=key)

            # Default channels if not specified
            if not channels:
                priority = cfg.get("priority", "normal")
                if priority in ("time_critical", "high"):
                    channels = [NotificationChannel.ALEXA, NotificationChannel.DISPLAY_BANNER]
                else:
                    channels = [NotificationChannel.DISPLAY_BANNER]

            self._definitions[key] = AlarmDefinition(
                key=key,
                name=cfg["name"],
                trigger_topic=cfg["trigger_topic"],
                severity=AlarmSeverity(cfg.get("severity", "warning")),
                priority=AlarmPriority(cfg.get("priority", "normal")),
                tts_template=cfg["tts_template"],
                repeat_interval_sec=cfg.get("repeat_interval_sec", 0),
                channels=channels,
                location_field=cfg.get("location_field"),
                auto_resolve=ar,
                severity_from_payload=cfg.get("severity_from_payload"),
                tts_from_payload=cfg.get("tts_from_payload"),
            )
        log.info("alarm_manager.config_loaded", definitions=list(self._definitions))

    # ── lifecycle ───────────────────────────────────────────────────────

    def add_listener(self, listener: StateListener) -> None:
        self._listeners.append(listener)

    def _notify(self) -> None:
        state = list(self._active.values())
        for listener in self._listeners:
            try:
                listener(state)
            except Exception:
                log.exception("alarm_manager.listener_error")

    async def start(self) -> None:
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(), name="alarm-scheduler")
        log.info("alarm_manager.started")

    async def stop(self) -> None:
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        log.info("alarm_manager.stopped")

    # ── public API ──────────────────────────────────────────────────────

    def trigger(self, definition_key: str, payload: dict[str, Any], source_topic: str) -> None:
        """Called when an MQTT alert arrives for the given definition key."""
        defn = self._definitions.get(definition_key)
        if not defn:
            log.warning("alarm_manager.unknown_definition", key=definition_key)
            return

        # ── Dynamic severity from payload ────────────────────────────────
        effective_severity = defn.severity
        if defn.severity_from_payload:
            payload_sev = _deep_get(payload, defn.severity_from_payload)
            if payload_sev:
                # Handle 'ignore' — suppress alarm entirely
                if payload_sev.lower() == SEVERITY_IGNORE:
                    log.info("alarm_manager.ignored_by_payload", key=definition_key, severity=payload_sev)
                    return
                try:
                    effective_severity = AlarmSeverity(payload_sev.lower())
                except ValueError:
                    log.warning("alarm_manager.unknown_payload_severity", severity=payload_sev, key=definition_key)

        # Extract location from payload
        location = ""
        if defn.location_field:
            location = _deep_get(payload, defn.location_field)

        # Interpolate TTS template
        tts_text = defn.tts_template
        if "{location}" in tts_text:
            tts_text = tts_text.replace("{location}", location or "unknown location")

        # Dynamic TTS from payload
        if defn.tts_from_payload:
            payload_tts = _deep_get(payload, defn.tts_from_payload)
            if payload_tts:
                tts_text = f"Alexa Announce {payload_tts}"

        # Severity-aware repeat interval override
        effective_repeat = defn.repeat_interval_sec
        if defn.severity_from_payload:  # only for payload-driven severity
            if effective_severity == AlarmSeverity.CRITICAL:
                effective_repeat = defn.repeat_interval_sec  # keep YAML value (8s for EQ)
            elif effective_severity == AlarmSeverity.WARNING:
                effective_repeat = max(defn.repeat_interval_sec, 30)  # at least 30s
            elif effective_severity == AlarmSeverity.ADVISORY:
                effective_repeat = 0  # announce once
            elif effective_severity == AlarmSeverity.INFO:
                effective_repeat = 0  # announce once, no Alexa

        # INFO severity: display only, no voice
        effective_channels = list(defn.channels)
        if effective_severity == AlarmSeverity.INFO:
            effective_channels = [c for c in effective_channels if c != NotificationChannel.ALEXA]
        if effective_severity == AlarmSeverity.ADVISORY:
            effective_channels = [c for c in effective_channels if c != NotificationChannel.ALEXA]

        # Deduplication: one active alarm per definition key
        existing_id = self._active_by_key.get(definition_key)
        if existing_id and existing_id in self._active:
            existing = self._active[existing_id]
            if existing.state in (AlarmState.ACTIVE, AlarmState.ACKED):
                existing.payload = payload
                existing.location = location
                existing.tts_text = tts_text
                existing.severity = effective_severity
                existing.channels = effective_channels
                existing.repeat_interval_sec = effective_repeat
                if existing.state == AlarmState.ACTIVE:
                    log.info("alarm_manager.retriggered", key=definition_key)
                    self._announce(existing)
                return

        # Create new alarm
        alarm = ActiveAlarm(
            alarm_id=ActiveAlarm.make_id(),
            definition_key=definition_key,
            state=AlarmState.ACTIVE,
            severity=effective_severity,
            priority=defn.priority,
            name=defn.name,
            tts_text=tts_text,
            channels=effective_channels,
            triggered_at=ActiveAlarm.now(),
            last_announced_at=None,
            repeat_count=0,
            payload=payload,
            source_topic=source_topic,
            repeat_interval_sec=effective_repeat,
            location=location,
        )
        self._active[alarm.alarm_id] = alarm
        self._active_by_key[definition_key] = alarm.alarm_id

        log.info(
            "alarm_manager.triggered",
            alarm_id=alarm.alarm_id,
            key=definition_key,
            severity=effective_severity.value,
            priority=defn.priority.value,
            location=location,
            channels=[c.value for c in effective_channels],
        )

        # Fire first announcement immediately
        self._announce(alarm)
        self._dispatcher.raise_banner(alarm)
        self._notify()

    def ack(self, alarm_id: str) -> bool:
        alarm = self._active.get(alarm_id)
        if not alarm or alarm.state != AlarmState.ACTIVE:
            return False
        alarm.state = AlarmState.ACKED
        alarm.acked_at = ActiveAlarm.now()
        log.info("alarm_manager.acked", alarm_id=alarm_id)
        self._notify()
        return True

    def resolve(self, alarm_id: str, reason: str = "manual") -> bool:
        alarm = self._active.get(alarm_id)
        if not alarm:
            return False
        alarm.state = AlarmState.RESOLVED
        alarm.resolved_at = ActiveAlarm.now()
        alarm.resolve_reason = reason

        self._dispatcher.clear_banner(alarm)
        self._store.archive(alarm)

        del self._active[alarm_id]
        if self._active_by_key.get(alarm.definition_key) == alarm_id:
            del self._active_by_key[alarm.definition_key]

        log.info("alarm_manager.resolved", alarm_id=alarm_id, reason=reason)
        self._notify()
        return True

    def get_active(self) -> list[ActiveAlarm]:
        return list(self._active.values())

    def get_definitions(self) -> dict[str, AlarmDefinition]:
        return dict(self._definitions)

    # ── MQTT message routing ────────────────────────────────────────────

    def on_mqtt_message(self, topic: str, payload: dict[str, Any]) -> None:
        """Route an incoming MQTT message to trigger or auto-resolve logic."""
        # Check triggers
        for key, defn in self._definitions.items():
            if _topic_matches(defn.trigger_topic, topic):
                self.trigger(key, payload, source_topic=topic)
                return

        # Check auto-resolve topics
        for key, defn in self._definitions.items():
            if defn.auto_resolve.topic and _topic_matches(defn.auto_resolve.topic, topic):
                alarm_id = self._active_by_key.get(key)
                if alarm_id:
                    self.resolve(alarm_id, reason=f"auto:{defn.auto_resolve.topic}")
                return

        # Check sensor_clear
        for key, defn in self._definitions.items():
            if defn.auto_resolve.sensor_clear and _topic_matches(defn.trigger_topic, topic):
                # Check for sensor-cleared indicators
                active_val = _deep_get(payload, "data.active")
                wet_val = _deep_get(payload, "data.wet")
                occupied_val = _deep_get(payload, "data.occupied")

                cleared = (
                    active_val in ("False", "false", "0")
                    or wet_val in ("False", "false", "0")
                    or occupied_val in ("False", "false", "0")
                )
                if cleared:
                    alarm_id = self._active_by_key.get(key)
                    if alarm_id:
                        alarm = self._active.get(alarm_id)
                        # Only auto-resolve after user Ack
                        if alarm and alarm.state == AlarmState.ACKED:
                            self.resolve(alarm_id, reason="sensor_clear")
                return

    # ── internals ───────────────────────────────────────────────────────

    def _announce(self, alarm: ActiveAlarm) -> None:
        """Fire an announcement across the alarm's notification channels."""
        self._dispatcher.announce(alarm)
        alarm.last_announced_at = ActiveAlarm.now()
        alarm.repeat_count += 1

    async def _scheduler_loop(self) -> None:
        """Background task: drives repeat announcements and timeout auto-resolve."""
        log.info("alarm_manager.scheduler_started")
        while True:
            await asyncio.sleep(1)
            now = datetime.now(timezone.utc)

            for alarm in list(self._active.values()):
                defn = self._definitions.get(alarm.definition_key)
                if not defn:
                    continue

                # ── auto-resolve timeout ────────────────────────────────
                if defn.auto_resolve.timeout_sec:
                    age = (now - alarm.triggered_at).total_seconds()
                    if age >= defn.auto_resolve.timeout_sec:
                        log.info(
                            "alarm_manager.timeout_resolve",
                            alarm_id=alarm.alarm_id,
                            age_sec=round(age),
                        )
                        self.resolve(alarm.alarm_id, reason="timeout")
                        continue

                # ── repeat announcement (ACTIVE only) ───────────────────
                if alarm.state != AlarmState.ACTIVE:
                    continue
                if alarm.repeat_interval_sec <= 0:
                    continue

                last = alarm.last_announced_at or alarm.triggered_at
                elapsed = (now - last).total_seconds()
                if elapsed >= alarm.repeat_interval_sec:
                    log.debug(
                        "alarm_manager.repeat",
                        alarm_id=alarm.alarm_id,
                        repeat=alarm.repeat_count,
                    )
                    self._announce(alarm)
                    self._notify()

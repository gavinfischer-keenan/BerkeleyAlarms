"""Core alarm state machine and repeat scheduler.

AlarmManager is the heart of the service. It:
  - Loads alarm definitions from alarms.yml
  - Maintains in-memory active alarm state (thread-safe dict)
  - Fires the first announcement immediately on trigger
  - Runs an asyncio background task that drives repeat announcements
  - Handles auto-resolve (topic-based and timeout-based)
  - Exposes ack() and resolve() for the REST API
  - Notifies registered listeners (WebSocket broadcaster) on state change
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
    ActiveAlarm,
    AlarmDefinition,
    AlarmSeverity,
    AlarmState,
    AutoResolveConfig,
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
    # Convert MQTT wildcards to regex
    regex = re.escape(pattern).replace(r"\#", ".*").replace(r"\+", "[^/]+")
    return bool(re.fullmatch(regex, topic))


class AlarmManager:
    """Central alarm state machine."""

    def __init__(
        self,
        config_path: Path,
        store: AlarmStore,
        alexa_handler,
        display_handler,
    ) -> None:
        self._definitions: dict[str, AlarmDefinition] = {}
        self._active: dict[str, ActiveAlarm] = {}   # alarm_id → ActiveAlarm
        # Also track: one active alarm per definition_key (dedup)
        # If an earthquake alarm is already ACTIVE, retrigger refreshes payload but
        # does not create a second alarm.
        self._active_by_key: dict[str, str] = {}    # definition_key → alarm_id

        self._store = store
        self._alexa = alexa_handler
        self._display = display_handler
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
            self._definitions[key] = AlarmDefinition(
                key=key,
                name=cfg["name"],
                trigger_topic=cfg["trigger_topic"],
                severity=AlarmSeverity(cfg.get("severity", "warning")),
                tts_template=cfg["tts_template"],
                repeat_interval_sec=cfg.get("repeat_interval_sec", 0),
                location_field=cfg.get("location_field"),
                auto_resolve=ar,
            )
        log.info("alarm_manager.config_loaded", definitions=list(self._definitions))

    # ── lifecycle ───────────────────────────────────────────────────────

    def add_listener(self, listener: StateListener) -> None:
        """Register a callback invoked whenever active alarm state changes."""
        self._listeners.append(listener)

    def _notify(self) -> None:
        state = list(self._active.values())
        for listener in self._listeners:
            try:
                listener(state)
            except Exception:
                log.exception("alarm_manager.listener_error")

    async def start(self) -> None:
        """Start the repeat-announcement scheduler loop."""
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(), name="alarm-scheduler")
        log.info("alarm_manager.started")

    async def stop(self) -> None:
        """Stop the scheduler and resolve all active alarms."""
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        log.info("alarm_manager.stopped")

    # ── public API ──────────────────────────────────────────────────────

    def trigger(self, definition_key: str, payload: dict[str, Any], source_topic: str) -> None:
        """Called when an MQTT alert arrives for the given definition key.

        If no alarm of this type is currently ACTIVE/ACKED, creates one.
        If one already exists and is ACTIVE, refreshes its payload and
        fires an immediate re-announcement (e.g. stronger EQ reading).
        """
        defn = self._definitions.get(definition_key)
        if not defn:
            log.warning("alarm_manager.unknown_definition", key=definition_key)
            return

        # Extract location from payload
        location = ""
        if defn.location_field:
            location = _deep_get(payload, defn.location_field)

        # Interpolate TTS template
        tts_text = defn.tts_template
        if "{location}" in tts_text:
            tts_text = tts_text.replace("{location}", location or "unknown location")

        # Deduplication: one active alarm per definition key
        existing_id = self._active_by_key.get(definition_key)
        if existing_id and existing_id in self._active:
            existing = self._active[existing_id]
            if existing.state in (AlarmState.ACTIVE, AlarmState.ACKED):
                # Refresh payload (may have updated magnitude etc.) — do NOT re-announce
                # for ACKED alarms (user silenced it). DO re-announce for ACTIVE ones.
                existing.payload = payload
                existing.location = location
                existing.tts_text = tts_text
                if existing.state == AlarmState.ACTIVE:
                    log.info(
                        "alarm_manager.retriggered",
                        key=definition_key,
                        alarm_id=existing_id,
                    )
                    # Re-announce immediately with updated data
                    self._announce(existing)
                return

        # Create new alarm
        alarm = ActiveAlarm(
            alarm_id=ActiveAlarm.make_id(),
            definition_key=definition_key,
            state=AlarmState.ACTIVE,
            severity=defn.severity,
            name=defn.name,
            tts_text=tts_text,
            triggered_at=ActiveAlarm.now(),
            last_announced_at=None,
            repeat_count=0,
            payload=payload,
            source_topic=source_topic,
            location=location,
        )
        self._active[alarm.alarm_id] = alarm
        self._active_by_key[definition_key] = alarm.alarm_id

        log.info(
            "alarm_manager.triggered",
            alarm_id=alarm.alarm_id,
            key=definition_key,
            severity=defn.severity.value,
            location=location,
        )

        # Fire first announcement immediately
        self._announce(alarm)
        self._display.raise_banner(alarm)
        self._notify()

    def ack(self, alarm_id: str) -> bool:
        """User acknowledges alarm — stops Alexa repeat but keeps UI badge."""
        alarm = self._active.get(alarm_id)
        if not alarm:
            return False
        if alarm.state != AlarmState.ACTIVE:
            return False
        alarm.state = AlarmState.ACKED
        alarm.acked_at = ActiveAlarm.now()
        log.info("alarm_manager.acked", alarm_id=alarm_id)
        self._notify()
        return True

    def resolve(self, alarm_id: str, reason: str = "manual") -> bool:
        """Resolve an alarm — clears it from active state, archives to history."""
        alarm = self._active.get(alarm_id)
        if not alarm:
            return False
        alarm.state = AlarmState.RESOLVED
        alarm.resolved_at = ActiveAlarm.now()
        alarm.resolve_reason = reason

        self._display.clear_banner(alarm)
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

        # Check sensor_clear: e.g. home/alerts/leak/sensor-01 with wet:false
        for key, defn in self._definitions.items():
            if defn.auto_resolve.sensor_clear and _topic_matches(defn.trigger_topic, topic):
                # If wet is now False (sensor cleared), resolve after user has acked
                wet = _deep_get(payload, "data.wet")
                occupied = _deep_get(payload, "data.occupied")
                cleared = (wet == "False" or wet is False) or (
                    occupied == "False" or occupied is False
                )
                if cleared:
                    alarm_id = self._active_by_key.get(key)
                    if alarm_id:
                        alarm = self._active.get(alarm_id)
                        # Only auto-resolve if user has already acknowledged
                        if alarm and alarm.state == AlarmState.ACKED:
                            self.resolve(alarm_id, reason="sensor_clear")
                return

    # ── internals ───────────────────────────────────────────────────────

    def _announce(self, alarm: ActiveAlarm) -> None:
        """Fire an Alexa TTS announcement for the alarm."""
        self._alexa.announce(alarm)
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

                # ── repeat announcement (ACTIVE only — not ACKED) ───────
                if alarm.state != AlarmState.ACTIVE:
                    continue
                if defn.repeat_interval_sec <= 0:
                    continue

                last = alarm.last_announced_at or alarm.triggered_at
                elapsed = (now - last).total_seconds()
                if elapsed >= defn.repeat_interval_sec:
                    log.debug(
                        "alarm_manager.repeat",
                        alarm_id=alarm.alarm_id,
                        repeat=alarm.repeat_count,
                        elapsed_sec=round(elapsed),
                    )
                    self._announce(alarm)
                    self._notify()

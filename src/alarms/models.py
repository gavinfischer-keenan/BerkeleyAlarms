"""Data models for alarm definitions and active alarm instances."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class AlarmState(str, Enum):
    ACTIVE = "active"       # Repeating announcements
    ACKED = "acked"         # User acknowledged — silent but visible in UI
    RESOLVED = "resolved"   # Closed (archived to history)


class AlarmSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


# ── Alarm definition (loaded from alarms.yml) ──────────────────────────────

@dataclass
class AutoResolveConfig:
    topic: str | None = None          # resolve when message arrives on this topic
    timeout_sec: int | None = None    # resolve after this many seconds
    sensor_clear: bool = False        # resolve when payload contains wet/occupied: false


@dataclass
class AlarmDefinition:
    key: str                              # e.g. "earthquake", "leak"
    name: str
    trigger_topic: str                    # MQTT topic pattern (# wildcards OK)
    severity: AlarmSeverity
    tts_template: str                     # Alexa TTS text; {location} interpolated
    repeat_interval_sec: int              # 0 = no repeat
    location_field: str | None = None    # dotted path into payload, e.g. "data.location"
    auto_resolve: AutoResolveConfig = field(default_factory=AutoResolveConfig)


# ── Active alarm instance ──────────────────────────────────────────────────

@dataclass
class ActiveAlarm:
    alarm_id: str
    definition_key: str
    state: AlarmState
    severity: AlarmSeverity
    name: str
    tts_text: str                      # resolved TTS (location already interpolated)
    triggered_at: datetime
    last_announced_at: datetime | None
    repeat_count: int
    payload: dict[str, Any]           # original MQTT payload
    source_topic: str                 # the exact topic that triggered this alarm
    location: str = ""
    acked_at: datetime | None = None
    resolved_at: datetime | None = None
    resolve_reason: str = ""

    # ── convenience ────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        return {
            "alarm_id": self.alarm_id,
            "definition_key": self.definition_key,
            "state": self.state.value,
            "severity": self.severity.value,
            "name": self.name,
            "tts_text": self.tts_text,
            "triggered_at": self.triggered_at.isoformat(),
            "last_announced_at": self.last_announced_at.isoformat()
            if self.last_announced_at else None,
            "repeat_count": self.repeat_count,
            "location": self.location,
            "source_topic": self.source_topic,
            "acked_at": self.acked_at.isoformat() if self.acked_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "resolve_reason": self.resolve_reason,
        }

    @staticmethod
    def make_id() -> str:
        return str(uuid.uuid4())[:12]

    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc)

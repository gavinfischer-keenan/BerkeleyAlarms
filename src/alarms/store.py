"""SQLite-backed alarm history store.

Active alarms live in memory (managed by AlarmManager).
Resolved alarms are archived here for the history UI.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from alarms.models import ActiveAlarm

log = structlog.get_logger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS alarm_history (
    alarm_id        TEXT PRIMARY KEY,
    definition_key  TEXT NOT NULL,
    severity        TEXT NOT NULL,
    name            TEXT NOT NULL,
    tts_text        TEXT NOT NULL,
    location        TEXT DEFAULT '',
    source_topic    TEXT DEFAULT '',
    triggered_at    TEXT NOT NULL,
    acked_at        TEXT,
    resolved_at     TEXT NOT NULL,
    resolve_reason  TEXT DEFAULT '',
    repeat_count    INTEGER DEFAULT 0,
    payload         TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_ah_resolved ON alarm_history(resolved_at);
CREATE INDEX IF NOT EXISTS idx_ah_key      ON alarm_history(definition_key);
"""


class AlarmStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db_path = db_path
        self._lock = threading.Lock()
        self._init_db()
        log.info("alarm_store.ready", path=str(db_path))

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._lock, self._get_conn() as conn:
            conn.executescript(_SCHEMA)

    # ── write ───────────────────────────────────────────────────────────

    def archive(self, alarm: ActiveAlarm) -> None:
        """Archive a resolved alarm to history."""
        with self._lock, self._get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO alarm_history
                    (alarm_id, definition_key, severity, name, tts_text, location,
                     source_topic, triggered_at, acked_at, resolved_at,
                     resolve_reason, repeat_count, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    alarm.alarm_id,
                    alarm.definition_key,
                    alarm.severity.value,
                    alarm.name,
                    alarm.tts_text,
                    alarm.location,
                    alarm.source_topic,
                    alarm.triggered_at.isoformat(),
                    alarm.acked_at.isoformat() if alarm.acked_at else None,
                    (alarm.resolved_at or datetime.now(timezone.utc)).isoformat(),
                    alarm.resolve_reason,
                    alarm.repeat_count,
                    json.dumps(alarm.payload),
                ),
            )
        log.debug("alarm_store.archived", alarm_id=alarm.alarm_id)

    # ── read ────────────────────────────────────────────────────────────

    def history(self, limit: int = 50, definition_key: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if definition_key:
            clauses.append("definition_key = ?")
            params.append(definition_key)
        where = " AND ".join(clauses) if clauses else "1=1"
        params.append(limit)
        with self._get_conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM alarm_history WHERE {where} ORDER BY resolved_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

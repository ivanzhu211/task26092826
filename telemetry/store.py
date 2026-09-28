"""SQLite persistence layer: schema and low-level operations.

All multi-statement mutations are executed inside a single IMMEDIATE
transaction by the caller (TelemetryService), so a crashed process leaves
each batch either fully applied or not applied at all.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Iterator, Optional, Sequence

from .models import HOUR_MS, MetricSpec, Retention

_SCHEMA = """
CREATE TABLE IF NOT EXISTS metrics (
    name TEXT PRIMARY KEY,
    allowed_tag_keys TEXT NOT NULL,
    min_value REAL NOT NULL,
    max_value REAL NOT NULL,
    interval_ms INTEGER NOT NULL,
    retention TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS series (
    metric TEXT NOT NULL,
    series_key TEXT NOT NULL,
    min_ts INTEGER NOT NULL,
    max_ts INTEGER NOT NULL,
    PRIMARY KEY (metric, series_key)
);
CREATE TABLE IF NOT EXISTS points (
    metric TEXT NOT NULL,
    series_key TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    value REAL NOT NULL,
    tags_json TEXT NOT NULL,
    PRIMARY KEY (metric, series_key, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_points_metric_ts ON points (metric, ts_ms);
CREATE TABLE IF NOT EXISTS occupied_slots (
    metric TEXT NOT NULL,
    series_key TEXT NOT NULL,
    slot_ms INTEGER NOT NULL,
    PRIMARY KEY (metric, series_key, slot_ms)
);
CREATE INDEX IF NOT EXISTS idx_slots_metric_slot ON occupied_slots (metric, slot_ms);
CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    value REAL NOT NULL,
    tags_json TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quarantine_metric_ts ON quarantine (metric, ts_ms);
CREATE TABLE IF NOT EXISTS hourly_stats (
    metric TEXT NOT NULL,
    hour_ms INTEGER NOT NULL,
    stored INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    quarantined INTEGER NOT NULL DEFAULT 0,
    occupied_slots INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (metric, hour_ms)
);
"""


class Store:
    """Thread-safe wrapper around a single SQLite connection."""

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self.lock = threading.RLock()

    def close(self) -> None:
        with self.lock:
            self._conn.close()

    def begin(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    # -- metrics ---------------------------------------------------------

    def insert_metric(self, spec: MetricSpec) -> None:
        self._conn.execute(
            "INSERT INTO metrics (name, allowed_tag_keys, min_value, max_value,"
            " interval_ms, retention) VALUES (?, ?, ?, ?, ?, ?)",
            (
                spec.name,
                json.dumps(sorted(spec.allowed_tag_keys)),
                spec.min_value,
                spec.max_value,
                spec.interval_ms,
                spec.retention.value,
            ),
        )

    def get_metric(self, name: str) -> Optional[MetricSpec]:
        row = self._conn.execute(
            "SELECT name, allowed_tag_keys, min_value, max_value, interval_ms,"
            " retention FROM metrics WHERE name = ?",
            (name,),
        ).fetchone()
        if row is None:
            return None
        return MetricSpec(
            name=row[0],
            allowed_tag_keys=frozenset(json.loads(row[1])),
            min_value=row[2],
            max_value=row[3],
            interval_ms=row[4],
            retention=Retention(row[5]),
        )

    def list_metrics(self) -> list[MetricSpec]:
        rows = self._conn.execute(
            "SELECT name, allowed_tag_keys, min_value, max_value, interval_ms,"
            " retention FROM metrics"
        ).fetchall()
        return [
            MetricSpec(
                name=r[0],
                allowed_tag_keys=frozenset(json.loads(r[1])),
                min_value=r[2],
                max_value=r[3],
                interval_ms=r[4],
                retention=Retention(r[5]),
            )
            for r in rows
        ]

    # -- series ----------------------------------------------------------

    def get_series_bounds(self, metric: str, series_key: str) -> Optional[tuple[int, int]]:
        row = self._conn.execute(
            "SELECT min_ts, max_ts FROM series WHERE metric = ? AND series_key = ?",
            (metric, series_key),
        ).fetchone()
        return (row[0], row[1]) if row else None

    def count_series(self, metric: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM series WHERE metric = ?", (metric,)
        ).fetchone()
        return row[0]

    def insert_series(self, metric: str, series_key: str, ts_ms: int) -> None:
        self._conn.execute(
            "INSERT INTO series (metric, series_key, min_ts, max_ts) VALUES (?, ?, ?, ?)",
            (metric, series_key, ts_ms, ts_ms),
        )

    def update_series_bounds(
        self, metric: str, series_key: str, min_ts: int, max_ts: int
    ) -> None:
        self._conn.execute(
            "UPDATE series SET min_ts = ?, max_ts = ? WHERE metric = ? AND series_key = ?",
            (min_ts, max_ts, metric, series_key),
        )

    def series_bounds_for_metric(self, metric: str) -> list[tuple[int, int]]:
        rows = self._conn.execute(
            "SELECT min_ts, max_ts FROM series WHERE metric = ?", (metric,)
        ).fetchall()
        return [(r[0], r[1]) for r in rows]

    # -- points ----------------------------------------------------------

    def try_insert_point(
        self, metric: str, series_key: str, ts_ms: int, value: float, tags_json: str
    ) -> bool:
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO points (metric, series_key, ts_ms, value, tags_json)"
            " VALUES (?, ?, ?, ?, ?)",
            (metric, series_key, ts_ms, value, tags_json),
        )
        return cur.rowcount == 1

    def try_insert_slot(self, metric: str, series_key: str, slot_ms: int) -> bool:
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO occupied_slots (metric, series_key, slot_ms)"
            " VALUES (?, ?, ?)",
            (metric, series_key, slot_ms),
        )
        return cur.rowcount == 1

    def read_points(
        self, metric: str, start_ms: int, end_ms: int
    ) -> list[tuple[int, float, str]]:
        rows = self._conn.execute(
            "SELECT ts_ms, value, tags_json FROM points"
            " WHERE metric = ? AND ts_ms >= ? AND ts_ms < ? ORDER BY ts_ms",
            (metric, start_ms, end_ms),
        ).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]

    # -- quarantine ------------------------------------------------------

    def insert_quarantine(
        self, metric: str, ts_ms: int, value: float, tags_json: str, reason: str
    ) -> None:
        self._conn.execute(
            "INSERT INTO quarantine (metric, ts_ms, value, tags_json, reason)"
            " VALUES (?, ?, ?, ?, ?)",
            (metric, ts_ms, value, tags_json, reason),
        )

    def read_quarantine(
        self, metric: str, start_ms: int, end_ms: int
    ) -> list[tuple[int, float, str, str]]:
        rows = self._conn.execute(
            "SELECT ts_ms, value, tags_json, reason FROM quarantine"
            " WHERE metric = ? AND ts_ms >= ? AND ts_ms < ? ORDER BY id",
            (metric, start_ms, end_ms),
        ).fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in rows]

    # -- hourly stats ----------------------------------------------------

    def bump_hourly_stats(
        self,
        metric: str,
        hour_ms: int,
        stored: int = 0,
        duplicates: int = 0,
        quarantined: int = 0,
        occupied_slots: int = 0,
    ) -> None:
        self._conn.execute(
            "INSERT INTO hourly_stats (metric, hour_ms, stored, duplicates,"
            " quarantined, occupied_slots) VALUES (?, ?, ?, ?, ?, ?)"
            " ON CONFLICT (metric, hour_ms) DO UPDATE SET"
            " stored = stored + excluded.stored,"
            " duplicates = duplicates + excluded.duplicates,"
            " quarantined = quarantined + excluded.quarantined,"
            " occupied_slots = occupied_slots + excluded.occupied_slots",
            (metric, hour_ms, stored, duplicates, quarantined, occupied_slots),
        )

    def get_hourly_stats(
        self, metric: str, hours: Sequence[int]
    ) -> dict[int, tuple[int, int, int, int]]:
        if not hours:
            return {}
        placeholders = ",".join("?" for _ in hours)
        rows = self._conn.execute(
            f"SELECT hour_ms, stored, duplicates, quarantined, occupied_slots"
            f" FROM hourly_stats WHERE metric = ? AND hour_ms IN ({placeholders})",
            (metric, *hours),
        ).fetchall()
        return {r[0]: (r[1], r[2], r[3], r[4]) for r in rows}

    # -- cleanup ---------------------------------------------------------

    def delete_expired(self, metric: str, boundary_ms: int) -> None:
        """Delete points, quarantine, slots and stats in expired hours."""
        self._conn.execute(
            "DELETE FROM points WHERE metric = ? AND ts_ms < ?", (metric, boundary_ms)
        )
        self._conn.execute(
            "DELETE FROM quarantine WHERE metric = ? AND ts_ms < ?",
            (metric, boundary_ms),
        )
        self._conn.execute(
            "DELETE FROM occupied_slots WHERE metric = ? AND slot_ms < ?",
            (metric, boundary_ms),
        )
        self._conn.execute(
            "DELETE FROM hourly_stats WHERE metric = ? AND hour_ms < ?",
            (metric, boundary_ms),
        )

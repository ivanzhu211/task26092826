"""SQLite 存储层：连接与 schema。

设计要点：
- 单连接 + WAL；所有访问由调用方（Service）持锁串行化。
- 每个批次 / 每次清理各在一个事务里完成，崩溃后要么整批生效、要么完全没生效。
- series_hour 冗余 metric 列，报表按 (metric, hour) 直接索引汇总，不随点数增长。
"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS metrics (
    name          TEXT PRIMARY KEY,
    tag_keys      TEXT NOT NULL,          -- JSON array
    min_value     REAL NOT NULL,
    max_value     REAL NOT NULL,
    interval_ms   INTEGER NOT NULL,
    retention_ms  INTEGER                 -- NULL = 永久
);

CREATE TABLE IF NOT EXISTS series (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    metric     TEXT NOT NULL,
    tags_key   TEXT NOT NULL,             -- 排序后的 k=v 连接串，区分大小写
    tags_json  TEXT NOT NULL,
    first_ms   INTEGER,                   -- 已存点的最早/最晚时间；清理不改变
    last_ms    INTEGER,
    UNIQUE (metric, tags_key)
);

CREATE TABLE IF NOT EXISTS points (
    series_id  INTEGER NOT NULL,
    ts_ms      INTEGER NOT NULL,
    value      REAL NOT NULL,
    hour_ms    INTEGER NOT NULL,
    UNIQUE (series_id, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_points_series_ts ON points (series_id, ts_ms);

CREATE TABLE IF NOT EXISTS quarantine (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    metric     TEXT NOT NULL,
    ts_ms      INTEGER NOT NULL,
    hour_ms    INTEGER NOT NULL,
    value      REAL NOT NULL,
    tags_json  TEXT NOT NULL,
    reason     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quarantine_metric_ts ON quarantine (metric, ts_ms);

CREATE TABLE IF NOT EXISTS hourly_stats (
    metric       TEXT NOT NULL,
    hour_ms      INTEGER NOT NULL,
    stored       INTEGER NOT NULL DEFAULT 0,
    duplicates   INTEGER NOT NULL DEFAULT 0,
    quarantined  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (metric, hour_ms)
);

-- 每条序列每个小时已填槽数（去重后的槽）
CREATE TABLE IF NOT EXISTS series_hour (
    metric     TEXT NOT NULL,
    series_id  INTEGER NOT NULL,
    hour_ms    INTEGER NOT NULL,
    filled     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (series_id, hour_ms)
);
CREATE INDEX IF NOT EXISTS idx_series_hour_metric_hour
    ON series_hour (metric, hour_ms);
"""


class Storage:
    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

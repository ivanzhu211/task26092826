"""TelemetryService：注册指标、批量写入、小时报告、清理、读回。

并发模型：单进程内一把 RLock 串行化所有 DB 访问；每批写入、每次清理
各为一个事务，因此崩溃后任一批要么完整生效、要么完全没生效。
报告读取看到的是事务边界后的完整快照，不会看到某批的一部分。
"""
from __future__ import annotations

import json
import threading
from typing import Optional

from .models import (
    HOUR_MS,
    MAX_BATCH_SIZE,
    MAX_SERIES_PER_METRIC,
    BatchResult,
    DataPoint,
    HourlyReport,
    HourStatus,
    MetricConfig,
    QuarantineReason,
    QuarantinedRecord,
    RejectedRecord,
)
from .storage import Storage
from .validation import check_format, hour_of, slot_of, valid_metric_name

_VALID_RETENTION_DAYS = (30, 90, None)


def _canon_tags_key(tags: dict) -> str:
    """标签集合的规范串：与顺序无关、区分大小写、无分隔符歧义。"""
    return json.dumps(sorted(tags.items()), ensure_ascii=False,
                      separators=(",", ":"))


class TelemetryService:
    def __init__(self, db_path: str) -> None:
        self._storage = Storage(db_path)
        self._lock = threading.RLock()
        self._metrics: dict[str, MetricConfig] = {}
        # (metric, tags_key) -> [series_id, first_ms, last_ms]
        self._series: dict[tuple[str, str], list] = {}
        self._series_metric_by_id: dict[int, str] = {}
        self._dirty_ids: set[int] = set()
        self._load_caches()

    def _load_caches(self) -> None:
        conn = self._storage.conn
        for row in conn.execute(
                "SELECT name, tag_keys, min_value, max_value, interval_ms,"
                " retention_ms FROM metrics"):
            self._metrics[row[0]] = MetricConfig(
                name=row[0],
                tag_keys=tuple(json.loads(row[1])),
                min_value=row[2],
                max_value=row[3],
                interval_ms=row[4],
                retention_ms=row[5],
            )
        for row in conn.execute(
                "SELECT id, metric, tags_key, first_ms, last_ms FROM series"):
            self._series[(row[1], row[2])] = [row[0], row[3], row[4]]
            self._series_metric_by_id[row[0]] = row[1]

    def close(self) -> None:
        with self._lock:
            self._storage.close()

    # ---------------- 注册 ----------------

    def register_metric(self, name, tag_keys, min_value, max_value,
                        interval_ms, retention_days) -> None:
        if not valid_metric_name(name):
            raise ValueError(f"invalid metric name: {name!r}")
        if retention_days not in _VALID_RETENTION_DAYS:
            raise ValueError("retention_days must be 30, 90 or None")
        if not isinstance(interval_ms, int) or isinstance(interval_ms, bool) \
                or not 1000 <= interval_ms <= HOUR_MS \
                or HOUR_MS % interval_ms != 0:
            raise ValueError("interval_ms must divide 1 hour and be within"
                             " [1000, 3600000]")
        keys = tuple(tag_keys)
        for key in keys:
            if not isinstance(key, str) or not key or len(key) > 64:
                raise ValueError(f"invalid tag key: {key!r}")
        if len(set(keys)) != len(keys):
            raise ValueError("duplicate tag keys")
        lo, hi = float(min_value), float(max_value)
        if not (lo <= hi):
            raise ValueError("min_value must be <= max_value")
        retention_ms = (None if retention_days is None
                        else retention_days * 86_400_000)
        with self._lock:
            if name in self._metrics:
                raise ValueError(f"metric already registered: {name}")
            self._storage.conn.execute(
                "INSERT INTO metrics"
                " (name, tag_keys, min_value, max_value, interval_ms,"
                "  retention_ms) VALUES (?,?,?,?,?,?)",
                (name, json.dumps(list(keys), ensure_ascii=False),
                 lo, hi, interval_ms, retention_ms))
            self._storage.conn.commit()
            self._metrics[name] = MetricConfig(
                name=name, tag_keys=keys, min_value=lo, max_value=hi,
                interval_ms=interval_ms, retention_ms=retention_ms)

    def metric_config(self, name: str) -> Optional[MetricConfig]:
        return self._metrics.get(name)

    # ---------------- 批量写入 ----------------

    def write_batch(self, points, now_ms: int) -> BatchResult:
        points = list(points)
        result = BatchResult()
        if not 1 <= len(points) <= MAX_BATCH_SIZE:
            result.batch_rejected = True
            return result
        with self._lock:
            valid: list[tuple[int, DataPoint, MetricConfig]] = []
            for index, point in enumerate(points):
                config = (self._metrics.get(point.metric)
                          if isinstance(point.metric, str) else None)
                reason = check_format(point, config, now_ms)
                if reason is None:
                    valid.append((index, point, config))
                else:
                    result.rejected.append(RejectedRecord(index, point, reason))
            # 超过 10%（严格大于）整批拒绝，什么都不生效
            if len(result.rejected) * 10 > len(points):
                result.batch_rejected = True
                return result
            self._apply_batch(valid, result)
            return result

    def _apply_batch(self, valid, result: BatchResult) -> None:
        conn = self._storage.conn
        # 每小时统计增量：(metric, hour) -> [stored, dup, quar]
        stats: dict[tuple[str, int], list[int]] = {}
        # 每序列每小时新填槽数：(series_id, hour) -> n
        fills: dict[tuple[int, int], int] = {}
        try:
            for index, point, config in valid:
                quar_reason = self._quarantine_reason(point, config)
                hour = hour_of(point.ts_ms)
                key = (point.metric, hour)
                bucket = stats.setdefault(key, [0, 0, 0])
                if quar_reason is not None:
                    conn.execute(
                        "INSERT INTO quarantine"
                        " (metric, ts_ms, hour_ms, value, tags_json, reason)"
                        " VALUES (?,?,?,?,?,?)",
                        (point.metric, point.ts_ms, hour, point.value,
                         json.dumps(point.tags, ensure_ascii=False),
                         quar_reason.value))
                    bucket[2] += 1
                    result.quarantined.append(
                        QuarantinedRecord(index, point, quar_reason))
                    continue
                series = self._get_or_create_series(point)
                series_id = series[0]
                dup = conn.execute(
                    "SELECT 1 FROM points WHERE series_id=? AND ts_ms=?",
                    (series_id, point.ts_ms)).fetchone()
                if dup is not None:
                    bucket[1] += 1
                    result.duplicates += 1
                    continue
                interval = config.interval_ms
                slot_start = slot_of(point.ts_ms, interval) * interval
                slot_filled = conn.execute(
                    "SELECT 1 FROM points WHERE series_id=? AND ts_ms>=?"
                    " AND ts_ms<? LIMIT 1",
                    (series_id, slot_start, slot_start + interval)).fetchone()
                conn.execute(
                    "INSERT INTO points (series_id, ts_ms, value, hour_ms)"
                    " VALUES (?,?,?,?)",
                    (series_id, point.ts_ms, point.value, hour))
                if slot_filled is None:
                    fkey = (series_id, hour)
                    fills[fkey] = fills.get(fkey, 0) + 1
                bucket[0] += 1
                result.stored += 1
                if series[1] is None or point.ts_ms < series[1]:
                    series[1] = point.ts_ms
                if series[2] is None or point.ts_ms > series[2]:
                    series[2] = point.ts_ms
                self._dirty_ids.add(series_id)
            for (metric, hour), (stored, dup, quar) in stats.items():
                conn.execute(
                    "INSERT INTO hourly_stats"
                    " (metric, hour_ms, stored, duplicates, quarantined)"
                    " VALUES (?,?,?,?,?)"
                    " ON CONFLICT (metric, hour_ms) DO UPDATE SET"
                    " stored=stored+excluded.stored,"
                    " duplicates=duplicates+excluded.duplicates,"
                    " quarantined=quarantined+excluded.quarantined",
                    (metric, hour, stored, dup, quar))
            for (series_id, hour), n in fills.items():
                metric = self._series_metric_by_id[series_id]
                conn.execute(
                    "INSERT INTO series_hour (metric, series_id, hour_ms,"
                    " filled) VALUES (?,?,?,?)"
                    " ON CONFLICT (series_id, hour_ms) DO UPDATE SET"
                    " filled=filled+excluded.filled",
                    (metric, series_id, hour, n))
            for series_id in self._dirty_ids:
                entry = self._series_entry_by_id(series_id)
                conn.execute(
                    "UPDATE series SET first_ms=?, last_ms=? WHERE id=?",
                    (entry[1], entry[2], series_id))
            self._dirty_ids.clear()
            conn.commit()
        except BaseException:
            conn.rollback()
            # 事务回滚后内存缓存可能与库不一致（新建序列、first/last），
            # 直接从库重建，保证缓存永远与已提交数据一致。
            self._metrics.clear()
            self._series.clear()
            self._series_metric_by_id.clear()
            self._dirty_ids.clear()
            self._load_caches()
            raise

    def _series_entry_by_id(self, series_id: int) -> list:
        for (metric, _), entry in self._series.items():
            if entry[0] == series_id:
                return entry
        raise KeyError(series_id)

    def _quarantine_reason(self, point: DataPoint,
                           config: MetricConfig) -> Optional[QuarantineReason]:
        allowed = set(config.tag_keys)
        for key in point.tags:
            if key not in allowed:
                return QuarantineReason.UNREGISTERED_TAG_KEY
        if not (config.min_value <= point.value <= config.max_value):
            return QuarantineReason.VALUE_OUT_OF_RANGE
        return None

    def _get_or_create_series(self, point: DataPoint) -> list:
        key = (point.metric, _canon_tags_key(point.tags))
        series = self._series.get(key)
        if series is not None:
            return series
        count = sum(1 for (metric, _) in self._series
                    if metric == point.metric)
        if count >= MAX_SERIES_PER_METRIC:
            raise ValueError(
                f"series limit exceeded for metric {point.metric}")
        cur = self._storage.conn.execute(
            "INSERT INTO series (metric, tags_key, tags_json, first_ms,"
            " last_ms) VALUES (?,?,?,?,?)",
            (point.metric, key[1],
             json.dumps(point.tags, ensure_ascii=False), None, None))
        series = [cur.lastrowid, None, None]
        self._series[key] = series
        self._series_metric_by_id[series[0]] = point.metric
        return series

    # ---------------- 小时报告 ----------------

    def _is_expired(self, config: MetricConfig, hour_ms: int,
                    now_ms: int) -> bool:
        return (config.retention_ms is not None
                and hour_ms + HOUR_MS <= now_ms - config.retention_ms)

    def hourly_report(self, metric, hour_start_ms, now_ms) -> HourlyReport:
        with self._lock:
            config = self._metrics.get(metric)
            if config is None:
                raise KeyError(f"metric not registered: {metric}")
            hour = hour_of(hour_start_ms)
            if self._is_expired(config, hour, now_ms):
                return HourlyReport(metric, hour, HourStatus.EXPIRED)
            stats, fills = self._report_rows(metric, hour, hour + HOUR_MS)
            return self._build_report(config, hour, stats.get(hour),
                                      fills.get(hour, 0))

    def report_range(self, metric, start_hour_ms, end_hour_ms,
                     now_ms) -> list:
        """[start_hour_ms, end_hour_ms) 内每个整点小时一份报告。"""
        with self._lock:
            config = self._metrics.get(metric)
            if config is None:
                raise KeyError(f"metric not registered: {metric}")
            start = hour_of(start_hour_ms)
            end = hour_of(end_hour_ms)
            stats, fills = self._report_rows(metric, start, end)
            reports = []
            hour = start
            while hour < end:
                if self._is_expired(config, hour, now_ms):
                    reports.append(HourlyReport(metric, hour,
                                                HourStatus.EXPIRED))
                else:
                    reports.append(self._build_report(
                        config, hour, stats.get(hour), fills.get(hour, 0)))
                hour += HOUR_MS
            return reports

    def _report_rows(self, metric, start_ms, end_ms):
        conn = self._storage.conn
        stats = {}
        for row in conn.execute(
                "SELECT hour_ms, stored, duplicates, quarantined"
                " FROM hourly_stats WHERE metric=? AND hour_ms>=?"
                " AND hour_ms<?", (metric, start_ms, end_ms)):
            stats[row[0]] = (row[1], row[2], row[3])
        fills = {}
        for row in conn.execute(
                "SELECT hour_ms, SUM(filled) FROM series_hour"
                " WHERE metric=? AND hour_ms>=? AND hour_ms<?"
                " GROUP BY hour_ms", (metric, start_ms, end_ms)):
            fills[row[0]] = row[1]
        return stats, fills

    def _build_report(self, config, hour, stat, filled) -> HourlyReport:
        stored, dup, quar = stat if stat is not None else (0, 0, 0)
        interval = config.interval_ms
        hour_first_slot = hour // interval
        hour_last_slot = (hour + HOUR_MS) // interval - 1
        expected = 0
        for (metric, _), entry in self._series.items():
            if metric != config.name or entry[1] is None:
                continue
            first_slot = slot_of(entry[1], interval)
            last_slot = slot_of(entry[2], interval)
            lo = max(first_slot, hour_first_slot)
            hi = min(last_slot, hour_last_slot)
            if hi >= lo:
                expected += hi - lo + 1
        dup_rate = (dup / (stored + dup)) if (stored + dup) > 0 else None
        missing_rate = (1.0 - filled / expected) if expected > 0 else None
        return HourlyReport(
            metric=config.name, hour_start_ms=hour, status=HourStatus.OK,
            stored=stored, duplicates=dup, duplicate_rate=dup_rate,
            expected_slots=expected, filled_slots=filled,
            missing_rate=missing_rate, quarantined=quar)

    # ---------------- 清理 ----------------

    def cleanup(self, now_ms: int) -> None:
        """按各指标保留期删除过期整小时：点、隔离、统计一起删。

        序列的最早/最晚时间保持不变。与写入共用同一把锁，因此清理
        结束后不可能残留按 now_ms 已过期的小时数据。
        """
        with self._lock:
            conn = self._storage.conn
            try:
                for config in self._metrics.values():
                    if config.retention_ms is None:
                        continue
                    max_hour = now_ms - config.retention_ms - HOUR_MS
                    # hour_end <= now - retention <=> hour_ms <= max_hour
                    conn.execute(
                        "DELETE FROM points WHERE hour_ms<=? AND series_id"
                        " IN (SELECT id FROM series WHERE metric=?)",
                        (max_hour, config.name))
                    conn.execute(
                        "DELETE FROM quarantine WHERE metric=? AND"
                        " hour_ms<=?", (config.name, max_hour))
                    conn.execute(
                        "DELETE FROM hourly_stats WHERE metric=? AND"
                        " hour_ms<=?", (config.name, max_hour))
                    conn.execute(
                        "DELETE FROM series_hour WHERE metric=? AND"
                        " hour_ms<=?", (config.name, max_hour))
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    # ---------------- 读回 ----------------

    def read_points(self, metric, start_ms, end_ms) -> list:
        """[start_ms, end_ms) 内已存点，按时间升序。"""
        with self._lock:
            rows = self._storage.conn.execute(
                "SELECT s.tags_json, p.ts_ms, p.value FROM points p"
                " JOIN series s ON s.id=p.series_id"
                " WHERE s.metric=? AND p.ts_ms>=? AND p.ts_ms<?"
                " ORDER BY p.ts_ms", (metric, start_ms, end_ms)).fetchall()
            return [DataPoint(metric, ts, value, json.loads(tags))
                    for tags, ts, value in rows]

    def read_quarantine(self, metric, start_ms, end_ms) -> list:
        """[start_ms, end_ms) 内隔离记录，按时间升序。"""
        with self._lock:
            rows = self._storage.conn.execute(
                "SELECT ts_ms, value, tags_json, reason FROM quarantine"
                " WHERE metric=? AND ts_ms>=? AND ts_ms<?"
                " ORDER BY ts_ms", (metric, start_ms, end_ms)).fetchall()
            return [
                (DataPoint(metric, ts, value, json.loads(tags)),
                 QuarantineReason(reason))
                for ts, value, tags, reason in rows
            ]

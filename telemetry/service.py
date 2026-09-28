"""TelemetryService: public facade used by the gateway.

Thread-safety: every public method takes the store lock, so concurrent
gateways see each batch atomically and the result is equivalent to some
sequential order of batches. Each batch is one SQLite transaction, so a
crash leaves it fully applied or not applied at all.
"""

from __future__ import annotations

import json
import math
from typing import Optional, Sequence

from .models import (
    HOUR_MS,
    MAX_BATCH_SIZE,
    MAX_SERIES_PER_METRIC,
    BatchResult,
    DataPoint,
    HourlyReport,
    MetricSpec,
    QuarantineReason,
    QuarantineRecord,
    RejectedRecord,
    RejectReason,
    Retention,
)
from .store import Store
from .validation import hour_of, is_valid_interval, is_valid_metric_name, validate_point


def _series_key(tags: dict[str, str]) -> str:
    return json.dumps(sorted(tags.items()), separators=(",", ":"), ensure_ascii=True)


def _slot_of(ts_ms: int, interval_ms: int) -> int:
    return ts_ms - (ts_ms % interval_ms)


def _expiry_boundary(now_ms: int, retention: Retention) -> Optional[int]:
    duration = retention.duration_ms
    if duration is None:
        return None
    return ((now_ms - duration) // HOUR_MS) * HOUR_MS


class TelemetryService:
    def __init__(self, db_path: str) -> None:
        self._store = Store(db_path)
        # High-water mark of every caller-supplied "now". Time never goes
        # backward, so a write that lands after a cleanup must be validated
        # against the newest clock the service has seen.
        self._max_now_ms = 0

    def __enter__(self) -> "TelemetryService":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._store.close()

    # -- registration ----------------------------------------------------

    def register_metric(
        self,
        name: str,
        allowed_tag_keys: Sequence[str],
        min_value: float,
        max_value: float,
        interval_ms: int,
        retention: Retention,
    ) -> MetricSpec:
        if not is_valid_metric_name(name):
            raise ValueError(f"invalid metric name: {name!r}")
        if not is_valid_interval(interval_ms):
            raise ValueError(f"invalid sampling interval: {interval_ms!r}")
        if not isinstance(retention, Retention):
            raise ValueError(f"invalid retention: {retention!r}")
        if not (math.isfinite(min_value) and math.isfinite(max_value)):
            raise ValueError("value bounds must be finite")
        if min_value > max_value:
            raise ValueError("min_value must not exceed max_value")
        keys = frozenset(allowed_tag_keys)
        spec = MetricSpec(name, keys, float(min_value), float(max_value), interval_ms, retention)
        with self._store.lock:
            if self._store.get_metric(name) is not None:
                raise ValueError(f"metric already registered: {name!r}")
            self._store.begin()
            try:
                self._store.insert_metric(spec)
                self._store.commit()
            except Exception:
                self._store.rollback()
                raise
        return spec

    def get_metric(self, name: str) -> Optional[MetricSpec]:
        with self._store.lock:
            return self._store.get_metric(name)

    # -- ingestion -------------------------------------------------------

    def write_batch(self, points: Sequence[DataPoint], now_ms: int) -> BatchResult:
        points = list(points)
        if not 1 <= len(points) <= MAX_BATCH_SIZE:
            return BatchResult(0, 0, 0, (), batch_rejected=True)

        specs: dict[str, Optional[MetricSpec]] = {}
        rejected: list[RejectedRecord] = []
        valid: list[tuple[int, DataPoint, MetricSpec]] = []
        with self._store.lock:
            self._max_now_ms = max(self._max_now_ms, now_ms)
            now_ms = self._max_now_ms
            for index, point in enumerate(points):
                if point.metric not in specs:
                    specs[point.metric] = self._store.get_metric(point.metric)
                spec = specs[point.metric]
                reason = validate_point(point, spec, now_ms)
                if reason is not None:
                    rejected.append(RejectedRecord(index, point, reason))
                else:
                    valid.append((index, point, spec))  # type: ignore[arg-type]

            if len(rejected) * 10 > len(points):
                return BatchResult(0, 0, 0, tuple(rejected), batch_rejected=True)

            accepted = 0
            duplicates = 0
            quarantined = 0
            stats: dict[tuple[str, int], list[int]] = {}
            bounds_cache: dict[tuple[str, str], list[int]] = {}
            series_counts: dict[str, int] = {}

            def bump(metric: str, hour: int, stored: int = 0,
                     dup: int = 0, quar: int = 0, occ: int = 0) -> None:
                entry = stats.setdefault((metric, hour), [0, 0, 0, 0])
                entry[0] += stored
                entry[1] += dup
                entry[2] += quar
                entry[3] += occ

            self._store.begin()
            try:
                for index, point, spec in valid:
                    metric = point.metric
                    hour = hour_of(point.ts_ms)
                    quar_reason: Optional[QuarantineReason] = None
                    if any(k not in spec.allowed_tag_keys for k in point.tags):
                        quar_reason = QuarantineReason.UNREGISTERED_TAG_KEY
                    elif not spec.min_value <= point.value <= spec.max_value:
                        quar_reason = QuarantineReason.VALUE_OUT_OF_RANGE
                    if quar_reason is not None:
                        self._store.insert_quarantine(
                            metric,
                            point.ts_ms,
                            float(point.value),
                            json.dumps(point.tags, ensure_ascii=True),
                            quar_reason.value,
                        )
                        bump(metric, hour, quar=1)
                        quarantined += 1
                        continue

                    skey = _series_key(point.tags)
                    cache_key = (metric, skey)
                    bounds = bounds_cache.get(cache_key)
                    if bounds is None:
                        row = self._store.get_series_bounds(metric, skey)
                        if row is None:
                            count = series_counts.get(metric)
                            if count is None:
                                count = self._store.count_series(metric)
                            if count >= MAX_SERIES_PER_METRIC:
                                rejected.append(
                                    RejectedRecord(
                                        index, point, RejectReason.TOO_MANY_SERIES
                                    )
                                )
                                continue
                            self._store.insert_series(metric, skey, point.ts_ms)
                            series_counts[metric] = count + 1
                            row = (point.ts_ms, point.ts_ms)
                        bounds = [row[0], row[1]]
                        bounds_cache[cache_key] = bounds

                    if not self._store.try_insert_point(
                        metric, skey, point.ts_ms, float(point.value),
                        json.dumps(point.tags, ensure_ascii=True),
                    ):
                        bump(metric, hour, dup=1)
                        duplicates += 1
                        continue

                    slot = _slot_of(point.ts_ms, spec.interval_ms)
                    occ = 1 if self._store.try_insert_slot(metric, skey, slot) else 0
                    bump(metric, hour, stored=1, occ=occ)
                    accepted += 1
                    if point.ts_ms < bounds[0]:
                        bounds[0] = point.ts_ms
                    if point.ts_ms > bounds[1]:
                        bounds[1] = point.ts_ms

                for (metric, skey), bounds in bounds_cache.items():
                    self._store.update_series_bounds(metric, skey, bounds[0], bounds[1])
                for (metric, hour), (st, dup, quar, occ) in stats.items():
                    self._store.bump_hourly_stats(
                        metric, hour, stored=st, duplicates=dup, quarantined=quar,
                        occupied_slots=occ,
                    )
                self._store.commit()
            except Exception:
                self._store.rollback()
                raise

        return BatchResult(
            accepted, duplicates, quarantined, tuple(rejected), batch_rejected=False
        )

    # -- reporting -------------------------------------------------------

    def hourly_report(self, metric: str, hour_start_ms: int, now_ms: int) -> HourlyReport:
        reports = self.hourly_reports(metric, hour_start_ms, hour_start_ms + HOUR_MS, now_ms)
        return reports[0]

    def hourly_reports(
        self, metric: str, start_hour_ms: int, end_hour_ms: int, now_ms: int
    ) -> list[HourlyReport]:
        if start_hour_ms % HOUR_MS or end_hour_ms % HOUR_MS:
            raise ValueError("hour bounds must be aligned to whole hours")
        if end_hour_ms <= start_hour_ms:
            return []
        with self._store.lock:
            spec = self._store.get_metric(metric)
            if spec is None:
                raise ValueError(f"metric not registered: {metric!r}")
            hours = list(range(start_hour_ms, end_hour_ms, HOUR_MS))
            stats = self._store.get_hourly_stats(metric, hours)
            bounds = self._store.series_bounds_for_metric(metric)
            boundary = _expiry_boundary(now_ms, spec.retention)

        interval = spec.interval_ms
        slots_per_hour = HOUR_MS // interval
        index_of = {h: i for i, h in enumerate(hours)}
        expected = [0] * len(hours)
        diff = [0] * (len(hours) + 1)

        def add_full(lo_hour: int, hi_hour: int) -> None:
            lo = max(lo_hour, start_hour_ms)
            hi = min(hi_hour, end_hour_ms)
            if lo < hi:
                diff[(lo - start_hour_ms) // HOUR_MS] += slots_per_hour
                diff[(hi - start_hour_ms) // HOUR_MS] -= slots_per_hour

        def add_partial(hour: int, count: int) -> None:
            idx = index_of.get(hour)
            if idx is not None and count > 0:
                expected[idx] += count

        for min_ts, max_ts in bounds:
            s_min = _slot_of(min_ts, interval)
            s_max = _slot_of(max_ts, interval)
            h_min = hour_of(s_min)
            h_max = hour_of(s_max)
            if h_max < start_hour_ms or h_min >= end_hour_ms:
                continue
            if h_min == h_max:
                add_partial(h_min, (s_max - s_min) // interval + 1)
                continue
            add_full(h_min + HOUR_MS, h_max)
            first_end = min(s_max, h_min + HOUR_MS - interval)
            add_partial(h_min, (first_end - s_min) // interval + 1)
            last_start = max(s_min, h_max)
            add_partial(h_max, (s_max - last_start) // interval + 1)

        running = 0
        reports: list[HourlyReport] = []
        for i, hour in enumerate(hours):
            running += diff[i]
            exp = running + expected[i]
            stored, dup, quar, occ = stats.get(hour, (0, 0, 0, 0))
            expired = boundary is not None and hour < boundary
            if expired:
                stored = dup = quar = occ = 0
                exp = 0
            dup_rate = dup / (stored + dup) if stored + dup > 0 else None
            missing_rate = 1.0 - occ / exp if exp > 0 else None
            reports.append(
                HourlyReport(
                    metric=metric,
                    hour_start_ms=hour,
                    stored=stored,
                    duplicates=dup,
                    duplicate_rate=dup_rate,
                    expected_slots=exp,
                    occupied_slots=occ,
                    missing_rate=missing_rate,
                    quarantined=quar,
                    expired=expired,
                )
            )
        return reports

    # -- cleanup ---------------------------------------------------------

    def cleanup(self, now_ms: int) -> None:
        with self._store.lock:
            self._max_now_ms = max(self._max_now_ms, now_ms)
            self._store.begin()
            try:
                for spec in self._store.list_metrics():
                    boundary = _expiry_boundary(now_ms, spec.retention)
                    if boundary is not None and boundary > 0:
                        self._store.delete_expired(spec.name, boundary)
                self._store.commit()
            except Exception:
                self._store.rollback()
                raise

    # -- read-back -------------------------------------------------------

    def read_points(self, metric: str, start_ms: int, end_ms: int) -> list[DataPoint]:
        with self._store.lock:
            rows = self._store.read_points(metric, start_ms, end_ms)
        return [
            DataPoint(metric, ts, value, json.loads(tags_json))
            for ts, value, tags_json in rows
        ]

    def read_quarantine(
        self, metric: str, start_ms: int, end_ms: int
    ) -> list[QuarantineRecord]:
        with self._store.lock:
            rows = self._store.read_quarantine(metric, start_ms, end_ms)
        return [
            QuarantineRecord(
                metric=metric,
                ts_ms=ts,
                value=value,
                tags=json.loads(tags_json),
                reason=QuarantineReason(reason),
                received_hour_ms=hour_of(ts),
            )
            for ts, value, tags_json, reason in rows
        ]

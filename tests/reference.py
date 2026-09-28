"""朴素参照模型：内存中按规格逐条重算，用于对照 TelemetryService。

只追求明显正确，不追求性能。接口与 TelemetryService 对齐：
记录被接受的批次后，可计算任意小时报告、读回点与隔离记录。
"""
from __future__ import annotations

import math
import re
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from telemetry.models import (  # noqa: E402
    HOUR_MS,
    MAX_FUTURE_SKEW_MS,
    MAX_TAGS,
    MAX_TAG_KEY_LEN,
    MAX_TAG_VALUE_LEN,
    DataPoint,
    HourlyReport,
    HourStatus,
    QuarantineReason,
    RejectReason,
)

_NAME_RE = re.compile(r"[A-Za-z0-9_]{1,128}\Z", re.ASCII)


def hour_of(ts_ms: int) -> int:
    return (ts_ms // HOUR_MS) * HOUR_MS


class RefModel:
    def __init__(self) -> None:
        # metric -> dict(tag_keys=set, min, max, interval_ms, retention_ms)
        self.metrics = {}
        # metric -> {tags_key: {"tags": dict, "points": {ts: value},
        #                      "first": int|None, "last": int|None}}
        self.series = {}
        # (metric, hour) -> [stored, dup, quar]
        self.stats = {}
        # metric -> [(DataPoint, QuarantineReason)]
        self.quarantine = {}

    # ---- 注册 ----

    def register_metric(self, name, tag_keys, min_value, max_value,
                        interval_ms, retention_days):
        retention_ms = (None if retention_days is None
                        else retention_days * 86_400_000)
        self.metrics[name] = {
            "tag_keys": set(tag_keys),
            "min": float(min_value),
            "max": float(max_value),
            "interval_ms": interval_ms,
            "retention_ms": retention_ms,
        }
        self.series.setdefault(name, {})
        self.quarantine.setdefault(name, [])

    # ---- 写入 ----

    def _format_error(self, p, now_ms):
        if not isinstance(p.metric, str) or not _NAME_RE.match(p.metric):
            return RejectReason.BAD_METRIC_NAME
        cfg = self.metrics.get(p.metric)
        if cfg is None:
            return RejectReason.METRIC_NOT_REGISTERED
        if not isinstance(p.tags, dict) or len(p.tags) > MAX_TAGS:
            return RejectReason.TOO_MANY_TAGS
        for k, v in p.tags.items():
            if not isinstance(k, str) or not k or len(k) > MAX_TAG_KEY_LEN:
                return RejectReason.BAD_TAG_KEY
            if not isinstance(v, str) or len(v) > MAX_TAG_VALUE_LEN:
                return RejectReason.BAD_TAG_VALUE
        if not isinstance(p.value, (int, float)) or isinstance(p.value, bool) \
                or not math.isfinite(p.value):
            return RejectReason.NON_FINITE_VALUE
        if not isinstance(p.ts_ms, int) or isinstance(p.ts_ms, bool):
            return RejectReason.BAD_TIMESTAMP
        if p.ts_ms > now_ms + MAX_FUTURE_SKEW_MS:
            return RejectReason.TIMESTAMP_TOO_FAR_FUTURE
        if cfg["retention_ms"] is not None:
            if hour_of(p.ts_ms) + HOUR_MS <= now_ms - cfg["retention_ms"]:
                return RejectReason.TIMESTAMP_EXPIRED
        return None

    def apply_batch(self, points, now_ms):
        """返回 (batch_rejected, rejected_reasons: dict index->reason)。"""
        points = list(points)
        if not 1 <= len(points) <= 5000:
            return True, {}
        rejected = {}
        for i, p in enumerate(points):
            err = self._format_error(p, now_ms)
            if err is not None:
                rejected[i] = err
        if len(rejected) * 10 > len(points):
            return True, rejected
        for i, p in enumerate(points):
            if i in rejected:
                continue
            cfg = self.metrics[p.metric]
            hour = hour_of(p.ts_ms)
            bucket = self.stats.setdefault((p.metric, hour), [0, 0, 0])
            quar = None
            if any(k not in cfg["tag_keys"] for k in p.tags):
                quar = QuarantineReason.UNREGISTERED_TAG_KEY
            elif not (cfg["min"] <= p.value <= cfg["max"]):
                quar = QuarantineReason.VALUE_OUT_OF_RANGE
            if quar is not None:
                bucket[2] += 1
                self.quarantine[p.metric].append((p, quar))
                continue
            key = tuple(sorted(p.tags.items()))
            series = self.series[p.metric].setdefault(
                key, {"tags": dict(p.tags), "points": {},
                      "first": None, "last": None})
            if p.ts_ms in series["points"]:
                bucket[1] += 1
                continue
            series["points"][p.ts_ms] = p.value
            bucket[0] += 1
            if series["first"] is None or p.ts_ms < series["first"]:
                series["first"] = p.ts_ms
            if series["last"] is None or p.ts_ms > series["last"]:
                series["last"] = p.ts_ms
        return False, rejected

    # ---- 清理 ----

    def cleanup(self, now_ms):
        for metric, cfg in self.metrics.items():
            retention = cfg["retention_ms"]
            if retention is None:
                continue
            cutoff = now_ms - retention  # hour_end <= cutoff 的小时删除
            for series in self.series[metric].values():
                series["points"] = {
                    ts: v for ts, v in series["points"].items()
                    if hour_of(ts) + HOUR_MS > cutoff}
            self.quarantine[metric] = [
                (p, r) for p, r in self.quarantine[metric]
                if hour_of(p.ts_ms) + HOUR_MS > cutoff]
            for key in [k for k in self.stats
                        if k[0] == metric
                        and k[1] + HOUR_MS <= cutoff]:
                del self.stats[key]

    # ---- 报告 ----

    def hourly_report(self, metric, hour_start_ms, now_ms) -> HourlyReport:
        cfg = self.metrics[metric]
        hour = hour_of(hour_start_ms)
        if (cfg["retention_ms"] is not None
                and hour + HOUR_MS <= now_ms - cfg["retention_ms"]):
            return HourlyReport(metric, hour, HourStatus.EXPIRED)
        interval = cfg["interval_ms"]
        hour_first_slot = hour // interval
        hour_last_slot = (hour + HOUR_MS) // interval - 1
        expected = 0
        filled = 0
        for series in self.series[metric].values():
            if series["first"] is None:
                continue
            first_slot = series["first"] // interval
            last_slot = series["last"] // interval
            lo = max(first_slot, hour_first_slot)
            hi = min(last_slot, hour_last_slot)
            if hi >= lo:
                expected += hi - lo + 1
                slots = {ts // interval for ts in series["points"]
                         if lo <= ts // interval <= hi}
                filled += len(slots)
        stored, dup, quar = self.stats.get((metric, hour), (0, 0, 0))
        dup_rate = dup / (stored + dup) if stored + dup else None
        missing = 1.0 - filled / expected if expected else None
        return HourlyReport(metric, hour, HourStatus.OK, stored, dup,
                            dup_rate, expected, filled, missing, quar)

    # ---- 读回 ----

    def read_points(self, metric, start_ms, end_ms):
        out = []
        for series in self.series[metric].values():
            for ts, value in series["points"].items():
                if start_ms <= ts < end_ms:
                    out.append(DataPoint(metric, ts, value,
                                         dict(series["tags"])))
        out.sort(key=lambda p: p.ts_ms)
        return out

    def read_quarantine(self, metric, start_ms, end_ms):
        return sorted(
            [(p, r) for p, r in self.quarantine[metric]
             if start_ms <= p.ts_ms < end_ms],
            key=lambda t: t[0].ts_ms)

"""Independent reference model of the spec, used to cross-check the service.

Deliberately naive: recomputes everything from the recorded accepted
history so bugs in the service cannot hide behind shared cleverness.
"""

from __future__ import annotations

import math
from collections import Counter

from telemetry.models import HOUR_MS, DataPoint, MetricSpec

MAX_FUTURE_SKEW_MS = 10 * 60 * 1000


def hour_of(ts_ms: int) -> int:
    return ts_ms - (ts_ms % HOUR_MS)


def _valid_name(name: str) -> bool:
    if not isinstance(name, str) or not 1 <= len(name) <= 128:
        return False
    return all(c.isascii() and (c.isalnum() or c == "_") for c in name)


class RefModel:
    def __init__(self) -> None:
        self.specs: dict[str, MetricSpec] = {}
        self.points: dict[str, set[tuple[tuple, int]]] = {}
        self.bounds: dict[str, dict[tuple, list[int]]] = {}
        self.stored: dict[str, Counter] = {}
        self.dups: dict[str, Counter] = {}
        self.quar: dict[str, Counter] = {}
        self.quar_records: dict[str, list[DataPoint]] = {}

    def register(self, spec: MetricSpec) -> None:
        self.specs[spec.name] = spec
        m = spec.name
        self.points[m] = set()
        self.bounds[m] = {}
        self.stored[m] = Counter()
        self.dups[m] = Counter()
        self.quar[m] = Counter()
        self.quar_records[m] = []

    def _format_invalid(self, p: DataPoint, now_ms: int) -> bool:
        spec = self.specs.get(p.metric)
        if not _valid_name(p.metric) or spec is None:
            return True
        if len(p.tags) > 8:
            return True
        for k, v in p.tags.items():
            if not isinstance(k, str) or not 1 <= len(k) <= 64:
                return True
            if not isinstance(v, str) or len(v) > 256:
                return True
        if not isinstance(p.value, (int, float)) or isinstance(p.value, bool):
            return True
        if not math.isfinite(p.value):
            return True
        if p.ts_ms > now_ms + MAX_FUTURE_SKEW_MS:
            return True
        dur = spec.retention.duration_ms
        if dur is not None and hour_of(p.ts_ms) + HOUR_MS <= now_ms - dur:
            return True
        return False

    def apply_batch(self, points: list[DataPoint], now_ms: int) -> dict:
        """Apply one batch per the spec; return expected outcome counts."""
        result = {
            "batch_rejected": False, "accepted": 0, "duplicates": 0,
            "quarantined": 0, "rejected": 0,
        }
        if not 1 <= len(points) <= 5000:
            result["batch_rejected"] = True
            return result
        invalid_idx = [i for i, p in enumerate(points) if self._format_invalid(p, now_ms)]
        if len(invalid_idx) * 10 > len(points):
            result["batch_rejected"] = True
            result["rejected"] = len(invalid_idx)
            return result
        result["rejected"] = len(invalid_idx)
        invalid_set = set(invalid_idx)
        for i, p in enumerate(points):
            if i in invalid_set:
                continue
            spec = self.specs[p.metric]
            m = p.metric
            hour = hour_of(p.ts_ms)
            if any(k not in spec.allowed_tag_keys for k in p.tags) or not (
                spec.min_value <= p.value <= spec.max_value
            ):
                self.quar[m][hour] += 1
                self.quar_records[m].append(p)
                result["quarantined"] += 1
                continue
            skey = tuple(sorted(p.tags.items()))
            ident = (skey, p.ts_ms)
            if ident in self.points[m]:
                self.dups[m][hour] += 1
                result["duplicates"] += 1
                continue
            self.points[m].add(ident)
            self.stored[m][hour] += 1
            b = self.bounds[m].setdefault(skey, [p.ts_ms, p.ts_ms])
            b[0] = min(b[0], p.ts_ms)
            b[1] = max(b[1], p.ts_ms)
            result["accepted"] += 1
        return result

    def expected_slots(self, metric: str, hour: int) -> int:
        spec = self.specs[metric]
        iv = spec.interval_ms
        total = 0
        for lo, hi in self.bounds[metric].values():
            s = lo - (lo % iv)
            end = hi - (hi % iv)
            while s <= end:
                if hour_of(s) == hour:
                    total += 1
                s += iv
        return total

    def occupied_slots(self, metric: str, hour: int) -> int:
        spec = self.specs[metric]
        iv = spec.interval_ms
        slots = set()
        for skey, ts in self.points[metric]:
            slot = ts - (ts % iv)
            if hour_of(slot) == hour:
                slots.add((skey, slot))
        return len(slots)

    def expected_report(self, metric: str, hour: int, now_ms: int) -> dict:
        spec = self.specs[metric]
        dur = spec.retention.duration_ms
        if dur is not None and hour < ((now_ms - dur) // HOUR_MS) * HOUR_MS:
            return {"expired": True, "stored": 0, "duplicates": 0,
                    "duplicate_rate": None, "expected_slots": 0,
                    "occupied_slots": 0, "missing_rate": None, "quarantined": 0}
        stored = self.stored[metric][hour]
        dup = self.dups[metric][hour]
        quar = self.quar[metric][hour]
        exp = self.expected_slots(metric, hour)
        occ = self.occupied_slots(metric, hour)
        return {
            "expired": False,
            "stored": stored,
            "duplicates": dup,
            "duplicate_rate": dup / (stored + dup) if stored + dup else None,
            "expected_slots": exp,
            "occupied_slots": occ,
            "missing_rate": 1.0 - occ / exp if exp else None,
            "quarantined": quar,
        }


def assert_reports_match(service, model: RefModel, metric: str,
                         start_hour: int, end_hour: int, now_ms: int) -> None:
    reports = service.hourly_reports(metric, start_hour, end_hour, now_ms)
    for rep in reports:
        exp = model.expected_report(metric, rep.hour_start_ms, now_ms)
        got = {
            "expired": rep.expired, "stored": rep.stored, "duplicates": rep.duplicates,
            "duplicate_rate": rep.duplicate_rate, "expected_slots": rep.expected_slots,
            "occupied_slots": rep.occupied_slots, "missing_rate": rep.missing_rate,
            "quarantined": rep.quarantined,
        }
        assert got == exp, f"hour {rep.hour_start_ms}: got {got} want {exp}"

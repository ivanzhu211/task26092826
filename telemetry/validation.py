"""Format validation for metric names, intervals, and incoming points."""

from __future__ import annotations

import math
from typing import Optional

from .models import (
    HOUR_MS,
    MAX_FUTURE_SKEW_MS,
    MAX_TAG_KEY_LEN,
    MAX_TAGS_PER_POINT,
    MAX_TAG_VALUE_LEN,
    DataPoint,
    MetricSpec,
    RejectReason,
)

_NAME_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
)


def is_valid_metric_name(name: str) -> bool:
    if not isinstance(name, str) or not 1 <= len(name) <= 128:
        return False
    return all(ch in _NAME_CHARS for ch in name)


def is_valid_interval(interval_ms: int) -> bool:
    return (
        isinstance(interval_ms, int)
        and 1000 <= interval_ms <= HOUR_MS
        and HOUR_MS % interval_ms == 0
    )


def hour_of(ts_ms: int) -> int:
    return ts_ms - (ts_ms % HOUR_MS)


def validate_point(
    point: DataPoint, spec: Optional[MetricSpec], now_ms: int
) -> Optional[RejectReason]:
    """Return the rejection reason for a point, or None if format-valid."""
    if not is_valid_metric_name(point.metric):
        return RejectReason.BAD_METRIC_NAME
    if spec is None:
        return RejectReason.METRIC_NOT_REGISTERED
    tags = point.tags
    if len(tags) > MAX_TAGS_PER_POINT:
        return RejectReason.TOO_MANY_TAGS
    for key, value in tags.items():
        if not isinstance(key, str) or not 1 <= len(key) <= MAX_TAG_KEY_LEN:
            return RejectReason.BAD_TAG_KEY
        if not isinstance(value, str) or len(value) > MAX_TAG_VALUE_LEN:
            return RejectReason.BAD_TAG_VALUE
    if not isinstance(point.value, (int, float)) or isinstance(point.value, bool):
        return RejectReason.NON_FINITE_VALUE
    if not math.isfinite(point.value):
        return RejectReason.NON_FINITE_VALUE
    if point.ts_ms > now_ms + MAX_FUTURE_SKEW_MS:
        return RejectReason.TIMESTAMP_TOO_FAR_IN_FUTURE
    retention_ms = spec.retention.duration_ms
    if retention_ms is not None:
        hour_end = hour_of(point.ts_ms) + HOUR_MS
        if hour_end <= now_ms - retention_ms:
            return RejectReason.TIMESTAMP_EXPIRED
    return None

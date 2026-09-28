"""格式校验：纯函数，不碰数据库。"""
from __future__ import annotations

import math
import re
from typing import Optional

from .models import (
    HOUR_MS,
    MAX_FUTURE_SKEW_MS,
    MAX_TAG_KEY_LEN,
    MAX_TAG_VALUE_LEN,
    MAX_TAGS,
    DataPoint,
    MetricConfig,
    RejectReason,
)

_METRIC_NAME_RE = re.compile(r"[A-Za-z0-9_]{1,128}\Z", re.ASCII)


def valid_metric_name(name) -> bool:
    return isinstance(name, str) and _METRIC_NAME_RE.match(name) is not None


def hour_of(ts_ms: int) -> int:
    """UTC 整点切分；恰在整点归入新的一小时（floor 语义天然满足）。"""
    return (ts_ms // HOUR_MS) * HOUR_MS


def slot_of(ts_ms: int, interval_ms: int) -> int:
    """槽按采样间隔从 Unix 纪元起对齐。"""
    return ts_ms // interval_ms


def check_format(point: DataPoint, config: Optional[MetricConfig],
                 now_ms: int) -> Optional[RejectReason]:
    """返回 None 表示格式合格，否则返回拒绝原因。"""
    if not valid_metric_name(point.metric):
        return RejectReason.BAD_METRIC_NAME
    if config is None:
        return RejectReason.METRIC_NOT_REGISTERED
    tags = point.tags
    if not isinstance(tags, dict):
        return RejectReason.TOO_MANY_TAGS
    if len(tags) > MAX_TAGS:
        return RejectReason.TOO_MANY_TAGS
    for key, value in tags.items():
        if not isinstance(key, str) or not key or len(key) > MAX_TAG_KEY_LEN:
            return RejectReason.BAD_TAG_KEY
        if not isinstance(value, str) or len(value) > MAX_TAG_VALUE_LEN:
            return RejectReason.BAD_TAG_VALUE
    if not isinstance(point.value, (int, float)) or isinstance(point.value, bool) \
            or not math.isfinite(point.value):
        return RejectReason.NON_FINITE_VALUE
    if not isinstance(point.ts_ms, int) or isinstance(point.ts_ms, bool):
        return RejectReason.BAD_TIMESTAMP
    if point.ts_ms > now_ms + MAX_FUTURE_SKEW_MS:
        return RejectReason.TIMESTAMP_TOO_FAR_FUTURE
    if config.retention_ms is not None:
        hour_end = hour_of(point.ts_ms) + HOUR_MS
        if hour_end <= now_ms - config.retention_ms:
            return RejectReason.TIMESTAMP_EXPIRED
    return None

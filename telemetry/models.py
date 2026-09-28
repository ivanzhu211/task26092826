"""类型定义：数据点、批次结果、小时报告、指标配置。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


HOUR_MS = 3_600_000
MAX_BATCH_SIZE = 5000
MAX_TAGS = 8
MAX_TAG_KEY_LEN = 64
MAX_TAG_VALUE_LEN = 256
MAX_FUTURE_SKEW_MS = 10 * 60 * 1000
MAX_SERIES_PER_METRIC = 2000

RETENTION_DAYS_30 = 30
RETENTION_DAYS_90 = 90


class RejectReason(str, Enum):
    BAD_METRIC_NAME = "bad_metric_name"
    METRIC_NOT_REGISTERED = "metric_not_registered"
    BAD_TIMESTAMP = "bad_timestamp"
    TOO_MANY_TAGS = "too_many_tags"
    BAD_TAG_KEY = "bad_tag_key"
    BAD_TAG_VALUE = "bad_tag_value"
    NON_FINITE_VALUE = "non_finite_value"
    TIMESTAMP_TOO_FAR_FUTURE = "timestamp_too_far_future"
    TIMESTAMP_EXPIRED = "timestamp_expired"


class QuarantineReason(str, Enum):
    UNREGISTERED_TAG_KEY = "unregistered_tag_key"
    VALUE_OUT_OF_RANGE = "value_out_of_range"


class HourStatus(str, Enum):
    OK = "ok"
    EXPIRED = "expired"


@dataclass(frozen=True)
class DataPoint:
    metric: str
    ts_ms: int
    value: float
    tags: dict


@dataclass(frozen=True)
class RejectedRecord:
    index: int
    point: DataPoint
    reason: RejectReason


@dataclass(frozen=True)
class QuarantinedRecord:
    index: int
    point: DataPoint
    reason: QuarantineReason


@dataclass
class BatchResult:
    batch_rejected: bool = False
    stored: int = 0
    duplicates: int = 0
    quarantined: list = field(default_factory=list)
    rejected: list = field(default_factory=list)


@dataclass(frozen=True)
class HourlyReport:
    metric: str
    hour_start_ms: int
    status: HourStatus
    stored: Optional[int] = None
    duplicates: Optional[int] = None
    duplicate_rate: Optional[float] = None
    expected_slots: Optional[int] = None
    filled_slots: Optional[int] = None
    missing_rate: Optional[float] = None
    quarantined: Optional[int] = None


@dataclass(frozen=True)
class MetricConfig:
    name: str
    tag_keys: tuple
    min_value: float
    max_value: float
    interval_ms: int
    retention_ms: Optional[int]  # None 表示永久

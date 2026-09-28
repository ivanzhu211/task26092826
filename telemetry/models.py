"""Data types for the telemetry ingestion library."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

HOUR_MS = 3_600_000
MAX_BATCH_SIZE = 5000
MAX_SERIES_PER_METRIC = 2000
MAX_TAGS_PER_POINT = 8
MAX_TAG_KEY_LEN = 64
MAX_TAG_VALUE_LEN = 256
MAX_FUTURE_SKEW_MS = 10 * 60 * 1000

RETENTION_30D_MS = 30 * 24 * HOUR_MS
RETENTION_90D_MS = 90 * 24 * HOUR_MS


class Retention(Enum):
    DAYS_30 = "30d"
    DAYS_90 = "90d"
    FOREVER = "forever"

    @property
    def duration_ms(self) -> Optional[int]:
        if self is Retention.DAYS_30:
            return RETENTION_30D_MS
        if self is Retention.DAYS_90:
            return RETENTION_90D_MS
        return None


class RejectReason(Enum):
    BAD_METRIC_NAME = "bad_metric_name"
    METRIC_NOT_REGISTERED = "metric_not_registered"
    TOO_MANY_TAGS = "too_many_tags"
    BAD_TAG_KEY = "bad_tag_key"
    BAD_TAG_VALUE = "bad_tag_value"
    NON_FINITE_VALUE = "non_finite_value"
    TIMESTAMP_TOO_FAR_IN_FUTURE = "timestamp_too_far_in_future"
    TIMESTAMP_EXPIRED = "timestamp_expired"
    TOO_MANY_SERIES = "too_many_series"


class QuarantineReason(Enum):
    UNREGISTERED_TAG_KEY = "unregistered_tag_key"
    VALUE_OUT_OF_RANGE = "value_out_of_range"


@dataclass(frozen=True)
class MetricSpec:
    name: str
    allowed_tag_keys: frozenset[str]
    min_value: float
    max_value: float
    interval_ms: int
    retention: Retention


@dataclass(frozen=True)
class DataPoint:
    metric: str
    ts_ms: int
    value: float
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class RejectedRecord:
    index: int
    point: DataPoint
    reason: RejectReason


@dataclass(frozen=True)
class QuarantineRecord:
    metric: str
    ts_ms: int
    value: float
    tags: dict[str, str]
    reason: QuarantineReason
    received_hour_ms: int


@dataclass(frozen=True)
class BatchResult:
    accepted: int
    duplicates: int
    quarantined: int
    rejected: tuple[RejectedRecord, ...]
    batch_rejected: bool = False


@dataclass(frozen=True)
class HourlyReport:
    metric: str
    hour_start_ms: int
    stored: int
    duplicates: int
    duplicate_rate: Optional[float]
    expected_slots: int
    occupied_slots: int
    missing_rate: Optional[float]
    quarantined: int
    expired: bool = False

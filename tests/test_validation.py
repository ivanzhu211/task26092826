import math

import pytest

from telemetry import DataPoint, RejectReason, Retention, TelemetryService
from tests.conftest import BASE_HOUR, HOUR, register
from tests.model import RefModel, assert_reports_match


def pt(metric="m", ts=BASE_HOUR, value=1.0, tags=None):
    return DataPoint(metric, ts, value, tags or {"line": "A"})


class TestFormatRules:
    def test_unregistered_metric_rejected(self, svc):
        m = RefModel()
        register(svc, m)
        r = svc.write_batch([pt("nope")], BASE_HOUR + HOUR)
        # 1 of 1 invalid = 100% > 10%, so the whole batch is rejected
        assert r.batch_rejected and r.accepted == 0
        assert r.rejected[0].reason is RejectReason.METRIC_NOT_REGISTERED
        points = [pt(ts=BASE_HOUR + i * 60_000) for i in range(9)] + [pt("nope")]
        r = svc.write_batch(points, BASE_HOUR + HOUR)
        assert not r.batch_rejected and r.accepted == 9

    @pytest.mark.parametrize("name", ["", "a" * 129, "has-dash", "has space", "café"])
    def test_bad_metric_names(self, svc, name):
        m = RefModel()
        register(svc, m)
        r = svc.write_batch([pt(name)], BASE_HOUR + HOUR)
        assert r.rejected[0].reason is RejectReason.BAD_METRIC_NAME

    def test_too_many_tags(self, svc):
        m = RefModel()
        register(svc, m, tags=tuple(f"k{i}" for i in range(9)))
        tags = {f"k{i}": "v" for i in range(9)}
        r = svc.write_batch([pt(tags=tags)], BASE_HOUR + HOUR)
        assert r.rejected[0].reason is RejectReason.TOO_MANY_TAGS

    def test_bad_tag_key_and_value(self, svc):
        m = RefModel()
        register(svc, m)
        now = BASE_HOUR + HOUR
        r = svc.write_batch([pt(tags={"": "v"})], now)
        assert r.rejected[0].reason is RejectReason.BAD_TAG_KEY
        r = svc.write_batch([pt(tags={"k" * 65: "v"})], now)
        assert r.rejected[0].reason is RejectReason.BAD_TAG_KEY
        r = svc.write_batch([pt(tags={"line": "v" * 257})], now)
        assert r.rejected[0].reason is RejectReason.BAD_TAG_VALUE

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
    def test_non_finite_values(self, svc, value):
        m = RefModel()
        register(svc, m)
        r = svc.write_batch([pt(value=value)], BASE_HOUR + HOUR)
        assert r.rejected[0].reason is RejectReason.NON_FINITE_VALUE

    def test_future_timestamp(self, svc):
        m = RefModel()
        register(svc, m)
        now = BASE_HOUR + HOUR
        ok = svc.write_batch([pt(ts=now + 600_000)], now)
        assert ok.accepted == 1
        bad = svc.write_batch([pt(ts=now + 600_001)], now)
        assert bad.rejected[0].reason is RejectReason.TIMESTAMP_TOO_FAR_IN_FUTURE

    def test_expired_timestamp(self, svc):
        m = RefModel()
        register(svc, m, retention=Retention.DAYS_30)
        now = BASE_HOUR + 40 * 24 * HOUR
        boundary = now - 30 * 24 * HOUR
        # hour ending exactly at the boundary is expired
        r = svc.write_batch([pt(ts=boundary - 1)], now)
        assert r.rejected[0].reason is RejectReason.TIMESTAMP_EXPIRED
        # first hour ending after the boundary is fine
        r = svc.write_batch([pt(ts=boundary)], now)
        assert r.accepted == 1

    def test_forever_retention_never_expires(self, svc):
        m = RefModel()
        register(svc, m, retention=Retention.FOREVER)
        now = BASE_HOUR + 1000 * 24 * HOUR
        r = svc.write_batch([pt(ts=BASE_HOUR)], now)
        assert r.accepted == 1


class TestBatchRules:
    def test_empty_and_oversize_batch_rejected(self, svc):
        m = RefModel()
        register(svc, m)
        now = BASE_HOUR + HOUR
        assert svc.write_batch([], now).batch_rejected
        assert svc.write_batch([pt() for _ in range(5001)], now).batch_rejected
        big = svc.write_batch(
            [DataPoint("m", BASE_HOUR + i * 1000, 1.0, {"line": "A"}) for i in range(60)],
            now,
        )
        assert not big.batch_rejected and big.accepted == 60

    def test_exactly_10_percent_invalid_accepted(self, svc):
        m = RefModel()
        register(svc, m)
        now = BASE_HOUR + HOUR
        points = [pt(ts=BASE_HOUR + i * 60_000) for i in range(9)]
        points.append(pt("unregistered"))
        r = svc.write_batch(points, now)
        assert not r.batch_rejected
        assert r.accepted == 9 and len(r.rejected) == 1
        m.apply_batch(points, now)
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, now)

    def test_over_10_percent_invalid_rejects_whole_batch(self, svc):
        m = RefModel()
        register(svc, m)
        now = BASE_HOUR + HOUR
        points = [pt(ts=BASE_HOUR + i * 60_000) for i in range(8)]
        points += [pt("bad1"), pt("bad2")]
        r = svc.write_batch(points, now)
        assert r.batch_rejected
        assert r.accepted == 0 and len(r.rejected) == 2
        # nothing took effect
        m.apply_batch(points, now)
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, now)
        assert svc.read_points("m", 0, now + HOUR) == []

    def test_rejected_records_carry_index_and_reason(self, svc):
        m = RefModel()
        register(svc, m)
        now = BASE_HOUR + HOUR
        points = [pt(value=float("nan")), pt(), pt("bad-name")]
        r = svc.write_batch(points, now)
        assert r.batch_rejected  # 2/3 invalid > 10%
        by_index = {rec.index: rec.reason for rec in r.rejected}
        assert by_index[0] is RejectReason.NON_FINITE_VALUE
        assert by_index[2] is RejectReason.BAD_METRIC_NAME

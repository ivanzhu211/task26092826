import pytest

from telemetry import DataPoint, Retention
from tests.conftest import BASE_HOUR, HOUR, register
from tests.model import RefModel, assert_reports_match

NOW = BASE_HOUR + 50 * HOUR


def pt(ts, value=1.0, line="A"):
    return DataPoint("m", ts, value, {"line": line})


class TestReports:
    def test_empty_hour_reports_no_data_rates(self, svc):
        m = RefModel()
        register(svc, m)
        rep = svc.hourly_report("m", BASE_HOUR, NOW)
        assert rep.stored == 0 and rep.duplicates == 0 and rep.quarantined == 0
        assert rep.expected_slots == 0 and rep.occupied_slots == 0
        assert rep.duplicate_rate is None and rep.missing_rate is None
        assert not rep.expired

    def test_unregistered_metric_report_raises(self, svc):
        with pytest.raises(ValueError):
            svc.hourly_report("ghost", BASE_HOUR, NOW)

    def test_unaligned_hour_range_rejected(self, svc):
        m = RefModel()
        register(svc, m)
        with pytest.raises(ValueError):
            svc.hourly_reports("m", BASE_HOUR + 1, BASE_HOUR + HOUR, NOW)

    def test_rates_computed(self, svc):
        m = RefModel()
        register(svc, m)
        batch = [pt(BASE_HOUR + i * 60_000) for i in range(30)]
        batch += [pt(BASE_HOUR + i * 60_000) for i in range(10)]  # 10 duplicates
        svc.write_batch(batch, NOW)
        m.apply_batch(batch, NOW)
        rep = svc.hourly_report("m", BASE_HOUR, NOW)
        assert rep.stored == 30 and rep.duplicates == 10
        assert rep.duplicate_rate == 10 / 40
        # series spans slots 0..29 -> 30 expected, all occupied
        assert rep.expected_slots == 30 and rep.occupied_slots == 30
        assert rep.missing_rate == 0.0
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, NOW)

    def test_range_report_covers_every_hour(self, svc):
        m = RefModel()
        register(svc, m)
        svc.write_batch([pt(BASE_HOUR), pt(BASE_HOUR + 10 * HOUR)], NOW)
        m.apply_batch([pt(BASE_HOUR), pt(BASE_HOUR + 10 * HOUR)], NOW)
        reports = svc.hourly_reports("m", BASE_HOUR, BASE_HOUR + 11 * HOUR, NOW)
        assert len(reports) == 11
        assert [r.hour_start_ms for r in reports] == [
            BASE_HOUR + i * HOUR for i in range(11)
        ]
        middle = reports[5]
        assert middle.expected_slots == 60 and middle.missing_rate == 1.0
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 11 * HOUR, NOW)

    def test_point_exactly_on_hour_boundary(self, svc):
        m = RefModel()
        register(svc, m)
        svc.write_batch([pt(BASE_HOUR + HOUR)], NOW)
        m.apply_batch([pt(BASE_HOUR + HOUR)], NOW)
        before = svc.hourly_report("m", BASE_HOUR, NOW)
        after = svc.hourly_report("m", BASE_HOUR + HOUR, NOW)
        assert before.stored == 0 and after.stored == 1
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 2 * HOUR, NOW)


class TestReadBack:
    def test_read_points_time_range(self, svc):
        m = RefModel()
        register(svc, m)
        batch = [pt(BASE_HOUR + i * 60_000, float(i)) for i in range(120)]
        svc.write_batch(batch, NOW)
        got = svc.read_points("m", BASE_HOUR + 30 * 60_000, BASE_HOUR + 60 * 60_000)
        assert [p.value for p in got] == [float(i) for i in range(30, 60)]
        assert all(p.metric == "m" and p.tags == {"line": "A"} for p in got)

    def test_read_quarantine_time_range(self, svc):
        m = RefModel()
        register(svc, m)
        batch = [pt(BASE_HOUR + i * 60_000, 999.0) for i in range(10)]
        svc.write_batch(batch, NOW)
        got = svc.read_quarantine("m", BASE_HOUR + 3 * 60_000, BASE_HOUR + 5 * 60_000)
        assert len(got) == 2
        assert got[0].ts_ms == BASE_HOUR + 3 * 60_000

    def test_read_points_other_metric_isolated(self, svc):
        m = RefModel()
        register(svc, m)
        register(svc, m, name="other")
        svc.write_batch([pt(BASE_HOUR)], NOW)
        svc.write_batch([DataPoint("other", BASE_HOUR, 2.0, {"line": "A"})], NOW)
        assert len(svc.read_points("m", 0, NOW + HOUR)) == 1
        assert svc.read_points("m", 0, NOW + HOUR)[0].value == 1.0

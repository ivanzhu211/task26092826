from telemetry import DataPoint, QuarantineReason
from tests.conftest import BASE_HOUR, HOUR, register
from tests.model import RefModel, assert_reports_match

NOW = BASE_HOUR + 10 * HOUR


def pt(ts, value=1.0, tags=None):
    return DataPoint("m", ts, value, tags if tags is not None else {"line": "A"})


class TestQuarantine:
    def test_unregistered_tag_key_quarantined(self, svc):
        m = RefModel()
        register(svc, m)
        p = pt(BASE_HOUR, tags={"line": "A", "rogue": "x"})
        r = svc.write_batch([p], NOW)
        assert r.quarantined == 1 and r.accepted == 0
        m.apply_batch([p], NOW)
        records = svc.read_quarantine("m", 0, NOW + HOUR)
        assert len(records) == 1
        assert records[0].reason is QuarantineReason.UNREGISTERED_TAG_KEY
        assert records[0].tags == {"line": "A", "rogue": "x"}
        assert records[0].value == 1.0
        # quarantined points are not stored and do not affect slots
        assert svc.read_points("m", 0, NOW + HOUR) == []
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, NOW)

    def test_out_of_range_quarantined_bounds_inclusive(self, svc):
        m = RefModel()
        register(svc, m, lo=0.0, hi=100.0)
        points = [pt(BASE_HOUR, 0.0), pt(BASE_HOUR + 60_000, 100.0),
                  pt(BASE_HOUR + 120_000, -0.001), pt(BASE_HOUR + 180_000, 100.001)]
        r = svc.write_batch(points, NOW)
        assert (r.accepted, r.quarantined) == (2, 2)
        m.apply_batch(points, NOW)
        reasons = [rec.reason for rec in svc.read_quarantine("m", 0, NOW + HOUR)]
        assert reasons == [QuarantineReason.VALUE_OUT_OF_RANGE] * 2
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, NOW)

    def test_quarantine_interleaved_with_duplicates(self, svc):
        m = RefModel()
        register(svc, m)
        good = pt(BASE_HOUR, 1.0)
        batch = [
            good,                                   # stored
            pt(BASE_HOUR, 2.0),                     # duplicate of good
            pt(BASE_HOUR + 60_000, 999.0),          # quarantine: range
            pt(BASE_HOUR + 120_000, 3.0),           # stored
            pt(BASE_HOUR + 120_000, 4.0, {"x": "y"}),  # quarantine: tag key
            pt(BASE_HOUR + 120_000, 5.0),           # duplicate
        ]
        r = svc.write_batch(batch, NOW)
        assert (r.accepted, r.duplicates, r.quarantined) == (2, 2, 2)
        m.apply_batch(batch, NOW)
        rep = svc.hourly_report("m", BASE_HOUR, NOW)
        assert rep.stored == 2 and rep.duplicates == 2 and rep.quarantined == 2
        assert rep.duplicate_rate == 0.5
        # resend the same batch: quarantine happens again, and all four
        # non-quarantined points are now duplicates of stored points
        r2 = svc.write_batch(batch, NOW)
        assert (r2.accepted, r2.duplicates, r2.quarantined) == (0, 4, 2)
        m.apply_batch(batch, NOW)
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, NOW)

    def test_quarantined_points_do_not_extend_series_bounds(self, svc):
        m = RefModel()
        register(svc, m)
        svc.write_batch([pt(BASE_HOUR + 10 * HOUR, 1.0)], NOW)
        m.apply_batch([pt(BASE_HOUR + 10 * HOUR, 1.0)], NOW)
        # quarantined point far in the past must not create expectations
        svc.write_batch([pt(BASE_HOUR, 999.0)], NOW)
        m.apply_batch([pt(BASE_HOUR, 999.0)], NOW)
        rep = svc.hourly_report("m", BASE_HOUR + 5 * HOUR, NOW)
        assert rep.expected_slots == 0 and rep.missing_rate is None
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 11 * HOUR, NOW)

    def test_quarantine_counted_per_hour_of_point(self, svc):
        m = RefModel()
        register(svc, m)
        points = [pt(BASE_HOUR + HOUR - 1, 999.0), pt(BASE_HOUR + HOUR, 999.0)]
        svc.write_batch(points, NOW)
        m.apply_batch(points, NOW)
        r1 = svc.hourly_report("m", BASE_HOUR, NOW)
        r2 = svc.hourly_report("m", BASE_HOUR + HOUR, NOW)
        assert r1.quarantined == 1 and r2.quarantined == 1
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 2 * HOUR, NOW)

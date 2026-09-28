from telemetry import DataPoint
from tests.conftest import BASE_HOUR, HOUR, register
from tests.model import RefModel, assert_reports_match

NOW = BASE_HOUR + 10 * HOUR


def pt(ts, value=1.0, line="A"):
    return DataPoint("m", ts, value, {"line": line})


class TestDuplicates:
    def test_same_batch_duplicates_keep_first(self, svc):
        m = RefModel()
        register(svc, m)
        points = [pt(BASE_HOUR, 1.0), pt(BASE_HOUR, 2.0), pt(BASE_HOUR, 3.0)]
        r = svc.write_batch(points, NOW)
        assert (r.accepted, r.duplicates) == (1, 2)
        m.apply_batch(points, NOW)
        # first point wins
        stored = svc.read_points("m", 0, NOW + HOUR)
        assert len(stored) == 1 and stored[0].value == 1.0
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, NOW)

    def test_cross_batch_duplicates(self, svc):
        m = RefModel()
        register(svc, m)
        b1 = [pt(BASE_HOUR + i * 60_000, float(i)) for i in range(10)]
        b2 = [pt(BASE_HOUR + i * 60_000, 50.0 + i) for i in range(5, 15)]
        r1 = svc.write_batch(b1, NOW)
        r2 = svc.write_batch(b2, NOW)
        assert (r1.accepted, r1.duplicates) == (10, 0)
        assert (r2.accepted, r2.duplicates) == (5, 5)
        m.apply_batch(b1, NOW)
        m.apply_batch(b2, NOW)
        stored = svc.read_points("m", 0, NOW + HOUR)
        assert len(stored) == 15
        # values from the first batch survive for the overlapping timestamps
        by_ts = {p.ts_ms: p.value for p in stored}
        assert by_ts[BASE_HOUR + 5 * 60_000] == 5.0
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 2 * HOUR, NOW)

    def test_tag_order_and_case_sensitivity(self, svc):
        m = RefModel()
        register(svc, m, tags=("line", "cell"))
        # same tag set, different order in the dict -> same series -> duplicate
        p1 = DataPoint("m", BASE_HOUR, 1.0, {"line": "A", "cell": "1"})
        p2 = DataPoint("m", BASE_HOUR, 2.0, {"cell": "1", "line": "A"})
        # case-sensitive: different series
        p3 = DataPoint("m", BASE_HOUR, 3.0, {"line": "a", "cell": "1"})
        r = svc.write_batch([p1, p2, p3], NOW)
        assert (r.accepted, r.duplicates) == (2, 1)
        m.apply_batch([p1, p2, p3], NOW)
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, NOW)

    def test_resent_full_batch_counts_all_duplicates(self, svc):
        m = RefModel()
        register(svc, m)
        batch = [pt(BASE_HOUR + i * 60_000, float(i)) for i in range(30)]
        svc.write_batch(batch, NOW)
        r = svc.write_batch(batch, NOW)  # gateway resends the whole batch
        assert (r.accepted, r.duplicates) == (0, 30)
        m.apply_batch(batch, NOW)
        m.apply_batch(batch, NOW)
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, NOW)

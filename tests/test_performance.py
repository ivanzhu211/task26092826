import time

from telemetry import DataPoint, Retention
from tests.conftest import BASE_HOUR, HOUR, register
from tests.model import RefModel, assert_reports_match

DAY = 24 * HOUR
NOW = BASE_HOUR + 3 * DAY  # points span ~2.8 days, all within retention
N_SERIES = 50
N_POINTS = 200_000
BATCH = 5_000


def build_points():
    points = []
    for i in range(N_POINTS):
        series = i % N_SERIES
        slot = i // N_SERIES  # 4000 distinct slots per series
        ts = BASE_HOUR + slot * 60_000
        points.append(DataPoint("m", ts, float(i % 97), {"line": f"L{series}"}))
    return points


class TestPerformance:
    def test_write_and_report_performance(self, svc):
        m = RefModel()
        register(svc, m, tags=("line",), interval=60_000,
                 retention=Retention.DAYS_30)
        points = build_points()

        worst_batch = 0.0
        for start in range(0, N_POINTS, BATCH):
            batch = points[start:start + BATCH]
            t0 = time.perf_counter()
            r = svc.write_batch(batch, NOW)
            elapsed = time.perf_counter() - t0
            worst_batch = max(worst_batch, elapsed)
            assert r.accepted == BATCH
        assert worst_batch < 1.0, f"batch write took {worst_batch:.3f}s"

        # single-hour report: well under 50ms
        svc.hourly_report("m", BASE_HOUR, NOW)  # warm up
        worst = 0.0
        for h in range(10):
            t0 = time.perf_counter()
            svc.hourly_report("m", BASE_HOUR + h * HOUR, NOW)
            worst = max(worst, time.perf_counter() - t0)
        assert worst < 0.05, f"single-hour report took {worst*1000:.1f}ms"

        # 30 days of hourly reports in one call: under 1s
        t0 = time.perf_counter()
        reports = svc.hourly_reports("m", BASE_HOUR, BASE_HOUR + 30 * DAY, NOW)
        elapsed = time.perf_counter() - t0
        assert len(reports) == 30 * 24
        assert elapsed < 1.0, f"30-day report took {elapsed:.3f}s"

        # spot-check correctness against the reference model
        for start in range(0, N_POINTS, BATCH):
            m.apply_batch(points[start:start + BATCH], NOW)
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 6 * HOUR, NOW)

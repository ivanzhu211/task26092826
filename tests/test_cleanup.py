import sqlite3
import threading

from telemetry import DataPoint, Retention
from tests.conftest import BASE_HOUR, HOUR, make_service, register
from tests.model import RefModel, assert_reports_match

DAY = 24 * HOUR
NOW = BASE_HOUR + 100 * DAY


def pt(ts, value=1.0, line="A"):
    return DataPoint("m", ts, value, {"line": line})


class TestCleanup:
    def test_expired_hours_fully_removed(self, tmp_path):
        svc = make_service(tmp_path)
        m = RefModel()
        register(svc, m, retention=Retention.DAYS_30)
        now = BASE_HOUR + 40 * DAY  # retention boundary sits at day 10
        # points spread across 40 days, some quarantined
        batches = []
        for d in range(40):
            batch = [pt(BASE_HOUR + d * DAY + i * 60_000) for i in range(30)]
            batch.append(pt(BASE_HOUR + d * DAY, 999.0))  # quarantined
            batches.append(batch)
        for b in batches:
            svc.write_batch(b, now)
            m.apply_batch(b, now)

        svc.cleanup(now)
        boundary = now - 30 * DAY
        boundary_hour = boundary - (boundary % HOUR)
        # nothing remains in expired hours: points, quarantine, stats
        assert svc.read_points("m", 0, boundary_hour) == []
        assert svc.read_quarantine("m", 0, boundary_hour) == []
        raw = sqlite3.connect(svc._store._conn.execute("PRAGMA database_list").fetchone()[2])
        for table, col in (("points", "ts_ms"), ("quarantine", "ts_ms"),
                           ("occupied_slots", "slot_ms"), ("hourly_stats", "hour_ms")):
            n = raw.execute(
                f"SELECT COUNT(*) FROM {table} WHERE metric='m' AND {col} < ?",
                (boundary_hour,),
            ).fetchone()[0]
            assert n == 0, f"{table} still has expired rows"
        raw.close()
        # expired hours report as expired; recent hours unaffected
        old = svc.hourly_report("m", BASE_HOUR, now)
        assert old.expired and old.stored == 0 and old.missing_rate is None
        recent = svc.hourly_report("m", BASE_HOUR + 39 * DAY, now)
        assert not recent.expired and recent.stored == 30 and recent.quarantined == 1
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 40 * DAY, now)
        svc.close()

    def test_series_bounds_survive_cleanup(self, tmp_path):
        svc = make_service(tmp_path)
        m = RefModel()
        register(svc, m, retention=Retention.DAYS_30)
        now = BASE_HOUR + 34 * DAY
        # series spans from day 5 to day 34 -> expectations exist in between
        svc.write_batch([pt(BASE_HOUR + 5 * DAY)], now)
        svc.write_batch([pt(BASE_HOUR + 34 * DAY)], now)
        m.apply_batch([pt(BASE_HOUR + 5 * DAY)], now)
        m.apply_batch([pt(BASE_HOUR + 34 * DAY)], now)
        svc.cleanup(now)
        # a non-expired middle hour still counts every slot as expected
        mid = svc.hourly_report("m", BASE_HOUR + 33 * DAY, now)
        assert mid.expected_slots == 60 and mid.missing_rate == 1.0
        assert_reports_match(svc, m, "m", BASE_HOUR + 4 * DAY, BASE_HOUR + 35 * DAY, now)
        svc.close()

    def test_cleanup_concurrent_with_writes(self, tmp_path):
        svc = make_service(tmp_path)
        m = RefModel()
        register(svc, m, retention=Retention.DAYS_30)
        stop = threading.Event()
        clock_lock = threading.Lock()
        clock = {"now": BASE_HOUR + 31 * DAY}

        def tick():
            with clock_lock:
                clock["now"] += HOUR
                return clock["now"]

        def cleaner():
            while not stop.is_set():
                svc.cleanup(tick())

        threads = [threading.Thread(target=cleaner) for _ in range(2)]
        for t in threads:
            t.start()
        # writers keep writing recent data while cleanup runs
        batches = []
        for i in range(40):
            now = tick()
            batch = [pt(now - 2 * HOUR + j * 60_000, float(j), line=f"L{j % 3}")
                     for j in range(20)]
            r = svc.write_batch(batch, now)
            assert not r.batch_rejected
            batches.append((batch, now))
        stop.set()
        for t in threads:
            t.join()

        final_now = tick()
        svc.cleanup(final_now)
        for batch, now in batches:
            m.apply_batch(batch, now)
        boundary = final_now - 30 * DAY
        boundary_hour = boundary - (boundary % HOUR)
        # after cleanup, no expired residue anywhere
        assert svc.read_points("m", 0, boundary_hour) == []
        assert svc.read_quarantine("m", 0, boundary_hour) == []
        assert_reports_match(
            svc, m, "m", boundary_hour, boundary_hour + 3 * DAY, final_now
        )
        svc.close()

    def test_forever_retention_cleanup_is_noop(self, tmp_path):
        svc = make_service(tmp_path)
        m = RefModel()
        register(svc, m, retention=Retention.FOREVER)
        svc.write_batch([pt(BASE_HOUR)], NOW)
        m.apply_batch([pt(BASE_HOUR)], NOW)
        svc.cleanup(NOW + 1000 * DAY)
        rep = svc.hourly_report("m", BASE_HOUR, NOW + 1000 * DAY)
        assert rep.stored == 1 and not rep.expired
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, NOW + 1000 * DAY)
        svc.close()

import threading
from concurrent.futures import ThreadPoolExecutor

from telemetry import DataPoint
from tests.conftest import BASE_HOUR, HOUR, register
from tests.model import RefModel, assert_reports_match

NOW = BASE_HOUR + 500 * HOUR
THREADS = 8
BATCHES_PER_THREAD = 15
BATCH_SIZE = 100


def make_batch(thread: int, batch: int) -> list[DataPoint]:
    # Threads deliberately overlap on the same series and timestamps:
    # timestamps cycle through a shared 3-hour window.
    points = []
    for i in range(BATCH_SIZE):
        slot = (thread * 7 + batch * 13 + i) % 180
        ts = BASE_HOUR + slot * 60_000
        line = f"L{(thread + batch + i) % 4}"
        points.append(DataPoint("m", ts, float(i), {"line": line}))
    return points


class TestConcurrentWrites:
    def test_overlapping_batches_match_sequential_model(self, tmp_path):
        from tests.conftest import make_service
        svc = make_service(tmp_path)
        m = RefModel()
        register(svc, m)
        all_batches = [
            make_batch(t, b) for t in range(THREADS) for b in range(BATCHES_PER_THREAD)
        ]
        results = [None] * len(all_batches)

        def run(idx):
            results[idx] = svc.write_batch(all_batches[idx], NOW)

        with ThreadPoolExecutor(max_workers=THREADS) as pool:
            list(pool.map(run, range(len(all_batches))))

        # Aggregates are order-independent: every distinct valid point is
        # stored exactly once, every other occurrence is a duplicate.
        totals = {"accepted": 0, "duplicates": 0, "quarantined": 0}
        for batch, result in zip(all_batches, results):
            assert result is not None and not result.batch_rejected
            expected = m.apply_batch(batch, NOW)
            totals["accepted"] += result.accepted
            totals["duplicates"] += result.duplicates
            totals["quarantined"] += result.quarantined
            assert result.quarantined == expected["quarantined"]
        assert totals["accepted"] == len(m.points["m"])
        occ_total = sum(m.stored["m"].values()) + sum(m.dups["m"].values())
        assert totals["accepted"] + totals["duplicates"] == occ_total
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 3 * HOUR, NOW)
        svc.close()

    def test_concurrent_reads_never_see_partial_batch(self, tmp_path):
        from tests.conftest import make_service
        svc = make_service(tmp_path)
        m = RefModel()
        register(svc, m)
        stop = threading.Event()
        inconsistencies = []

        def reader():
            while not stop.is_set():
                for h in range(3):
                    rep = svc.hourly_report("m", BASE_HOUR + h * HOUR, NOW)
                    if not (0 <= rep.occupied_slots <= rep.expected_slots):
                        inconsistencies.append((h, rep))
                    if rep.missing_rate is not None and not 0.0 <= rep.missing_rate <= 1.0:
                        inconsistencies.append((h, rep))

        readers = [threading.Thread(target=reader) for _ in range(3)]
        for t in readers:
            t.start()
        batches = [make_batch(0, b) for b in range(30)]
        for batch in batches:
            svc.write_batch(batch, NOW)
            m.apply_batch(batch, NOW)
        stop.set()
        for t in readers:
            t.join()
        assert inconsistencies == []
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 3 * HOUR, NOW)
        svc.close()

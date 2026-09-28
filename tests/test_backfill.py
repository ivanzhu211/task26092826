from telemetry import DataPoint
from tests.conftest import BASE_HOUR, HOUR, register
from tests.model import RefModel, assert_reports_match

# interval = 1 minute -> 60 slots per hour
NOW = BASE_HOUR + 100 * HOUR


def pt(ts, line="A"):
    return DataPoint("m", ts, 1.0, {"line": line})


class TestBackfill:
    def test_gap_hours_fully_missing_then_filled(self, svc):
        m = RefModel()
        register(svc, m)
        # sensor online at hour 0, then a 3-hour outage, back at hour 4
        b1 = [pt(BASE_HOUR + i * 60_000) for i in range(60)]
        b2 = [pt(BASE_HOUR + 4 * HOUR + i * 60_000) for i in range(60)]
        svc.write_batch(b1, NOW)
        m.apply_batch(b1, NOW)
        svc.write_batch(b2, NOW)
        m.apply_batch(b2, NOW)
        # outage hours: every slot expected, none occupied
        for h in (1, 2, 3):
            rep = svc.hourly_report("m", BASE_HOUR + h * HOUR, NOW)
            assert rep.expected_slots == 60
            assert rep.occupied_slots == 0
            assert rep.missing_rate == 1.0
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 5 * HOUR, NOW)

        # backfill arrives for the outage window
        backfill = []
        for h in (1, 2, 3):
            backfill += [pt(BASE_HOUR + h * HOUR + i * 60_000) for i in range(60)]
        r = svc.write_batch(backfill, NOW)
        assert r.accepted == 180
        m.apply_batch(backfill, NOW)
        for h in (1, 2, 3):
            rep = svc.hourly_report("m", BASE_HOUR + h * HOUR, NOW)
            assert rep.missing_rate == 0.0
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 5 * HOUR, NOW)

    def test_backfill_extends_min_and_max(self, svc):
        m = RefModel()
        register(svc, m)
        # initial data only in hour 10
        b = [pt(BASE_HOUR + 10 * HOUR + i * 60_000) for i in range(60)]
        svc.write_batch(b, NOW)
        m.apply_batch(b, NOW)
        # hour 5 has no expectation yet
        rep = svc.hourly_report("m", BASE_HOUR + 5 * HOUR, NOW)
        assert rep.expected_slots == 0 and rep.missing_rate is None
        # late backfill pushes the earliest time back to hour 5 and the
        # latest forward to hour 12; every hour in between becomes expected
        early = [pt(BASE_HOUR + 5 * HOUR + 30 * 60_000)]
        late = [pt(BASE_HOUR + 12 * HOUR + 30 * 60_000)]
        svc.write_batch(early, NOW)
        svc.write_batch(late, NOW)
        m.apply_batch(early, NOW)
        m.apply_batch(late, NOW)
        for h in list(range(6, 10)) + [11]:
            rep = svc.hourly_report("m", BASE_HOUR + h * HOUR, NOW)
            assert rep.expected_slots == 60, f"hour {h}"
            assert rep.missing_rate == 1.0, f"hour {h}"
        rep5 = svc.hourly_report("m", BASE_HOUR + 5 * HOUR, NOW)
        assert rep5.expected_slots == 30 and rep5.occupied_slots == 1
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 13 * HOUR, NOW)

    def test_slot_alignment_from_epoch(self, svc):
        m = RefModel()
        register(svc, m, interval=900_000)  # 15 minutes
        # a point exactly on the hour belongs to the new hour
        svc.write_batch([pt(BASE_HOUR + HOUR)], NOW)
        m.apply_batch([pt(BASE_HOUR + HOUR)], NOW)
        rep_prev = svc.hourly_report("m", BASE_HOUR, NOW)
        rep_new = svc.hourly_report("m", BASE_HOUR + HOUR, NOW)
        assert rep_prev.stored == 0 and rep_prev.expected_slots == 0
        assert rep_new.stored == 1 and rep_new.occupied_slots == 1
        assert rep_new.expected_slots == 1
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 2 * HOUR, NOW)

    def test_multiple_series_expected_slots_add_up(self, svc):
        m = RefModel()
        register(svc, m)
        b = []
        for line in ("A", "B", "C"):
            b += [DataPoint("m", BASE_HOUR + i * 60_000, 1.0, {"line": line})
                  for i in range(0, 60, 2)]  # half the slots each
        svc.write_batch(b, NOW)
        m.apply_batch(b, NOW)
        rep = svc.hourly_report("m", BASE_HOUR, NOW)
        # each series spans slots 0..58 -> 59 expected slots, 30 occupied
        assert rep.expected_slots == 177
        assert rep.occupied_slots == 90
        assert rep.missing_rate == 1.0 - 90 / 177
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + HOUR, NOW)

import pytest

from telemetry import DataPoint, TelemetryService
from tests.conftest import BASE_HOUR, HOUR, register
from tests.model import RefModel, assert_reports_match

NOW = BASE_HOUR + 50 * HOUR


def pt(ts, value=1.0, line="A"):
    return DataPoint("m", ts, value, {"line": line})


class TestDurability:
    def test_data_survives_reopen(self, tmp_path):
        db = str(tmp_path / "t.db")
        svc = TelemetryService(db)
        m = RefModel()
        register(svc, m)
        batch = [pt(BASE_HOUR + i * 60_000, float(i)) for i in range(30)]
        svc.write_batch(batch, NOW)
        m.apply_batch(batch, NOW)
        svc.close()

        svc2 = TelemetryService(db)
        assert svc2.get_metric("m") is not None
        assert len(svc2.read_points("m", 0, NOW + HOUR)) == 30
        assert_reports_match(svc2, m, "m", BASE_HOUR, BASE_HOUR + HOUR, NOW)
        svc2.close()

    def test_failed_batch_rolls_back_fully(self, tmp_path, monkeypatch):
        db = str(tmp_path / "t.db")
        svc = TelemetryService(db)
        m = RefModel()
        register(svc, m)
        good = [pt(BASE_HOUR + i * 60_000) for i in range(10)]
        svc.write_batch(good, NOW)
        m.apply_batch(good, NOW)

        from telemetry.store import Store
        original = Store.try_insert_point
        calls = {"n": 0}

        def flaky(self, *args):
            calls["n"] += 1
            if calls["n"] == 3:
                raise RuntimeError("simulated crash mid-batch")
            return original(self, *args)

        monkeypatch.setattr(Store, "try_insert_point", flaky)
        doomed = [pt(BASE_HOUR + HOUR + i * 60_000) for i in range(10)]
        with pytest.raises(RuntimeError):
            svc.write_batch(doomed, NOW)
        monkeypatch.undo()

        # nothing from the failed batch took effect
        assert svc.read_points("m", BASE_HOUR + HOUR, BASE_HOUR + 2 * HOUR) == []
        rep = svc.hourly_report("m", BASE_HOUR + HOUR, NOW)
        assert rep.stored == 0 and rep.occupied_slots == 0
        # earlier data and reports still consistent
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 2 * HOUR, NOW)
        # service still usable afterwards
        svc.write_batch(doomed, NOW)
        m.apply_batch(doomed, NOW)
        assert_reports_match(svc, m, "m", BASE_HOUR, BASE_HOUR + 2 * HOUR, NOW)
        svc.close()

    def test_registration_is_immutable(self, tmp_path):
        svc = TelemetryService(str(tmp_path / "t.db"))
        svc.register_metric("m", ["line"], 0.0, 100.0, 60_000,
                            retention=__import__("telemetry").Retention.DAYS_30)
        with pytest.raises(ValueError):
            svc.register_metric("m", ["other"], -1.0, 1.0, 30_000,
                                retention=__import__("telemetry").Retention.FOREVER)
        spec = svc.get_metric("m")
        assert spec.allowed_tag_keys == frozenset({"line"})
        assert spec.interval_ms == 60_000
        svc.close()

    def test_invalid_registration_rejected(self, tmp_path):
        svc = TelemetryService(str(tmp_path / "t.db"))
        from telemetry import Retention
        with pytest.raises(ValueError):
            svc.register_metric("bad name", [], 0, 1, 60_000, Retention.DAYS_30)
        with pytest.raises(ValueError):
            svc.register_metric("ok", [], 0, 1, 7_000, Retention.DAYS_30)  # not divisor
        with pytest.raises(ValueError):
            svc.register_metric("ok", [], 0, 1, 500, Retention.DAYS_30)  # < 1s
        with pytest.raises(ValueError):
            svc.register_metric("ok", [], 10.0, 1.0, 60_000, Retention.DAYS_30)
        svc.close()

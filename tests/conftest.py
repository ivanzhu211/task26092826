import pytest

from telemetry import DataPoint, Retention, TelemetryService
from telemetry.models import MetricSpec
from tests.model import RefModel

HOUR = 3_600_000
BASE = 1_700_000_000_000  # fixed arbitrary epoch ms, hour-aligned below
BASE_HOUR = BASE - (BASE % HOUR)


@pytest.fixture
def svc(tmp_path):
    service = TelemetryService(str(tmp_path / "telemetry.db"))
    yield service
    service.close()


def make_service(tmp_path):
    return TelemetryService(str(tmp_path / "telemetry.db"))


def register(svc, model, name="m", tags=("line",), lo=0.0, hi=100.0,
             interval=60_000, retention=Retention.DAYS_30):
    spec = svc.register_metric(name, tags, lo, hi, interval, retention)
    model.register(spec)
    return spec

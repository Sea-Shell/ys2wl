import pytest
from httpx import ASGITransport, AsyncClient
from prometheus_client import make_asgi_app
from sortarr.metrics import (
    api_calls_total,
    videos_added_total,
    videos_skipped_total,
    errors_total,
    pipeline_duration_seconds,
    last_pipeline_status,
    subscriptions_processed_total,
)


def test_metrics_exist():
    assert api_calls_total._name == "sortarr_api_calls"
    assert videos_added_total._name == "sortarr_videos_added"
    assert errors_total._name == "sortarr_errors"


def test_counter_increment():
    videos_added_total.inc()
    assert videos_added_total._value.get() > 0


def test_histogram_observe():
    pipeline_duration_seconds.observe(120.5)
    assert pipeline_duration_seconds._sum.get() > 0


def test_gauge_set():
    last_pipeline_status.set(1)
    assert last_pipeline_status._value.get() == 1


def test_gauge_inc_dec():
    subscriptions_processed_total.inc(5)
    assert subscriptions_processed_total._value.get() >= 5


def test_counter_labels():
    videos_skipped_total.labels(reason="too_short").inc()
    assert videos_skipped_total.labels(reason="too_short")._value.get() > 0


@pytest.mark.asyncio
async def test_metrics_asgi_endpoint_renders():
    app = make_asgi_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/")
    assert resp.status_code == 200
    assert "# HELP" in resp.text
    assert "# TYPE" in resp.text

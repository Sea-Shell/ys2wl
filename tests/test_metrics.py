from ys2wl.metrics import (
    api_calls_total, videos_added_total, videos_skipped_total,
    errors_total, pipeline_duration_seconds,
)


def test_metrics_exist():
    assert api_calls_total._name == "ys2wl_api_calls"
    assert videos_added_total._name == "ys2wl_videos_added"
    assert errors_total._name == "ys2wl_errors"


def test_counter_increment():
    videos_added_total.inc()
    assert videos_added_total._value.get() > 0

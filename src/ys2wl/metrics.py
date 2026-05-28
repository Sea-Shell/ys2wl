from prometheus_client import Counter, Histogram, Gauge

api_calls_total = Counter(
    "ys2wl_api_calls_total", "Total YouTube API calls", ["endpoint"],
)
videos_added_total = Counter(
    "ys2wl_videos_added_total", "Videos added to playlists",
)
videos_skipped_total = Counter(
    "ys2wl_videos_skipped_total", "Videos skipped by filters", ["reason"],
)
errors_total = Counter(
    "ys2wl_errors_total", "Total errors encountered",
)
pipeline_duration_seconds = Histogram(
    "ys2wl_pipeline_duration_seconds", "Pipeline run duration in seconds",
    buckets=[30, 60, 120, 300, 600, 1800, 3600],
)
subscriptions_processed_total = Counter(
    "ys2wl_subscriptions_processed_total", "Subscriptions processed",
)
subscriptions_skipped_total = Counter(
    "ys2wl_subscriptions_skipped_total", "Subscriptions skipped",
)
last_pipeline_status = Gauge(
    "ys2wl_last_pipeline_status", "Last pipeline run status (1=success, 0=fail)",
)
quota_estimate = Gauge(
    "ys2wl_quota_estimate", "Estimated YouTube API quota used in current run",
)

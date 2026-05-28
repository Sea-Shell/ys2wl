from ys2wl.models.pipeline import FilterResult


def duration_filter(duration_seconds: int, minimum: int, maximum: int) -> FilterResult:
    if minimum > 0 and duration_seconds < minimum:
        return FilterResult(
            passed=False,
            reason=f"Duration {duration_seconds}s < minimum {minimum}s",
            skipped_by="duration_filter",
        )
    if maximum > 0 and duration_seconds > maximum:
        return FilterResult(
            passed=False,
            reason=f"Duration {duration_seconds}s > maximum {maximum}s",
            skipped_by="duration_filter",
        )
    return FilterResult(passed=True)

import re
from sortarr.models.pipeline import FilterResult, PipelineSelector
from sortarr.models.youtube import Activity


def selector_filter(
    activity: Activity,
    channel_title: str,
    selectors: list[PipelineSelector],
    mode: str = "AND",  # kept for backward compat
) -> FilterResult:
    if not selectors:
        return FilterResult(passed=True)

    result = None
    for sel in selectors:
        value = _get_field_value(activity, channel_title, sel.field)
        matched = _matches(value, sel.operator, sel.pattern)
        if result is None:
            result = matched
        else:
            op = sel.combine_operator.upper()
            if op == "OR":
                result = result or matched
            else:
                result = result and matched

    if not result:
        return FilterResult(
            passed=False,
            reason="Selector did not match",
            skipped_by="selector",
        )
    return FilterResult(passed=True)


def _get_field_value(activity: Activity, channel_title: str, field: str) -> str:
    mapping = {
        "title": activity.title,
        "video_title": activity.title,
        "channel_title": channel_title,
        "description": activity.description or "",
    }
    return mapping.get(field, "")


def _matches(value: str, operator: str, pattern: str) -> bool:
    if not value or not pattern:
        return False
    if operator == "contains":
        return pattern.lower() in value.lower()
    elif operator == "regex":
        try:
            return bool(re.search(pattern, value, re.IGNORECASE))
        except re.error:
            return False
    elif operator == "equals":
        return value.lower() == pattern.lower()
    return False

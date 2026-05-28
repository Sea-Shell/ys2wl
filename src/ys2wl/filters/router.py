import re
from typing import Optional
from ys2wl.models.youtube import Activity, RoutingRule
from ys2wl.models.pipeline import RouteResult


def _get_field_value(
    activity: Activity, channel_title: str, duration_seconds: int, field: Optional[str]
) -> Optional[str]:
    mapping = {
        "channel_title": channel_title,
        "video_title": activity.title,
        "video_type": activity.video_type,
    }
    if field in mapping:
        return mapping[field]
    if field == "duration_seconds":
        return str(duration_seconds)
    return None


def _matches(value: Optional[str], operator: str, pattern: Optional[str]) -> bool:
    if value is None or pattern is None:
        return False
    if operator == "contains":
        return pattern.lower() in value.lower()
    elif operator == "regex":
        return bool(re.search(pattern, value, re.IGNORECASE))
    elif operator == "equals":
        return value.lower() == pattern.lower()
    elif operator == "lt":
        try:
            return float(value) < float(pattern)
        except ValueError:
            return False
    elif operator == "gt":
        try:
            return float(value) > float(pattern)
        except ValueError:
            return False
    return False


def evaluate_rules(
    activity: Activity,
    channel_title: str,
    duration_seconds: int,
    rules: list[RoutingRule],
    default_playlist_id: str,
    default_playlist_title: str,
) -> RouteResult:
    for rule in sorted(rules, key=lambda r: r.priority, reverse=True):
        if not rule.enabled:
            continue
        value = _get_field_value(activity, channel_title, duration_seconds, rule.field)
        if _matches(value, rule.operator, rule.pattern):
            return RouteResult(
                playlist_id=rule.destination_playlist_id,
                playlist_title=rule.destination_playlist_title or "",
                rule_name=rule.name,
            )
    return RouteResult(
        playlist_id=default_playlist_id,
        playlist_title=default_playlist_title,
        rule_name="default",
    )

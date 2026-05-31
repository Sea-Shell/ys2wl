import re
from typing import Optional
from ys2wl.models.youtube import Activity, RoutingRule
from ys2wl.models.pipeline import RouteResult
from ys2wl.core.utils import time_to_seconds


def _get_field_value(
    activity: Activity, channel_title: str, duration_seconds: int, field: Optional[str]
) -> Optional[str]:
    alias = {"title": "video_title", "duration": "duration_seconds"}
    resolved = alias.get(field, field)
    mapping = {
        "channel_title": channel_title,
        "video_title": activity.title,
        "video_type": activity.video_type,
    }
    if resolved in mapping:
        return mapping[resolved]
    if resolved == "duration_seconds":
        return str(duration_seconds)
    return None


def _matches(value: Optional[str], operator: str, pattern: Optional[str]) -> bool:
    if value is None:
        return False
    if not pattern:
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
) -> Optional[RouteResult]:
    catch_all_rule = None
    for rule in sorted(rules, key=lambda r: r.priority, reverse=True):
        if not rule.enabled:
            continue
        if rule.catch_all:
            catch_all_rule = rule
            continue
        value = _get_field_value(activity, channel_title, duration_seconds, rule.field)
        if not _matches(value, rule.operator, rule.pattern):
            continue
        # Check per-rule duration bounds — fall through if outside
        min_sec = time_to_seconds(rule.minimum_length)
        max_sec = time_to_seconds(rule.maximum_length)
        if min_sec > 0 and duration_seconds < min_sec:
            continue
        if max_sec > 0 and duration_seconds > max_sec:
            continue
        return RouteResult(
            playlist_id=rule.destination_playlist_id,
            playlist_title=rule.destination_playlist_title or "",
            rule_name=rule.name,
        )
    if catch_all_rule:
        return RouteResult(
            playlist_id=catch_all_rule.destination_playlist_id,
            playlist_title=catch_all_rule.destination_playlist_title or "",
            rule_name=catch_all_rule.name,
        )
    return None  # no rule matched

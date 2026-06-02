from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FilterResult:
    passed: bool
    reason: str = ""
    skipped_by: str = ""


@dataclass
class RouteResult:
    playlist_id: str
    playlist_title: str
    rule_name: str = "default"


@dataclass
class VideoResult:
    video_id: str
    title: str
    subscription_title: str
    subscription_id: str
    pipeline_id: str = ""
    pipeline_name: str = ""
    filter_result: Optional[FilterResult] = None
    route_result: Optional[RouteResult] = None
    added: bool = False
    error: Optional[str] = None


@dataclass
class PipelineSummary:
    started_at: str
    finished_at: Optional[str] = None
    status: str = "running"
    subscriptions_processed: int = 0
    subscriptions_skipped: int = 0
    videos_added: int = 0
    videos_skipped: int = 0
    errors: int = 0
    error_message: str = ""
    trigger: str = "scheduled"
    video_results: list[VideoResult] = field(default_factory=list)
    subscription_skips: list[dict] = field(default_factory=list)
    pipelines_invoked: int = 0
    pipelines_with_errors: int = 0


@dataclass
class PipelineConfig:
    id: str
    name: str
    enabled: bool = True
    selector_mode: str = "AND"
    duration_min_seconds: int = 0
    duration_max_seconds: int = 0
    check_db_exists: bool = False
    check_title_similarity: bool = False
    compare_distance: int = 80
    subscription_scope: str = "all"  # "all" | "selected"
    destination_playlist_id: str = ""
    destination_playlist_title: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass
class IgnoreList:
    id: str
    name: str
    list_type: str  # "word" | "video" | "subscription"
    created_at: str = ""
    entries: list[str] = field(default_factory=list)


@dataclass
class IgnoreListEntry:
    id: str
    ignore_list_id: str
    value: str
    created_at: str = ""


@dataclass
class PipelineSelector:
    id: str = ""
    pipeline_id: str = ""
    field: str = "title"  # "title" | "channel_title" | "description"
    operator: str = "contains"  # "contains" | "regex" | "equals"
    pattern: str = ""
    combine_operator: str = (
        "AND"  # "AND" | "OR" — how to combine with previous selector
    )
    created_at: str = ""

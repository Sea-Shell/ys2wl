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

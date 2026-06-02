from pydantic import BaseModel, Field
from typing import Optional


class HealthResponse(BaseModel):
    status: str = "ok"
    next_scheduled_run: str | None = None


class ConfigResponse(BaseModel):
    schedule: str
    compare_distance: int
    reprocess_days: int
    playlist_sleep: int
    subscription_sleep: int
    pipeline_concurrency: int
    activity_limit: int
    subscription_limit: int
    log_level: str
    published_after: Optional[str] = None
    no_webbrowser: bool
    public_url: str = ""
    credentials_file: str = ""


class ConfigUpdate(BaseModel):
    schedule: Optional[str] = None
    compare_distance: Optional[int] = Field(None, ge=0, le=100)
    reprocess_days: Optional[int] = Field(None, ge=0)
    playlist_sleep: Optional[int] = Field(None, ge=0)
    subscription_sleep: Optional[int] = Field(None, ge=0)
    pipeline_concurrency: Optional[int] = Field(None, ge=1, le=10)
    activity_limit: Optional[int] = Field(None, ge=0)
    subscription_limit: Optional[int] = Field(None, ge=0)
    log_level: Optional[str] = None
    published_after: Optional[str] = None
    no_webbrowser: Optional[bool] = None
    public_url: Optional[str] = None
    credentials_file: Optional[str] = None


class IgnoreEntryCreate(BaseModel):
    type: str  # subscription, video, words (legacy)
    pattern: str


class IgnoreEntryUpdate(BaseModel):
    pattern: str


class IgnoreEntryResponse(BaseModel):
    id: int
    type: str
    pattern: str
    created_at: str


class RoutingRuleCreate(BaseModel):
    name: str
    priority: int = 0
    field: Optional[str] = None
    operator: str = "contains"
    pattern: Optional[str] = None
    destination_playlist_id: str
    destination_playlist_title: str = ""
    minimum_length: str = "0s"
    maximum_length: str = "0s"
    catch_all: bool = False


class RoutingRuleUpdate(BaseModel):
    name: Optional[str] = None
    priority: Optional[int] = None
    field: Optional[str] = None
    operator: Optional[str] = None
    pattern: Optional[str] = None
    destination_playlist_id: Optional[str] = None
    destination_playlist_title: Optional[str] = None
    enabled: Optional[bool] = None
    minimum_length: Optional[str] = None
    maximum_length: Optional[str] = None
    catch_all: Optional[bool] = None


class RoutingRuleResponse(BaseModel):
    id: int
    name: str
    priority: int
    field: Optional[str] = None
    operator: str
    pattern: Optional[str] = None
    destination_playlist_id: str
    destination_playlist_title: str
    enabled: bool
    minimum_length: str = "0s"
    maximum_length: str = "0s"
    catch_all: bool = False


# --- Pipeline models ---


class PipelineCreate(BaseModel):
    name: str
    selector_mode: str = "AND"
    duration_min_seconds: int = 0
    duration_max_seconds: int = 0
    check_db_exists: bool = False
    check_title_similarity: bool = False
    compare_distance: int = 80
    subscription_scope: str = "all"
    destination_playlist_id: str
    destination_playlist_title: str = ""


class PipelineUpdate(BaseModel):
    name: Optional[str] = None
    enabled: Optional[bool] = None
    selector_mode: Optional[str] = None
    duration_min_seconds: Optional[int] = None
    duration_max_seconds: Optional[int] = None
    check_db_exists: Optional[bool] = None
    check_title_similarity: Optional[bool] = None
    compare_distance: Optional[int] = None
    subscription_scope: Optional[str] = None
    destination_playlist_id: Optional[str] = None
    destination_playlist_title: Optional[str] = None


class PipelineResponse(BaseModel):
    id: str
    name: str
    enabled: bool
    selector_mode: str
    duration_min_seconds: int
    duration_max_seconds: int
    check_db_exists: bool
    check_title_similarity: bool
    compare_distance: int
    subscription_scope: str
    destination_playlist_id: str
    destination_playlist_title: str
    created_at: str
    updated_at: str
    ignore_list_ids: list[str] = []
    selectors: list[dict] = []


class PipelineRunResponse(BaseModel):
    id: int
    started_at: str
    finished_at: Optional[str] = None
    status: str
    subscriptions_processed: int = 0
    subscriptions_skipped: int = 0
    videos_added: int = 0
    videos_skipped: int = 0
    errors: int = 0
    trigger: str = "scheduled"
    pipelines_invoked: int = 0
    pipelines_with_errors: int = 0
    dry_run: bool = False


class RunDecisionResponse(BaseModel):
    id: int
    video_id: Optional[str] = None
    title: Optional[str] = None
    subscription_title: Optional[str] = None
    action: str
    reason: Optional[str] = None
    reason_detail: Optional[str] = None
    routed_to: Optional[str] = None
    pipeline_id: Optional[str] = None
    pipeline_name: Optional[str] = None
    created_at: str


class TriggerResponse(BaseModel):
    run_id: int
    message: str = "Pipeline triggered"


class IgnoreListResponse(BaseModel):
    id: str
    name: str
    list_type: str
    created_at: str
    entries: list[str] = []


class IgnoreListCreate(BaseModel):
    name: str
    list_type: str  # "word" | "video" | "subscription"


class IgnoreListEntryCreate(BaseModel):
    value: str


class SubscriptionStat(BaseModel):
    subscription_title: str
    subscription_id: str = ""
    videos_added: int = 0
    last_added_at: Optional[str] = None
    status: str = "inactive"


class PlaylistResponse(BaseModel):
    id: str
    title: str

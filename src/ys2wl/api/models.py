from pydantic import BaseModel, Field
from typing import Optional


class HealthResponse(BaseModel):
    status: str = "ok"


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


class RoutingRuleCreate(BaseModel):
    name: str
    priority: int = 0
    field: Optional[str] = None
    operator: str = "contains"
    pattern: Optional[str] = None
    destination_playlist_id: str
    destination_playlist_title: str = ""


class RoutingRuleUpdate(BaseModel):
    name: Optional[str] = None
    priority: Optional[int] = None
    field: Optional[str] = None
    operator: Optional[str] = None
    pattern: Optional[str] = None
    destination_playlist_id: Optional[str] = None
    destination_playlist_title: Optional[str] = None
    enabled: Optional[bool] = None


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


class TriggerResponse(BaseModel):
    run_id: int
    message: str = "Pipeline triggered"

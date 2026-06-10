import logging

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional

log = logging.getLogger("sortarr.config")


class Settings(BaseSettings):
    model_config = {
        "env_prefix": "SORTARR_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }

    pickle_file: str = Field(default="credentials.pickle")
    credentials_file: str = Field(default="client_secret.json")
    database_file: str = Field(default="sortarr.db")
    schedule: str = Field(default="0 */6 * * *")
    api_port: int = Field(default=8080)
    log_level: str = Field(default="warning")
    log_file: str = Field(default="stream")
    compare_distance: int = Field(default=80, ge=0, le=100)
    reprocess_days: int = Field(default=2, ge=0)
    playlist_sleep: int = Field(default=10, ge=0)
    subscription_sleep: int = Field(default=30, ge=0)
    pipeline_concurrency: int = Field(default=1, ge=1, le=10)
    activity_limit: int = Field(default=0, ge=0)
    subscription_limit: int = Field(default=0, ge=0)
    published_after: Optional[str] = Field(default=None)
    no_webbrowser: bool = Field(default=False)
    public_url: str = Field(default="http://localhost:8080")
    playlist_tracker_schedule: str = Field(default="0 3 * * *")


def load_settings() -> Settings:
    s = Settings()
    log.debug("config loaded: %s", s.model_dump())
    return s

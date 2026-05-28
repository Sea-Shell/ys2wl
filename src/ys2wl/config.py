from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    model_config = {"env_prefix": "YS2WL_", "env_file": ".env", "env_file_encoding": "utf-8"}

    pickle_file: str = Field(default="credentials.pickle")
    credentials_file: str = Field(default="client_secret.json")
    database_file: str = Field(default="ys2wl.db")
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
    minimum_length: str = Field(default="0s")
    maximum_length: str = Field(default="0s")
    published_after: Optional[str] = Field(default=None)
    subscription_ignore_file: str = Field(default=".subscription-ignore")
    video_ignore_file: str = Field(default=".video-ignore")
    words_ignore_file: str = Field(default=".ignore-words")
    no_webbrowser: bool = Field(default=False)


def load_settings() -> Settings:
    return Settings()

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator
import sqlite3
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
import os
from ys2wl.config import load_settings, Settings
from google.auth.transport.requests import Request
from ys2wl.core.youtube import YouTubeAPIClient
from ys2wl.core.auth import load_credentials
from ys2wl.core.scheduler import PipelineScheduler
from ys2wl.db.migrations import init_db
from ys2wl.db import repository as repo
from ys2wl.api.routes import (
    health,
    config,
    rules,
    pipeline as pipeline_routes,
    subscriptions,
    stats as stats_routes,
)
from ys2wl.api.routes import auth as auth_routes
from prometheus_client import make_asgi_app

log = logging.getLogger("ys2wl.api")


class AppState:
    def __init__(self):
        self.settings: Settings = load_settings()
        self.db_con: sqlite3.Connection = None
        self.credentials = None
        self.youtube: YouTubeAPIClient | None = None
        self.scheduler: PipelineScheduler | None = None
        self.device_flow: dict | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator:
    state = AppState()
    app.state.ys2wl = state

    init_db(state.settings.database_file)
    state.db_con = sqlite3.connect(state.settings.database_file)
    state.db_con.row_factory = sqlite3.Row

    state.credentials = load_credentials(state.db_con)
    if state.credentials:
        if state.credentials.valid:
            log.info("Loaded saved credentials")
            state.youtube = YouTubeAPIClient(credentials=state.credentials)
        elif state.credentials.expired and state.credentials.refresh_token:
            log.info("Credentials expired — attempting refresh")
            try:
                state.credentials.refresh(Request())
                log.info("Credentials refreshed successfully")
                state.youtube = YouTubeAPIClient(credentials=state.credentials)
            except Exception as e:
                log.warning("Credential refresh failed: %s", e)
        else:
            log.info("Credentials invalid and cannot be refreshed — use UI auth flow")
    else:
        log.info("No saved credentials found — use UI auth flow")

    # Overlay DB config onto settings (DB wins)
    for key in [
        "schedule",
        "compare_distance",
        "reprocess_days",
        "playlist_sleep",
        "subscription_sleep",
        "pipeline_concurrency",
        "activity_limit",
        "subscription_limit",
        "log_level",
        "minimum_length",
        "maximum_length",
        "published_after",
        "no_webbrowser",
        "client_secret_json",
    ]:
        db_val = repo.get_config(state.db_con, key)
        if db_val is not None and hasattr(state.settings, key):
            try:
                val = getattr(state.settings, key)
                if isinstance(val, bool):
                    setattr(state.settings, key, db_val.lower() in ("true", "1", "yes"))
                elif isinstance(val, int):
                    setattr(state.settings, key, int(db_val))
                else:
                    setattr(state.settings, key, db_val)
            except (ValueError, TypeError):
                log.warning("Failed to parse config %s = %s", key, db_val)
        elif db_val is None:
            env_val = getattr(state.settings, key)
            if env_val is not None:
                repo.set_config(state.db_con, key, str(env_val))

    app.state.ys2wl = state
    log.info("ys2wl service started")
    yield

    if state.scheduler:
        state.scheduler.stop()
    if state.youtube:
        state.youtube.close()
    if state.db_con:
        state.db_con.close()
    log.info("ys2wl service stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="ys2wl",
        description="YouTube Subscription to Watch Later Service",
        version="2.0.0",
        lifespan=lifespan,
    )

    app.include_router(health.router, prefix="/api", tags=["health"])
    app.include_router(config.router, prefix="/api", tags=["config"])
    app.include_router(rules.router, prefix="/api", tags=["rules"])
    app.include_router(pipeline_routes.router, prefix="/api", tags=["pipeline"])
    app.include_router(subscriptions.router, prefix="/api", tags=["subscriptions"])
    app.include_router(stats_routes.router, prefix="/api", tags=["stats"])
    app.include_router(auth_routes.router, prefix="/api", tags=["auth"])

    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    ui_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "ui",
    )
    os.makedirs(ui_dir, exist_ok=True)
    app.mount("/ui", StaticFiles(directory=ui_dir, html=True), name="ui")

    @app.get("/")
    async def root():
        return RedirectResponse(url="/ui/index.html")

    return app

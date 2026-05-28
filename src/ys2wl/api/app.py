import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator
import sqlite3
from fastapi import FastAPI
from ys2wl.config import load_settings, Settings
from ys2wl.core.youtube import YouTubeAPIClient, authenticate
from ys2wl.core.scheduler import PipelineScheduler
from ys2wl.db.migrations import init_db
from ys2wl.db import repository as repo
from ys2wl.api.routes import health, config, rules, pipeline as pipeline_routes, subscriptions
from prometheus_client import make_asgi_app

log = logging.getLogger("ys2wl.api")

SCOPES = [
    "https://www.googleapis.com/auth/youtubepartner",
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.readonly",
]


class AppState:
    def __init__(self):
        self.settings: Settings = load_settings()
        self.db_con: sqlite3.Connection = None
        self.youtube: YouTubeAPIClient = None
        self.scheduler: PipelineScheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator:
    state = AppState()
    app.state.ys2wl = state

    init_db(state.settings.database_file)
    state.db_con = sqlite3.connect(state.settings.database_file)
    state.db_con.row_factory = sqlite3.Row

    credentials = authenticate(
        state.settings.credentials_file,
        state.settings.pickle_file,
        SCOPES,
        no_webbrowser=state.settings.no_webbrowser,
    )
    state.youtube = YouTubeAPIClient(credentials=credentials)

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

    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    return app

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator
import sqlite3
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
import os
from ys2wl.config import load_settings, Settings
from ys2wl.core.youtube import YouTubeAPIClient
from ys2wl.core.auth import load_credentials, SCOPES
from ys2wl.core.scheduler import PipelineScheduler
from ys2wl.db.migrations import init_db
from ys2wl.db import repository as repo
from ys2wl.api.routes import health, config, rules, pipeline as pipeline_routes, subscriptions
from ys2wl.api.routes import auth as auth_routes
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

    state.credentials = load_credentials(state.settings.pickle_file)
    if state.credentials and state.credentials.valid:
        log.info("Loaded saved credentials")
        state.youtube = YouTubeAPIClient(credentials=state.credentials)
    else:
        log.info("No valid credentials found — use UI auth flow")

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
    app.include_router(auth_routes.router, prefix="/api", tags=["auth"])

    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    ui_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "ui")
    os.makedirs(ui_dir, exist_ok=True)
    app.mount("/ui", StaticFiles(directory=ui_dir, html=True), name="ui")

    @app.get("/")
    async def root():
        return RedirectResponse(url="/ui/index.html")

    return app

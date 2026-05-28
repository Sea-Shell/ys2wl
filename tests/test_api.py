import pytest
import sqlite3
from unittest.mock import MagicMock, patch
from httpx import ASGITransport, AsyncClient
from ys2wl.api.models import ConfigResponse, TriggerResponse


@pytest.fixture
def app():
    from ys2wl.api.app import create_app
    app = create_app()
    from ys2wl.api.app import AppState
    state = AppState()
    app.state.ys2wl = state
    state.db_con = sqlite3.connect(":memory:")
    state.db_con.row_factory = sqlite3.Row
    state.db_con.executescript("""
        CREATE TABLE IF NOT EXISTS videos (
            videoId TEXT NOT NULL PRIMARY KEY,
            timestamp TEXT,
            title TEXT,
            subscriptionId TEXT,
            playlistId TEXT,
            duration_seconds INTEGER,
            route_rule TEXT
        );
        CREATE TABLE IF NOT EXISTS channel (
            id TEXT NOT NULL PRIMARY KEY,
            title TEXT
        );
        CREATE TABLE IF NOT EXISTS playlist (
            id TEXT NOT NULL PRIMARY KEY,
            title TEXT
        );
        CREATE TABLE IF NOT EXISTS subscription (
            id TEXT NOT NULL PRIMARY KEY,
            title TEXT,
            timestamp TEXT
        );
        CREATE TABLE IF NOT EXISTS last_run (
            id NUMBER NOT NULL PRIMARY KEY,
            timestamp TEXT
        );
        CREATE TABLE IF NOT EXISTS routing_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            field TEXT,
            operator TEXT NOT NULL DEFAULT 'contains',
            pattern TEXT,
            destination_playlist_id TEXT NOT NULL,
            destination_playlist_title TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            subscriptions_processed INTEGER DEFAULT 0,
            subscriptions_skipped INTEGER DEFAULT 0,
            videos_added INTEGER DEFAULT 0,
            videos_skipped INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            error_message TEXT,
            trigger TEXT DEFAULT 'scheduled'
        );
        CREATE TABLE IF NOT EXISTS app_config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    return app


@pytest.mark.asyncio
async def test_health_endpoint(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_config_endpoint(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/config")
    assert resp.status_code == 200
    data = resp.json()
    assert "schedule" in data
    assert "compare_distance" in data


@pytest.mark.asyncio
async def test_rules_crud(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/rules")
        assert resp.status_code == 200
        assert resp.json() == []

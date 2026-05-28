import pytest
import sqlite3
import tempfile
import os
from typing import Iterator
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from ys2wl.config import Settings
from ys2wl.db.migrations import init_db


@pytest.fixture
def db_path() -> Iterator[str]:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    init_db(path)
    yield path
    os.unlink(path)


@pytest.fixture
def db_con(db_path: str) -> Iterator[sqlite3.Connection]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    yield con
    con.close()


@pytest.fixture
def app(db_path: str) -> FastAPI:
    """Create app with test DB and minimal settings."""
    from ys2wl.api.app import AppState
    from ys2wl.api.routes import health, config, rules

    state = AppState()
    s = Settings(database_file=db_path)
    state.settings = s
    init_db(s.database_file)
    state.db_con = sqlite3.connect(s.database_file)
    state.db_con.row_factory = sqlite3.Row

    app = FastAPI()
    app.state.ys2wl = state
    app.include_router(health.router, prefix="/api")
    app.include_router(config.router, prefix="/api")
    app.include_router(rules.router, prefix="/api")
    return app


@pytest.mark.asyncio
async def test_health_returns_ok(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_config_get_and_update(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/config")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data["schedule"], str)
        assert isinstance(data["compare_distance"], int)

        resp2 = await client.put("/api/config", json={"compare_distance": 7})
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2["compare_distance"] == 7


@pytest.mark.asyncio
async def test_rule_lifecycle(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/rules",
            json={
                "name": "Test Rule",
                "priority": 10,
                "field": "title",
                "operator": "contains",
                "pattern": "trailer",
                "destination_playlist_id": "PLtest123",
                "destination_playlist_title": "Test Playlist",
            },
        )
    assert resp.status_code == 201
    rule = resp.json()
    assert rule["name"] == "Test Rule"
    assert rule["enabled"] is True
    rid = rule["id"]

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp2 = await client.get("/api/rules")
    assert resp2.status_code == 200
    rules = resp2.json()
    assert any(r["id"] == rid for r in rules)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp3 = await client.delete(f"/api/rules/{rid}")
    assert resp3.status_code == 204


@pytest.mark.asyncio
async def test_rules_empty_list(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/rules")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_rule_not_found(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.put("/api/rules/99999", json={"name": "Ghost"})
    assert resp.status_code == 404

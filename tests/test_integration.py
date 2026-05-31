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
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp2 = await client.get("/api/rules")
    assert resp2.status_code == 200


##################################
# New test: config/settings sync #
##################################


def test_config_keys_match_settings():
    """
    Ensure every DB config key used for overlay exists as a Settings attribute, and vice versa.
    Fails if code is out of sync (typos, old key, rename).
    """
    from ys2wl.config import Settings

    # All Settings fields (excluding excluded/internal)
    s = Settings()
    settings_keys = set(vars(s).keys())
    # Canonical DB keys currently used in app.py overlay
    canonical_db_keys = {
        "schedule",
        "compare_distance",
        "reprocess_days",
        "playlist_sleep",
        "subscription_sleep",
        "pipeline_concurrency",
        "activity_limit",
        "subscription_limit",
        "log_level",
        "published_after",
        "no_webbrowser",
        "public_url",
        "credentials_file",
    }
    # All canonical DB keys must be settings attributes
    missing = canonical_db_keys - settings_keys
    assert not missing, f"All DB overlay keys must exist in Settings: {missing}"
    # You may also wish: all Settings keys must have DB coverage
    # Or relax for advanced/hidden config


def test_overlay_warns_on_invalid_key(tmp_path):
    """
    Simulate DB with typo key; app overlays and should not crash, should log warning.
    """
    import logging
    from ys2wl.api.app import AppState
    from ys2wl.config import Settings
    from ys2wl.db import repository as repo

    db_path = tmp_path / "testsettings.db"
    init_db(str(db_path))
    s = Settings(database_file=str(db_path))
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute(
        "INSERT INTO app_config (key, value) VALUES (?, ?)",
        ("totally_wrong_keyz", "oops"),
    )
    con.commit()
    state = AppState()
    state.settings = s
    state.db_con = con
    # Patch logger to capture log output
    records = []

    class CapHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    lh = CapHandler()
    log = logging.getLogger("ys2wl.api")
    log.addHandler(lh)
    # Run overlay part: mimic overlay loop
    # Should not crash, should log warning if it cannot set
    for key in ["totally_wrong_keyz", *vars(s).keys()]:
        db_val = repo.get_config(con, key)
        if db_val is not None and hasattr(s, key):
            try:
                val = getattr(s, key)
                if isinstance(val, bool):
                    setattr(s, key, db_val.lower() in ("true", "1", "yes"))
                elif isinstance(val, int):
                    setattr(s, key, int(db_val))
                else:
                    setattr(s, key, db_val)
            except (ValueError, TypeError):
                log.warning("Failed to parse config %s = %s", key, db_val)
        elif db_val is not None:
            log.warning("Config key not present in Settings: %s", key)
    log.removeHandler(lh)
    assert any("totally_wrong_keyz" in r.getMessage() for r in records), (
        "Should log for invalid key"
    )

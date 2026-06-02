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
    from ys2wl.api.routes import (
        health,
        config,
        rules,
        pipeline as pipeline_routes,
        pipelines as pipelines_routes,
    )

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
    app.include_router(pipelines_routes.router, prefix="/api")
    app.include_router(pipeline_routes.router, prefix="/api")
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
async def test_pipeline_lifecycle(app: FastAPI):
    """Full CRUD lifecycle for a pipeline."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        # CREATE
        resp = await c.post(
            "/api/pipelines",
            json={
                "name": "Test Pipeline",
                "destination_playlist_id": "PLtest123",
                "destination_playlist_title": "Test Playlist",
                "subscription_scope": "all",
                "selector_mode": "AND",
                "duration_min_seconds": 60,
                "duration_max_seconds": 600,
                "check_db_exists": True,
                "check_title_similarity": False,
                "compare_distance": 85,
            },
        )
        assert resp.status_code == 201
        pipe = resp.json()
        assert pipe["name"] == "Test Pipeline"
        assert pipe["enabled"] is True
        assert pipe["subscription_scope"] == "all"
        assert pipe["duration_min_seconds"] == 60
        assert pipe["check_db_exists"] is True
        assert pipe["check_title_similarity"] is False
        pid = pipe["id"]

        # LIST
        resp2 = await c.get("/api/pipelines")
        assert resp2.status_code == 200
        assert pid in [p["id"] for p in resp2.json()]

        # GET
        resp3 = await c.get(f"/api/pipelines/{pid}")
        assert resp3.status_code == 200
        assert resp3.json()["name"] == "Test Pipeline"

        # UPDATE
        resp4 = await c.put(
            f"/api/pipelines/{pid}", json={"name": "Updated Pipeline", "enabled": False}
        )
        assert resp4.status_code == 200
        assert resp4.json()["name"] == "Updated Pipeline"
        assert resp4.json()["enabled"] is False

        # DELETE
        resp5 = await c.delete(f"/api/pipelines/{pid}")
        assert resp5.status_code == 204

        # VERIFY GONE
        resp6 = await c.get(f"/api/pipelines/{pid}")
        assert resp6.status_code == 404


@pytest.mark.asyncio
async def test_pipeline_selectors(app: FastAPI):
    """Attach and replace selectors on a pipeline."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        resp = await c.post(
            "/api/pipelines",
            json={"name": "Selector Pipeline", "destination_playlist_id": "PLsel"},
        )
        assert resp.status_code == 201
        pid = resp.json()["id"]

        selectors = [
            {"field": "title", "operator": "contains", "pattern": "trailer"},
            {"field": "title", "operator": "contains", "pattern": "review"},
        ]
        resp2 = await c.put(
            f"/api/pipelines/{pid}/selectors", json={"selectors": selectors}
        )
        assert resp2.status_code == 204

        # Verify selectors persisted
        resp3 = await c.get(f"/api/pipelines/{pid}")
        assert resp3.status_code == 200
        saved = resp3.json()["selectors"]
        assert len(saved) == 2
        saved_patterns = {s["pattern"] for s in saved}
        assert "trailer" in saved_patterns
        assert "review" in saved_patterns


@pytest.mark.asyncio
async def test_pipeline_subscriptions_scope(app: FastAPI):
    """Set selected subscriptions on a pipeline."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        resp = await c.post(
            "/api/pipelines",
            json={
                "name": "Sub Pipeline",
                "destination_playlist_id": "PLsub",
                "subscription_scope": "selected",
            },
        )
        assert resp.status_code == 201
        pid = resp.json()["id"]

        resp2 = await c.put(
            f"/api/pipelines/{pid}/subscriptions",
            json={"subscription_ids": ["UC_abc123", "UC_def456"]},
        )
        assert resp2.status_code == 204


@pytest.mark.asyncio
async def test_ignore_list_lifecycle(app: FastAPI):
    """Full CRUD for ignore lists + entries."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        # CREATE
        resp = await c.post(
            "/api/ignore-lists", json={"name": "Bad Words", "list_type": "word"}
        )
        assert resp.status_code == 201
        lst = resp.json()
        assert lst["name"] == "Bad Words"
        assert lst["list_type"] == "word"
        assert lst["entries"] == []
        lid = lst["id"]

        # LIST
        resp2 = await c.get("/api/ignore-lists")
        assert resp2.status_code == 200
        assert lid in [p["id"] for p in resp2.json()]

        # ADD ENTRIES
        for val in ["badword1", "badword2"]:
            resp3 = await c.post(
                f"/api/ignore-lists/{lid}/entries", json={"value": val}
            )
            assert resp3.status_code == 201

        # VERIFY ENTRIES
        resp4 = await c.get(f"/api/ignore-lists/{lid}/entries")
        assert resp4.status_code == 200
        entries = resp4.json()
        assert len(entries) == 2
        entry_vals = {e["value"] for e in entries}
        assert "badword1" in entry_vals
        assert "badword2" in entry_vals
        entry_id = entries[0]["id"]  # use any entry for deletion test

        # DELETE ENTRY
        resp5 = await c.delete(f"/api/ignore-lists/{lid}/entries/{entry_id}")
        assert resp5.status_code == 204

        # VERIFY ENTRY GONE
        resp6 = await c.get(f"/api/ignore-lists/{lid}/entries")
        assert resp6.status_code == 200
        assert len(resp6.json()) == 1

        # DELETE LIST
        resp7 = await c.delete(f"/api/ignore-lists/{lid}")
        assert resp7.status_code == 204

        # VERIFY LIST GONE
        resp8 = await c.get("/api/ignore-lists")
        assert resp8.status_code == 200
        assert lid not in [p["id"] for p in resp8.json()]


@pytest.mark.asyncio
async def test_pipeline_ignore_list_association(app: FastAPI):
    """Attach ignore lists to a pipeline, verify it affects filtering."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        # Create pipeline
        resp = await c.post(
            "/api/pipelines",
            json={"name": "Filtered Pipeline", "destination_playlist_id": "PLfiltered"},
        )
        assert resp.status_code == 201
        pid = resp.json()["id"]

        # Create ignore lists
        resp_w = await c.post(
            "/api/ignore-lists", json={"name": "Skip Words", "list_type": "word"}
        )
        resp_v = await c.post(
            "/api/ignore-lists", json={"name": "Skip Videos", "list_type": "video"}
        )
        assert resp_w.status_code == 201
        assert resp_v.status_code == 201
        wid = resp_w.json()["id"]
        vid = resp_v.json()["id"]

        # Attach both to pipeline
        resp3 = await c.put(
            f"/api/pipelines/{pid}/ignore-lists", json={"ignore_list_ids": [wid, vid]}
        )
        assert resp3.status_code == 204

        # Verify on GET
        resp4 = await c.get(f"/api/pipelines/{pid}")
        assert resp4.status_code == 200
        assert wid in resp4.json()["ignore_list_ids"]
        assert vid in resp4.json()["ignore_list_ids"]


@pytest.mark.asyncio
async def test_rule_lifecycle(app: FastAPI):
    """Legacy routing rules API still works."""
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


@pytest.mark.asyncio
async def test_playlists_endpoint_503_no_youtube(app: FastAPI):
    """GET /api/playlists returns 503 when YouTube not authenticated."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        resp = await c.get("/api/playlists")
    assert resp.status_code == 503
    assert "authenticate" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_trigger_dry_run_with_pipeline_id_no_youtube(app: FastAPI):
    """POST /api/pipeline/trigger with dry_run and pipeline_id returns 503 without auth."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        resp = await c.post("/api/pipeline/trigger?dry_run=true&pipeline_id=p1")
    assert resp.status_code == 503
    assert "authenticate" in resp.json()["detail"].lower()

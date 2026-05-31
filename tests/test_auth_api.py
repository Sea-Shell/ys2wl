import sqlite3

import pytest
from httpx import ASGITransport, AsyncClient


def _db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        "CREATE TABLE IF NOT EXISTS app_config (key TEXT PRIMARY KEY, value TEXT)"
    )
    return con


@pytest.mark.asyncio
async def test_auth_status_returns_not_authenticated():
    from ys2wl.api.app import create_app, AppState

    app = create_app()
    state = AppState()
    state.db_con = _db()
    app.state.ys2wl = state
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/auth/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "authenticated" in data


@pytest.mark.asyncio
async def test_auth_login_returns_400_without_creds():
    from ys2wl.api.app import create_app, AppState

    app = create_app()
    state = AppState()
    state.db_con = _db()
    app.state.ys2wl = state
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/auth/login")
    assert resp.status_code == 400

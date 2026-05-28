import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_auth_status_returns_not_authenticated():
    from ys2wl.api.app import create_app, AppState

    app = create_app()
    state = AppState()
    app.state.ys2wl = state
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/auth/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "authenticated" in data


@pytest.mark.asyncio
async def test_auth_device_returns_400_without_creds():
    from ys2wl.api.app import create_app, AppState

    app = create_app()
    state = AppState()
    app.state.ys2wl = state
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/auth/device")
    assert resp.status_code == 400

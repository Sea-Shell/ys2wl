import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_ui_root_redirects():
    from ys2wl.api.app import create_app

    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers.get("location") == "/ui/index.html"


@pytest.mark.asyncio
async def test_index_html_returns_html():
    from ys2wl.api.app import create_app

    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/ui/index.html")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "ys2wl" in resp.text

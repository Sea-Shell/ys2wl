import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def client():
    from ys2wl.api.app import create_app
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_ui_root_redirects(client):
    resp = await client.get("/", follow_redirects=False)
    assert resp.status_code == 307
    assert resp.headers.get("location") == "/ui/index.html"


@pytest.mark.asyncio
async def test_index_html_returns_html(client):
    resp = await client.get("/ui/index.html")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "ys2wl" in resp.text


@pytest.mark.asyncio
async def test_index_has_sidebar(client):
    resp = await client.get("/ui/index.html")
    assert "sidebar" in resp.text.lower() or "nav" in resp.text.lower()


@pytest.mark.asyncio
async def test_index_has_dashboard_section(client):
    resp = await client.get("/ui/index.html")
    assert 'id="dashboard"' in resp.text


@pytest.mark.asyncio
async def test_index_has_dark_theme(client):
    resp = await client.get("/ui/index.html")
    assert "--bg" in resp.text or "#0f172a" in resp.text


@pytest.mark.asyncio
async def test_index_has_five_nav_links(client):
    resp = await client.get("/ui/index.html")
    for page in ["dashboard", "subscriptions", "rules", "config", "runs"]:
        assert f'data-page="{page}"' in resp.text


@pytest.mark.asyncio
async def test_index_has_api_wrapper(client):
    resp = await client.get("/ui/index.html")
    assert "async function api" in resp.text


@pytest.mark.asyncio
async def test_dashboard_has_stats_cards(client):
    resp = await client.get("/ui/index.html")
    assert "statSubs" in resp.text or "Subscriptions" in resp.text
    assert "statRules" in resp.text or "Routing Rules" in resp.text


@pytest.mark.asyncio
async def test_dashboard_has_trigger_button(client):
    resp = await client.get("/ui/index.html")
    assert "Run Pipeline" in resp.text or "triggerBtn" in resp.text


@pytest.mark.asyncio
async def test_subscriptions_section_has_search(client):
    resp = await client.get("/ui/index.html")
    assert "subSearch" in resp.text or "Filter channels" in resp.text

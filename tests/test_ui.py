import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def client():
    from sortarr.api.app import create_app

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
    assert "sortarr" in resp.text


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
    assert "--canvas" in resp.text or "#0c0c0e" in resp.text


@pytest.mark.asyncio
async def test_index_has_five_nav_links(client):
    resp = await client.get("/ui/index.html")
    for page in ["dashboard", "subscriptions", "pipelines", "config", "runs"]:
        assert f'data-page="{page}"' in resp.text


@pytest.mark.asyncio
async def test_index_has_api_wrapper(client):
    resp = await client.get("/ui/index.html")
    assert "async function api" in resp.text


@pytest.mark.asyncio
async def test_dashboard_has_stats_cards(client):
    resp = await client.get("/ui/index.html")
    assert "statSubs" in resp.text or "Subscriptions" in resp.text
    assert "statPipelines" in resp.text or "Pipelines" in resp.text


@pytest.mark.asyncio
async def test_dashboard_has_trigger_button(client):
    resp = await client.get("/ui/index.html")
    assert "Run Pipeline" in resp.text or "triggerBtn" in resp.text


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_rules_section_has_table(client):
    resp = await client.get("/ui/index.html")
    assert "pipelinesList" in resp.text or "Add Pipeline" in resp.text


@pytest.mark.asyncio
async def test_rules_section_has_modal(client):
    resp = await client.get("/ui/index.html")
    assert "pipelineModal" in resp.text


@pytest.mark.asyncio
async def test_subscriptions_section_has_search(client):
    resp = await client.get("/ui/index.html")
    assert "subSearch" in resp.text or "Filter channels" in resp.text


@pytest.mark.asyncio
async def test_config_form_exists(client):
    resp = await client.get("/ui/index.html")
    assert "configForm" in resp.text or "Config" in resp.text


@pytest.mark.asyncio
async def test_runs_section_exists(client):
    resp = await client.get("/ui/index.html")
    assert "runsList" in resp.text or "Pipeline Runs" in resp.text

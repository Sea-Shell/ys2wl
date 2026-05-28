"""Integration tests for google-api-python-client with HttpMock.

Tests the real googleapiclient.discovery.build() factory and HttpError
without network access, using a cached discovery document fixture.
"""

import json
import os
import pytest
import httplib2
from googleapiclient.discovery import build
from googleapiclient.http import HttpMock
from googleapiclient.errors import HttpError

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
DISCOVERY_PATH = os.path.join(FIXTURE_DIR, "youtube_v3_discovery.json")


def test_build_youtube_v3():
    """build('youtube', 'v3') works with real discovery doc."""
    assert os.path.exists(DISCOVERY_PATH), f"Missing fixture: {DISCOVERY_PATH}"
    http = HttpMock(DISCOVERY_PATH, {"status": "200"})
    service = build("youtube", "v3", http=http, developerKey="test_key")
    assert service is not None
    assert hasattr(service, "channels")
    assert hasattr(service, "activities")
    assert hasattr(service, "subscriptions")
    assert hasattr(service, "videos")
    assert hasattr(service, "playlists")
    assert hasattr(service, "playlistItems")


def test_build_youtube_v3_channels_list():
    """Channels.list() request object is constructable."""
    http = HttpMock(DISCOVERY_PATH, {"status": "200"})
    service = build("youtube", "v3", http=http, developerKey="test_key")
    request = service.channels().list(part="snippet", mine=True)
    assert request.method == "GET"
    assert "/youtube/v3/channels" in request.uri
    assert "part=snippet" in request.uri
    assert "mine=true" in request.uri


def test_build_youtube_v3_subscriptions_list():
    """Subscriptions.list() request object is constructable."""
    http = HttpMock(DISCOVERY_PATH, {"status": "200"})
    service = build("youtube", "v3", http=http, developerKey="test_key")
    request = service.subscriptions().list(
        part="snippet", maxResults=50, mine=True, order="alphabetical"
    )
    assert request.method == "GET"
    assert "/youtube/v3/subscriptions" in request.uri
    assert "maxResults=50" in request.uri


def test_http_error_403():
    """HttpError with 403 has correct status and message."""
    resp = httplib2.Response({"status": 403})
    body = json.dumps({"error": {"message": "Access forbidden", "code": 403}}).encode()
    err = HttpError(
        resp, body, uri="https://youtube.googleapis.com/youtube/v3/channels"
    )
    assert err.resp.status == 403
    assert "Access forbidden" in str(err)


def test_http_error_404():
    """HttpError with 404 is constructable."""
    resp = httplib2.Response({"status": 404})
    body = json.dumps({"error": {"message": "Not found"}}).encode()
    err = HttpError(resp, body, uri="https://youtube.googleapis.com/youtube/v3/videos")
    assert err.resp.status == 404


def test_http_error_429():
    """HttpError with 429 (rate limit) is constructable."""
    resp = httplib2.Response({"status": 429})
    body = json.dumps(
        {"error": {"message": "Rate limit exceeded", "code": 429}}
    ).encode()
    err = HttpError(resp, body, uri="https://youtube.googleapis.com/youtube/v3/search")
    assert err.resp.status == 429


@pytest.mark.skipif(
    not os.path.exists(DISCOVERY_PATH),
    reason="requires youtube_v3_discovery.json fixture",
)
def test_execute_with_mock():
    """_execute_with_retry works with a mocked request."""
    from unittest.mock import MagicMock
    from ys2wl.core.youtube import YouTubeAPIClient
    from google.oauth2.credentials import Credentials as OAuth2Credentials

    creds = OAuth2Credentials(
        token="test",
        token_uri="https://oauth2.googleapis.com/token",
        client_id="id",
        client_secret="secret",
    )
    client = YouTubeAPIClient(credentials=creds, use_local=True)

    mock_request = MagicMock()
    mock_request.execute.return_value = {"items": [{"id": "test"}]}
    result = client._execute_with_retry(mock_request)
    assert result == {"items": [{"id": "test"}]}
    mock_request.execute.assert_called_once()

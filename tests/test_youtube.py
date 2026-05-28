import pytest
from unittest.mock import MagicMock
from ys2wl.core.youtube import YouTubeAPIClient
from ys2wl.models.youtube import Channel, Playlist, Subscription, Activity


@pytest.fixture
def mock_credentials():
    creds = MagicMock()
    creds.valid = True
    creds.expired = False
    return creds


@pytest.fixture
def client(mock_credentials):
    client = YouTubeAPIClient(
        credentials=mock_credentials, use_local=True, debug_dir="debug"
    )
    yield client
    client.close()


def test_local_subscriptions(client):
    subs = client.get_subscriptions()
    assert len(subs) > 0
    assert all(isinstance(s, Subscription) for s in subs)
    assert all(s.id for s in subs)


def test_local_channel(client):
    channels = client.get_channel_id()
    assert len(channels) > 0
    assert all(isinstance(c, Channel) for c in channels)


def test_local_subscription_activity(client):
    activities = client.get_subscription_activity("UC1", published_after="2024-01-01")
    assert all(isinstance(a, Activity) for a in activities)
    assert len(activities) > 0


def test_local_video_duration(client):
    duration = client.get_video_duration("test_video_id")
    assert isinstance(duration, int)
    assert duration > 0


def test_local_user_playlists(client):
    playlists = client.get_user_playlists("UC_test")
    assert len(playlists) > 0
    assert all(isinstance(p, Playlist) for p in playlists)


def test_local_get_playlist(client):
    items = client.get_playlist("PL_test")
    assert len(items) > 0


def test_api_call_tracking(client):
    assert client.api_calls[0] == 0

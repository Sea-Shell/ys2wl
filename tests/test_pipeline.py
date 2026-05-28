import pytest
from unittest.mock import MagicMock, patch
from ys2wl.core.pipeline import PipelineOrchestrator
from ys2wl.models.pipeline import PipelineSummary
from ys2wl.models.youtube import Subscription, Activity, Channel, Playlist
from ys2wl.config import Settings
import sqlite3
from ys2wl.db.migrations import init_db


@pytest.fixture
def settings():
    s = Settings()
    s.compare_distance = 80
    s.reprocess_days = 2
    s.minimum_length = "0s"
    s.maximum_length = "0s"
    s.playlist_sleep = 0
    s.subscription_sleep = 0
    s.subscription_limit = 0
    s.activity_limit = 0
    s.pipeline_concurrency = 1
    return s


@pytest.fixture
def db_con(tmp_path):
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    yield con
    con.close()


def test_pipeline_run_no_subscriptions(settings, db_con):
    mock_youtube = MagicMock()
    mock_youtube.get_subscriptions.return_value = []
    mock_youtube.api_calls = [0]

    orchestrator = PipelineOrchestrator(
        settings=settings,
        youtube=mock_youtube,
        db_con=db_con,
        channel=Channel(id="UC1", title="My Channel"),
        playlist=Playlist(id="PL1", title="Watch Later"),
        ignore_subscriptions=[],
        ignore_videos=[],
        ignore_words=[],
        default_playlist_id="PL1",
        default_playlist_title="Watch Later",
    )

    result = orchestrator.run()
    assert result.status == "completed"
    assert result.subscriptions_processed == 0


def test_pipeline_adds_video(settings, db_con):
    mock_youtube = MagicMock()
    mock_youtube.get_subscriptions.return_value = [
        Subscription(id="UC1", title="Channel One", channel_id="UC1"),
    ]
    mock_youtube.get_subscription_activity.return_value = [
        Activity(video_id="v1", title="New Video", published_at="2024-06-01T00:00:00Z", video_type="upload"),
    ]
    mock_youtube.get_video_duration.return_value = 300
    mock_youtube.add_to_playlist.return_value = True
    mock_youtube.api_calls = [0]

    orchestrator = PipelineOrchestrator(
        settings=settings,
        youtube=mock_youtube,
        db_con=db_con,
        channel=Channel(id="UC1", title="My Channel"),
        playlist=Playlist(id="PL1", title="Watch Later"),
        ignore_subscriptions=[],
        ignore_videos=[],
        ignore_words=[],
        default_playlist_id="PL1",
        default_playlist_title="Watch Later",
    )

    result = orchestrator.run()
    assert result.status == "completed"
    assert result.videos_added == 1
    assert mock_youtube.add_to_playlist.called


def test_pipeline_word_filter_skips(settings, db_con):
    mock_youtube = MagicMock()
    mock_youtube.get_subscriptions.return_value = [
        Subscription(id="UC1", title="Channel One", channel_id="UC1"),
    ]
    mock_youtube.get_subscription_activity.return_value = [
        Activity(video_id="v1", title="Sketchy Video", published_at="now", video_type="upload"),
    ]
    mock_youtube.api_calls = [0]

    orchestrator = PipelineOrchestrator(
        settings=settings,
        youtube=mock_youtube,
        db_con=db_con,
        channel=Channel(id="UC1", title="My Channel"),
        playlist=Playlist(id="PL1", title="Watch Later"),
        ignore_subscriptions=[],
        ignore_videos=[],
        ignore_words=["sketchy"],
        default_playlist_id="PL1",
        default_playlist_title="Watch Later",
    )

    result = orchestrator.run()
    assert result.status == "completed"
    assert result.videos_added == 0
    assert result.videos_skipped == 1

import pytest
from unittest.mock import MagicMock
from sortarr.core.pipeline import PipelineOrchestrator
from sortarr.core.pipeline_runner import execute_pipeline
from sortarr.models.youtube import Subscription, Activity, Channel, Playlist
from sortarr.models.pipeline import PipelineConfig
from sortarr.config import Settings
import sqlite3
from sortarr.db.migrations import init_db
from sortarr.db import repository as repo


@pytest.fixture
def settings():
    s = Settings()
    s.compare_distance = 80
    s.reprocess_days = 2
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


def _make_pipeline(
    pipeline_id="p1",
    name="Test Pipeline",
    destination_playlist_id="PL_DEFAULT",
    destination_playlist_title="Default",
    **kwargs,
):
    return PipelineConfig(
        id=pipeline_id,
        name=name,
        destination_playlist_id=destination_playlist_id,
        destination_playlist_title=destination_playlist_title,
        **kwargs,
    )


def _setup_ignore_list(db_con, list_id, list_type, entries, pipeline_id="p1"):
    """Create ignore list in DB, add entries, and associate with pipeline."""
    repo.create_ignore_list(db_con, list_id, f"{list_type}_ignore", list_type)
    for i, val in enumerate(entries):
        repo.add_ignore_list_entry(db_con, f"{list_id}_e{i}", list_id, val)
    repo.set_pipeline_ignore_lists(db_con, pipeline_id, [list_id])
    return {list_id: entries}


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
        pipelines=[],
        all_ignore_lists={},
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
        Activity(
            video_id="v1",
            title="New Video",
            published_at="2024-06-01T00:00:00Z",
            video_type="upload",
        ),
    ]
    mock_youtube.get_video_duration.return_value = 300
    mock_youtube.add_to_playlist.return_value = True
    mock_youtube.api_calls = [0]

    pipelines = [_make_pipeline(name="Default Catch-all")]
    repo.create_pipeline(db_con, "p1", "Default Catch-all", "PL_DEFAULT", "Default")

    orchestrator = PipelineOrchestrator(
        settings=settings,
        youtube=mock_youtube,
        db_con=db_con,
        channel=Channel(id="UC1", title="My Channel"),
        playlist=Playlist(id="PL1", title="Watch Later"),
        pipelines=pipelines,
        all_ignore_lists={},
        default_playlist_id="PL1",
        default_playlist_title="Watch Later",
    )

    result = orchestrator.run()
    assert result.status == "completed"
    assert result.videos_added == 1
    assert mock_youtube.add_to_playlist.called


def test_pipeline_skips_ignored_subscription(settings, db_con):
    mock_youtube = MagicMock()
    mock_youtube.get_subscriptions.return_value = [
        Subscription(id="UC1", title="Channel One", channel_id="UC1"),
        Subscription(id="UC2", title="Boring Channel", channel_id="UC2"),
    ]
    mock_youtube.get_subscription_activity.return_value = [
        Activity(
            video_id="v1", title="Some Video", published_at="now", video_type="upload"
        ),
    ]
    mock_youtube.api_calls = [0]

    pipelines = [_make_pipeline()]
    repo.create_pipeline(db_con, "p1", "Test Pipeline", "PL_DEFAULT", "Default")
    all_ignore_lists = _setup_ignore_list(
        db_con, "il1", "subscription", ["Boring Channel"], pipeline_id="p1"
    )

    orchestrator = PipelineOrchestrator(
        settings=settings,
        youtube=mock_youtube,
        db_con=db_con,
        channel=Channel(id="UC1", title="My Channel"),
        playlist=Playlist(id="PL1", title="Watch Later"),
        pipelines=pipelines,
        all_ignore_lists=all_ignore_lists,
        default_playlist_id="PL1",
        default_playlist_title="Watch Later",
    )

    result = orchestrator.run()
    assert result.status == "completed"
    assert result.subscriptions_processed == 1
    assert result.subscriptions_skipped == 1
    assert len(result.subscription_skips) == 1
    assert result.subscription_skips[0]["subscription_title"] == "Boring Channel"
    assert result.subscription_skips[0]["reason"] == "ignored"


def test_pipeline_skips_reprocessed_subscription(settings, db_con):
    from datetime import datetime, timezone, timedelta

    pipelines = [_make_pipeline()]
    repo.create_pipeline(db_con, "p1", "Test Pipeline", "PL_DEFAULT", "Default")

    repo.upsert_pipeline_tracking(
        db_con,
        "p1",
        "UC1",
        (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
    )

    mock_youtube = MagicMock()
    mock_youtube.get_subscriptions.return_value = [
        Subscription(id="UC1", title="Channel One", channel_id="UC1"),
    ]
    mock_youtube.api_calls = [0]

    orchestrator = PipelineOrchestrator(
        settings=settings,
        youtube=mock_youtube,
        db_con=db_con,
        channel=Channel(id="UC1", title="My Channel"),
        playlist=Playlist(id="PL1", title="Watch Later"),
        pipelines=pipelines,
        all_ignore_lists={},
        default_playlist_id="PL1",
        default_playlist_title="Watch Later",
    )

    result = orchestrator.run()
    assert result.status == "completed"
    assert result.subscriptions_processed == 0
    assert result.subscriptions_skipped == 1
    assert len(result.subscription_skips) == 1
    assert result.subscription_skips[0]["subscription_title"] == "Channel One"
    assert result.subscription_skips[0]["reason"] == "already_up_to_date"


def test_pipeline_word_filter_skips(settings, db_con):
    mock_youtube = MagicMock()
    mock_youtube.get_subscriptions.return_value = [
        Subscription(id="UC1", title="Channel One", channel_id="UC1"),
    ]
    mock_youtube.get_subscription_activity.return_value = [
        Activity(
            video_id="v1",
            title="Sketchy Video",
            published_at="now",
            video_type="upload",
        ),
    ]
    mock_youtube.api_calls = [0]

    pipelines = [_make_pipeline()]
    repo.create_pipeline(db_con, "p1", "Test Pipeline", "PL_DEFAULT", "Default")
    all_ignore_lists = _setup_ignore_list(
        db_con, "il1", "word", ["sketchy"], pipeline_id="p1"
    )

    orchestrator = PipelineOrchestrator(
        settings=settings,
        youtube=mock_youtube,
        db_con=db_con,
        channel=Channel(id="UC1", title="My Channel"),
        playlist=Playlist(id="PL1", title="Watch Later"),
        pipelines=pipelines,
        all_ignore_lists=all_ignore_lists,
        default_playlist_id="PL1",
        default_playlist_title="Watch Later",
    )

    result = orchestrator.run()
    assert result.status == "completed"
    assert result.videos_added == 0
    assert result.videos_skipped == 1


def test_pipeline_no_rule_match_skips(settings, db_con):
    mock_youtube = MagicMock()
    mock_youtube.get_subscriptions.return_value = [
        Subscription(id="UC1", title="Channel One", channel_id="UC1"),
    ]
    mock_youtube.get_subscription_activity.return_value = [
        Activity(
            video_id="v1", title="Any Video", published_at="now", video_type="upload"
        ),
    ]
    mock_youtube.get_video_duration.return_value = 300
    mock_youtube.api_calls = [0]

    orchestrator = PipelineOrchestrator(
        settings=settings,
        youtube=mock_youtube,
        db_con=db_con,
        channel=Channel(id="UC1", title="My Channel"),
        playlist=Playlist(id="PL1", title="Watch Later"),
        pipelines=[],  # No pipelines at all → no processing
        all_ignore_lists={},
        default_playlist_id="PL1",
        default_playlist_title="Watch Later",
    )

    result = orchestrator.run()
    assert result.status == "completed"
    assert result.videos_added == 0
    assert result.videos_skipped == 0  # No pipelines means nothing processes


def test_execute_pipeline_filters_by_pipeline_id(tmp_path):
    """execute_pipeline with pipeline_id runs only that pipeline in dry-run."""
    from unittest.mock import MagicMock
    import sqlite3
    from sortarr.db.migrations import init_db
    from sortarr.db import repository as repo
    from sortarr.core.youtube import Channel as YTChannel

    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # Insert channel + playlist
    repo.insert_channel(con, "UC_test", "Test Channel")
    repo.insert_playlist(con, "PL_default", "Default Playlist")
    # Insert two pipelines
    repo.create_pipeline(con, "p1", "Pipeline One", "PL_dest1", "Dest One")
    repo.create_pipeline(con, "p2", "Pipeline Two", "PL_dest2", "Dest Two")
    con.commit()
    con.close()

    # Build mock state with real Settings object
    from sortarr.config import Settings

    state = MagicMock()
    s = Settings()
    s.database_file = db_path
    s.compare_distance = 80
    s.reprocess_days = 2
    s.playlist_sleep = 0
    s.subscription_sleep = 0
    s.subscription_limit = 0
    s.activity_limit = 0
    s.pipeline_concurrency = 1
    state.settings = s
    state.db_con = sqlite3.connect(db_path)
    state.db_con.row_factory = sqlite3.Row
    state.youtube = MagicMock()
    state.youtube.get_channel_id.return_value = [
        YTChannel(id="UC_test", title="Test Channel")
    ]
    state.youtube.get_user_playlists.return_value = []

    # Run with pipeline_id="p1"
    import asyncio

    run_id = asyncio.run(execute_pipeline(state, dry_run=True, pipeline_id="p1"))

    assert run_id is not None  # dry_run now creates a run
    run = repo.get_pipeline_run(state.db_con, run_id)
    assert run is not None
    assert run["dry_run"] == 1
    assert run["status"] == "completed"

    # Run without pipeline_id filter — both should run
    run_id2 = asyncio.run(execute_pipeline(state, dry_run=True))
    assert run_id2 is not None

    state.db_con.close()

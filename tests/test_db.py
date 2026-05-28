import pytest
import sqlite3
from ys2wl.db.migrations import init_db
from ys2wl.db.repository import (
    video_exists, insert_video, get_all_video_titles,
    insert_channel, get_channel,
    insert_playlist, get_playlist,
    insert_subscription, get_subscription_timestamp,
    get_last_run, set_last_run,
    get_routing_rules, create_routing_rule, update_routing_rule, delete_routing_rule,
    create_pipeline_run, finish_pipeline_run, get_pipeline_runs, get_pipeline_run,
    get_config, set_config,
)


def test_init_db_creates_tables(tmp_path):
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    con = sqlite3.connect(db_path)
    cursor = con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    con.close()
    assert "videos" in tables
    assert "channel" in tables
    assert "playlist" in tables
    assert "subscription" in tables
    assert "last_run" in tables
    assert "routing_rules" in tables
    assert "pipeline_runs" in tables
    assert "app_config" in tables


@pytest.fixture
def db_con(tmp_path):
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    yield con
    con.close()


def test_video_crud(db_con):
    assert not video_exists(db_con, "v1")
    assert insert_video(db_con, "v1", "now", "Test", "sub1")
    assert video_exists(db_con, "v1")
    titles = get_all_video_titles(db_con)
    assert len(titles) == 1
    assert titles[0][1] == "Test"


def test_channel_crud(db_con):
    assert get_channel(db_con) is None
    assert insert_channel(db_con, "UC1", "My Channel")
    ch = get_channel(db_con)
    assert ch["id"] == "UC1"


def test_playlist_crud(db_con):
    assert get_playlist(db_con) is None
    assert insert_playlist(db_con, "PL1", "My Playlist")
    pl = get_playlist(db_con)
    assert pl["id"] == "PL1"


def test_subscription_crud(db_con):
    assert get_subscription_timestamp(db_con, "s1") is None
    assert insert_subscription(db_con, "s1", "Sub", "2024-01-01")
    assert get_subscription_timestamp(db_con, "s1") == "2024-01-01"


def test_last_run(db_con):
    assert get_last_run(db_con) is None
    assert set_last_run(db_con, "now")
    assert get_last_run(db_con) == "now"


def test_routing_rules(db_con):
    rules = get_routing_rules(db_con)
    assert len(rules) == 0
    rid = create_routing_rule(db_con, "Test", 10, "channel_title", "contains", "music", "PL1", "Music")
    assert rid is not None
    rules = get_routing_rules(db_con)
    assert len(rules) == 1
    assert rules[0]["name"] == "Test"
    assert update_routing_rule(db_con, rid, name="Updated")
    assert delete_routing_rule(db_con, rid)
    assert len(get_routing_rules(db_con)) == 0


def test_pipeline_runs(db_con):
    rid = create_pipeline_run(db_con, trigger="manual")
    assert rid is not None
    summary = {"status": "completed", "videos_added": 5, "subscriptions_processed": 10,
               "subscriptions_skipped": 2, "videos_skipped": 3, "errors": 0, "error_message": ""}
    assert finish_pipeline_run(db_con, rid, summary)
    runs = get_pipeline_runs(db_con)
    assert len(runs) == 1
    assert runs[0]["status"] == "completed"
    assert get_pipeline_run(db_con, rid)["videos_added"] == 5


def test_app_config(db_con):
    assert get_config(db_con, "test_key") is None
    assert set_config(db_con, "test_key", "test_value")
    assert get_config(db_con, "test_key") == "test_value"
    assert set_config(db_con, "test_key", "updated")
    assert get_config(db_con, "test_key") == "updated"

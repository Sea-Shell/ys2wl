from unittest.mock import Mock, MagicMock
from sortarr.core.playlist_tracker import PlaylistTracker


def test_exclude_pipeline_playlists():
    youtube = Mock()
    youtube.get_user_playlists.return_value = [
        type("Playlist", (), {"id": "PL1", "title": "Pipeline Dest"}),
        type("Playlist", (), {"id": "PL2", "title": "Custom List"}),
    ]
    db = Mock()
    db.execute.return_value.fetchall.return_value = [{"destination_playlist_id": "PL1"}]

    tracker = PlaylistTracker(youtube, db, channel_id="UC123")
    playlists = tracker._get_playlists()
    assert len(playlists) == 1
    assert playlists[0]["id"] == "PL2"


def test_process_video_new_match():
    youtube = Mock()
    db = Mock()
    db.execute.side_effect = [
        Mock(fetchone=Mock(return_value=None)),
        Mock(fetchone=Mock(return_value={"subscriptionId": "UC123"})),
        None,
        None,
        None,
    ]
    tracker = PlaylistTracker(youtube, db, channel_id="UC123")
    result = tracker._process_video("vid1", "PL99")
    assert result is True


def test_process_video_already_counted():
    youtube = Mock()
    db = Mock()
    db.execute.return_value.fetchone.return_value = {"counted": 1}
    tracker = PlaylistTracker(youtube, db, channel_id="UC123")
    result = tracker._process_video("vid1", "PL99")
    assert result is False


def test_process_video_not_in_videos_table():
    youtube = Mock()
    db = Mock()
    db.execute.side_effect = [
        Mock(fetchone=Mock(return_value=None)),
        Mock(fetchone=Mock(return_value=None)),
        None,
    ]
    tracker = PlaylistTracker(youtube, db, channel_id="UC123")
    result = tracker._process_video("vid1", "PL99")
    assert result is False


def test_run():
    youtube = Mock()
    youtube.get_user_playlists.return_value = [
        type("Playlist", (), {"id": "PL1", "title": "Custom List"}),
    ]
    youtube.get_playlist.return_value = [
        {
            "snippet": {
                "resourceId": {"videoId": "vid1"},
                "videoOwnerChannelId": "UC123",
                "title": "Video 1",
                "publishedAt": "2024-01-01T00:00:00Z",
            }
        },
    ]
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = []
    db.execute.return_value.fetchone.side_effect = [
        None,
        {"subscriptionId": "sub123"},
    ]

    tracker = PlaylistTracker(youtube, db, channel_id="UC123")
    result = tracker.run()

    assert result == {
        "playlists_processed": 1,
        "videos_found": 1,
        "videos_newly_counted": 1,
        "subscriptions_updated": 1,
    }


def test_upgrade_from_counted_0_to_1():
    youtube = Mock()
    db = MagicMock()

    db.execute.return_value.fetchall.return_value = []
    db.execute.return_value.fetchone.side_effect = [
        None,
        None,
    ]

    tracker = PlaylistTracker(youtube, db, channel_id="UC123")
    result1 = tracker._process_video("vid1", "PL99")
    assert result1 is False

    db.execute.return_value.fetchone.side_effect = [
        {"counted": 0},
        {"subscriptionId": "sub123"},
    ]

    result2 = tracker._process_video("vid1", "PL99")
    assert result2 is True

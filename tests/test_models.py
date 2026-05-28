from ys2wl.models.youtube import Channel, Playlist, Subscription, Activity, Video, RoutingRule
from ys2wl.models.pipeline import FilterResult, RouteResult, VideoResult, PipelineSummary


def test_channel():
    c = Channel(id="UC123", title="Test Channel")
    assert c.id == "UC123"
    assert c.title == "Test Channel"


def test_subscription():
    s = Subscription(id="sub1", title="Sub Name", channel_id="UC123")
    assert s.channel_id == "UC123"


def test_activity():
    a = Activity(video_id="v1", title="My Video", published_at="2024-01-01T00:00:00Z", video_type="upload")
    assert a.video_type == "upload"


def test_filter_result():
    r = FilterResult(passed=False, reason="too short", skipped_by="duration_filter")
    assert not r.passed
    assert r.skipped_by == "duration_filter"


def test_route_result():
    r = RouteResult(playlist_id="PLabc", playlist_title="Music", rule_name="gaming_rule")
    assert r.playlist_id == "PLabc"


def test_pipeline_summary_defaults():
    s = PipelineSummary(started_at="now")
    assert s.status == "running"
    assert s.videos_added == 0

from ys2wl.filters.word_filter import word_filter
from ys2wl.filters.duration_filter import duration_filter
from ys2wl.filters.title_similarity import title_similarity
from ys2wl.filters.ignore_list import ignore_list_filter
from ys2wl.filters.router import evaluate_rules
from ys2wl.models.youtube import RoutingRule, Activity


class TestWordFilter:
    def test_skip_on_word_match(self):
        result = word_filter("Learn Python Programming", ["python"])
        assert not result.passed
        assert result.skipped_by == "word_filter"

    def test_pass_when_no_match(self):
        result = word_filter("Hello World", ["python"])
        assert result.passed

    def test_case_insensitive(self):
        result = word_filter("PYTHON tutorial", ["python"])
        assert not result.passed

    def test_empty_ignore_list(self):
        result = word_filter("Python tutorial", [])
        assert result.passed

    def test_empty_title(self):
        result = word_filter("", ["test"])
        assert result.passed


class TestDurationFilter:
    def test_pass_when_no_limits(self):
        assert duration_filter(120, 0, 0).passed

    def test_too_short(self):
        result = duration_filter(30, 60, 0)
        assert not result.passed
        assert result.skipped_by == "duration_filter"

    def test_too_long(self):
        result = duration_filter(5000, 0, 3600)
        assert not result.passed

    def test_within_bounds(self):
        assert duration_filter(180, 60, 3600).passed

    def test_exact_minimum(self):
        assert duration_filter(60, 60, 0).passed

    def test_exact_maximum(self):
        assert duration_filter(3600, 0, 3600).passed


class TestIgnoreListFilter:
    def test_video_in_ignore_list(self):
        result = ignore_list_filter("v123", ["v123", "v456"])
        assert not result.passed
        assert result.skipped_by == "ignore_list"

    def test_video_not_in_ignore_list(self):
        result = ignore_list_filter("v789", ["v123", "v456"])
        assert result.passed

    def test_empty_ignore_list(self):
        result = ignore_list_filter("v123", [])
        assert result.passed


class TestTitleSimilarity:
    def test_identical_titles_return_low_distance(self):
        result = title_similarity("Hello World", [("v1", "Hello World")], 80)
        assert not result.passed

    def test_different_titles_pass(self):
        result = title_similarity("Unique Title", [("v1", "Something Else")], 80)
        assert result.passed

    def test_empty_db_passes(self):
        result = title_similarity("Anything", [], 80)
        assert result.passed

    def test_normalize_removes_special_chars(self):
        result = title_similarity("Hello-World!", [("v1", "Hello_World?")], 80)
        assert not result.passed


class TestRouter:
    def test_no_rules_returns_none(self):
        rules = []
        activity = Activity(
            video_id="v1", title="Test", published_at="now", video_type="upload"
        )
        result = evaluate_rules(
            activity, "Music Channel", 300, rules, "PL_DEFAULT", "Default"
        )
        assert result is None

    def test_catch_all_returns_playlist(self):
        rules = [
            RoutingRule(
                name="Catch All",
                priority=0,
                field=None,
                operator="contains",
                pattern=None,
                destination_playlist_id="PL_CATCH",
                destination_playlist_title="Catch All",
                enabled=True,
                catch_all=True,
            ),
        ]
        activity = Activity(
            video_id="v1", title="Test", published_at="now", video_type="upload"
        )
        result = evaluate_rules(
            activity, "Music Channel", 300, rules, "PL_DEFAULT", "Default"
        )
        assert result is not None
        assert result.playlist_id == "PL_CATCH"

    def test_channel_title_match(self):
        rules = [
            RoutingRule(
                name="Music",
                priority=10,
                field="channel_title",
                operator="contains",
                pattern="music",
                destination_playlist_id="PL_MUSIC",
                destination_playlist_title="Music",
                enabled=True,
            ),
        ]
        activity = Activity(
            video_id="v1", title="Song", published_at="now", video_type="upload"
        )
        result = evaluate_rules(
            activity, "Music Channel", 300, rules, "PL_DEFAULT", "Default"
        )
        assert result.playlist_id == "PL_MUSIC"

    def test_video_title_regex(self):
        rules = [
            RoutingRule(
                name="Tutorial",
                priority=10,
                field="video_title",
                operator="regex",
                pattern=".*tutorial.*",
                destination_playlist_id="PL_LEARN",
                destination_playlist_title="Learning",
                enabled=True,
            ),
        ]
        activity = Activity(
            video_id="v1",
            title="Python Tutorial 2024",
            published_at="now",
            video_type="upload",
        )
        result = evaluate_rules(
            activity, "Tech Channel", 600, rules, "PL_DEFAULT", "Default"
        )
        assert result.playlist_id == "PL_LEARN"

    def test_duration_lt(self):
        rules = [
            RoutingRule(
                name="Shorts",
                priority=10,
                field="duration_seconds",
                operator="lt",
                pattern="60",
                destination_playlist_id="PL_SHORTS",
                destination_playlist_title="Shorts",
                enabled=True,
            ),
        ]
        activity = Activity(
            video_id="v1", title="Short clip", published_at="now", video_type="upload"
        )
        result = evaluate_rules(
            activity, "Any Channel", 30, rules, "PL_DEFAULT", "Default"
        )
        assert result.playlist_id == "PL_SHORTS"

    def test_priority_ordering(self):
        rules = [
            RoutingRule(
                name="Low",
                priority=1,
                field="channel_title",
                operator="contains",
                pattern="test",
                destination_playlist_id="PL_LOW",
                destination_playlist_title="Low",
                enabled=True,
            ),
            RoutingRule(
                name="High",
                priority=100,
                field="channel_title",
                operator="contains",
                pattern="test",
                destination_playlist_id="PL_HIGH",
                destination_playlist_title="High",
                enabled=True,
            ),
        ]
        activity = Activity(
            video_id="v1", title="Video", published_at="now", video_type="upload"
        )
        result = evaluate_rules(
            activity, "Test Channel", 120, rules, "PL_DEFAULT", "Default"
        )
        assert result.playlist_id == "PL_HIGH"

    def test_disabled_rule_skipped(self):
        rules = [
            RoutingRule(
                name="Disabled",
                priority=10,
                field="channel_title",
                operator="contains",
                pattern="test",
                destination_playlist_id="PL_DISABLED",
                destination_playlist_title="Disabled",
                enabled=False,
            ),
        ]
        activity = Activity(
            video_id="v1", title="Video", published_at="now", video_type="upload"
        )
        result = evaluate_rules(
            activity, "Test Channel", 120, rules, "PL_DEFAULT", "Default"
        )
        assert result is None

    def test_rule_minimum_length_bounds(self):
        rules = [
            RoutingRule(
                name="Long Only",
                priority=10,
                field="channel_title",
                operator="contains",
                pattern="test",
                destination_playlist_id="PL_LONG",
                destination_playlist_title="Long Only",
                enabled=True,
                minimum_length="60s",
                maximum_length="0s",
            ),
        ]
        activity = Activity(
            video_id="v1", title="Video", published_at="now", video_type="upload"
        )
        # 30s < 60s minimum → falls through → None
        result = evaluate_rules(
            activity, "Test Channel", 30, rules, "PL_DEFAULT", "Default"
        )
        assert result is None
        # 120s >= 60s → matches
        result = evaluate_rules(
            activity, "Test Channel", 120, rules, "PL_DEFAULT", "Default"
        )
        assert result is not None
        assert result.playlist_id == "PL_LONG"

    def test_rule_maximum_length_bounds(self):
        rules = [
            RoutingRule(
                name="Shorts Only",
                priority=10,
                field="channel_title",
                operator="contains",
                pattern="test",
                destination_playlist_id="PL_SHORTS",
                destination_playlist_title="Shorts",
                enabled=True,
                minimum_length="0s",
                maximum_length="60s",
            ),
        ]
        activity = Activity(
            video_id="v1", title="Video", published_at="now", video_type="upload"
        )
        # 30s <= 60s → matches
        result = evaluate_rules(
            activity, "Test Channel", 30, rules, "PL_DEFAULT", "Default"
        )
        assert result is not None
        assert result.playlist_id == "PL_SHORTS"
        # 120s > 60s → falls through → None
        result = evaluate_rules(
            activity, "Test Channel", 120, rules, "PL_DEFAULT", "Default"
        )
        assert result is None

    def test_catch_all_fallback_after_no_match(self):
        rules = [
            RoutingRule(
                name="Specific",
                priority=10,
                field="channel_title",
                operator="contains",
                pattern="gaming",
                destination_playlist_id="PL_GAMING",
                destination_playlist_title="Gaming",
                enabled=True,
                minimum_length="0s",
                maximum_length="0s",
            ),
            RoutingRule(
                name="Everything Else",
                priority=0,
                field=None,
                operator="contains",
                pattern=None,
                destination_playlist_id="PL_CATCH",
                destination_playlist_title="Catch All",
                enabled=True,
                catch_all=True,
            ),
        ]
        activity = Activity(
            video_id="v1", title="Video", published_at="now", video_type="upload"
        )
        # No matching rule → falls through to catch-all
        result = evaluate_rules(
            activity, "Music Channel", 300, rules, "PL_DEFAULT", "Default"
        )
        assert result is not None
        assert result.playlist_id == "PL_CATCH"
        # Gaming channel matches the specific rule
        result = evaluate_rules(
            activity, "Gaming Channel", 300, rules, "PL_DEFAULT", "Default"
        )
        assert result is not None
        assert result.playlist_id == "PL_GAMING"


class TestFuzzRatio:
    def test_identical_strings(self):
        from ys2wl.filters.title_similarity import _fuzz_ratio

        assert _fuzz_ratio("hello", "hello") == 100

    def test_completely_different(self):
        from ys2wl.filters.title_similarity import _fuzz_ratio

        assert _fuzz_ratio("abc", "xyz") == 0

    def test_kitten_sitting(self):
        from ys2wl.filters.title_similarity import _fuzz_ratio

        ratio = _fuzz_ratio("kitten", "sitting")
        assert 55 <= ratio <= 60

    def test_empty_strings(self):
        from ys2wl.filters.title_similarity import _fuzz_ratio

        # early return for falsy input, not the max_len==0 branch
        assert _fuzz_ratio("", "") == 0

    def test_one_empty(self):
        from ys2wl.filters.title_similarity import _fuzz_ratio

        assert _fuzz_ratio("abc", "") == 0

    def test_reversed_args_same_result(self):
        from ys2wl.filters.title_similarity import _fuzz_ratio

        a = _fuzz_ratio("abcdef", "abcxyz")
        b = _fuzz_ratio("abcxyz", "abcdef")
        assert a == b

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlite3 import Connection
from ys2wl.config import Settings
from ys2wl.core.youtube import YouTubeAPIClient
from ys2wl.db import repository as repo
from ys2wl.filters.word_filter import word_filter
from ys2wl.filters.title_similarity import title_similarity
from ys2wl.filters.ignore_list import ignore_list_filter
from ys2wl.filters.router import evaluate_rules
from ys2wl import metrics
from ys2wl.models.pipeline import PipelineSummary, VideoResult, FilterResult
from ys2wl.models.youtube import Channel, Playlist, RoutingRule

log = logging.getLogger("ys2wl.pipeline")


class PipelineOrchestrator:
    def __init__(
        self,
        settings: Settings,
        youtube: YouTubeAPIClient,
        db_con: Connection,
        channel: Channel,
        playlist: Playlist,
        ignore_subscriptions: list[str],
        ignore_videos: list[str],
        ignore_words: list[str],
        default_playlist_id: str,
        default_playlist_title: str,
        routing_rules: Optional[list[RoutingRule]] = None,
    ):
        self.settings = settings
        self.youtube = youtube
        self.db_con = db_con
        self.channel = channel
        self.playlist = playlist
        self.ignore_subscriptions = ignore_subscriptions
        self.ignore_videos = ignore_videos
        self.ignore_words = ignore_words
        self.default_playlist_id = default_playlist_id
        self.default_playlist_title = default_playlist_title
        self.routing_rules = routing_rules or []

    def run(self) -> PipelineSummary:
        now_iso = datetime.now(timezone.utc).isoformat()
        summary = PipelineSummary(started_at=now_iso)
        start = time.time()

        try:
            subscriptions = self.youtube.get_subscriptions()
            log.info("Total subscriptions: %d", len(subscriptions))
        except Exception as e:
            summary.status = "failed"
            summary.error_message = str(e)
            summary.errors = 1
            return summary

        for s_idx, sub in enumerate(subscriptions):
            if sub.title in self.ignore_subscriptions:
                log.info("Skipping ignored subscription: %s", sub.title)
                summary.subscriptions_skipped += 1
                summary.subscription_skips.append(
                    dict(
                        subscription_title=sub.title,
                        reason="ignored",
                        reason_detail="Subscription is in the ignore list",
                    )
                )
                continue

            last_processed = repo.get_subscription_timestamp(self.db_con, sub.id)
            if last_processed:
                reprocess_threshold = datetime.now(timezone.utc) - timedelta(
                    days=self.settings.reprocess_days
                )
                try:
                    last_dt = datetime.fromisoformat(last_processed)
                    if last_dt.replace(tzinfo=timezone.utc) > reprocess_threshold:
                        log.info(
                            "Subscription '%s' processed within %d days, skipping",
                            sub.title,
                            self.settings.reprocess_days,
                        )
                        summary.subscriptions_skipped += 1
                        summary.subscription_skips.append(
                            dict(
                                subscription_title=sub.title,
                                reason="already_up_to_date",
                                reason_detail=f"Checked within {self.settings.reprocess_days} day reprocess window",
                            )
                        )
                        continue
                except ValueError:
                    pass

            published_after = (
                self.settings.published_after
                or last_processed
                or (datetime.now(timezone.utc) - timedelta(weeks=52)).isoformat()
            )

            try:
                activities = self.youtube.get_subscription_activity(
                    sub.channel_id, published_after=published_after
                )
            except Exception as e:
                log.error("Failed to get activity for %s: %s", sub.title, e)
                summary.errors += 1
                continue

            summary.subscriptions_processed += 1
            activity_count = 0
            for activity in activities:
                if (
                    self.settings.activity_limit > 0
                    and activity_count >= self.settings.activity_limit
                ):
                    break
                activity_count += 1

                result = self._process_activity(activity, sub.title, sub.id)
                summary.video_results.append(result)
                if result.added:
                    summary.videos_added += 1
                elif result.filter_result and not result.filter_result.passed:
                    summary.videos_skipped += 1
                if result.error:
                    summary.errors += 1

                if activity_count < len(activities):
                    time.sleep(self.settings.playlist_sleep)

            repo.insert_subscription(self.db_con, sub.id, sub.title, now_iso)

            if (
                self.settings.subscription_limit > 0
                and summary.subscriptions_processed >= self.settings.subscription_limit
            ):
                log.info("Subscription limit reached")
                break

            if s_idx < len(subscriptions) - 1:
                time.sleep(self.settings.subscription_sleep)

        summary.finished_at = datetime.now(timezone.utc).isoformat()
        summary.status = "completed" if summary.errors == 0 else "completed_with_errors"

        duration = time.time() - start
        metrics.pipeline_duration_seconds.observe(duration)
        metrics.subscriptions_processed_total.inc(summary.subscriptions_processed)
        metrics.subscriptions_skipped_total.inc(summary.subscriptions_skipped)
        metrics.videos_added_total.inc(summary.videos_added)
        metrics.videos_skipped_total.labels(reason="total").inc(summary.videos_skipped)
        metrics.errors_total.inc(summary.errors)
        metrics.last_pipeline_status.set(1 if summary.errors == 0 else 0)
        if self.youtube:
            metrics.quota_estimate.set(self.youtube.api_calls[0])

        return summary

    def _process_activity(
        self, activity, channel_title: str, subscription_id: str
    ) -> VideoResult:
        result = VideoResult(
            video_id=activity.video_id,
            title=activity.title,
            subscription_title=channel_title,
            subscription_id=subscription_id,
        )

        fr = word_filter(activity.title, self.ignore_words)
        if not fr.passed:
            result.filter_result = fr
            log.info("Filtered by word: %s - %s", channel_title, activity.title)
            return result

        fr = ignore_list_filter(activity.video_id, self.ignore_videos)
        if not fr.passed:
            result.filter_result = fr
            log.info(
                "Filtered by ignore list: %s - %s", channel_title, activity.video_id
            )
            return result

        if repo.video_exists(self.db_con, activity.video_id):
            result.filter_result = FilterResult(
                passed=False, reason="Already in DB", skipped_by="db_exists"
            )
            log.info(
                "Skipped (exists in DB): %s - %s", channel_title, activity.video_id
            )
            return result

        existing_titles = repo.get_all_video_titles(self.db_con)
        fr = title_similarity(
            activity.title, existing_titles, self.settings.compare_distance
        )
        if not fr.passed:
            result.filter_result = fr
            log.info(
                "Filtered by title similarity: %s - %s", channel_title, activity.title
            )
            return result

        video_length = 0
        try:
            video_length = self.youtube.get_video_duration(activity.video_id)
        except Exception as e:
            log.warning("Could not get duration for %s: %s", activity.video_id, e)

        route_result = evaluate_rules(
            activity,
            channel_title,
            video_length,
            self.routing_rules,
            self.default_playlist_id,
            self.default_playlist_title,
        )
        if route_result is None:
            result.filter_result = FilterResult(
                passed=False, reason="No matching rule", skipped_by="no_rule_match"
            )
            log.info("No rule matched: %s - %s", channel_title, activity.title)
            return result

        result.route_result = route_result

        try:
            success = self.youtube.add_to_playlist(
                route_result.playlist_id, activity.video_id
            )
            if success:
                repo.insert_video(
                    self.db_con,
                    activity.video_id,
                    datetime.now(timezone.utc).isoformat(),
                    activity.title,
                    subscription_id,
                    route_result.playlist_id,
                    video_length,
                    route_result.rule_name,
                )
                result.added = True
                log.info(
                    "Added: %s -> %s (%s)",
                    activity.title,
                    route_result.playlist_title,
                    route_result.rule_name,
                )
            else:
                result.error = "add_to_playlist returned False"
        except Exception as e:
            result.error = str(e)
            log.error("Failed to add video %s: %s", activity.video_id, e)

        return result

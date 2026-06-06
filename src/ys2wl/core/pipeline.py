import logging
import time
from datetime import datetime, timezone, timedelta
from sqlite3 import Connection
from ys2wl.config import Settings
from ys2wl.core.youtube import YouTubeAPIClient
from ys2wl.db.repository import pipeline as pl, videos as v, ignore_lists as il
from ys2wl.filters.word_filter import word_filter
from ys2wl.filters.title_similarity import title_similarity
from ys2wl.filters.ignore_list import ignore_list_filter
from ys2wl.filters.selector_filter import selector_filter
from ys2wl import metrics
from ys2wl.models.pipeline import (
    PipelineSummary,
    VideoResult,
    FilterResult,
    RouteResult,
    PipelineConfig,
    PipelineSelector,
)
from ys2wl.models.youtube import Channel, Playlist, Activity

log = logging.getLogger("ys2wl.pipeline")


class PipelineOrchestrator:
    def __init__(
        self,
        settings: Settings,
        youtube: YouTubeAPIClient,
        db_con: Connection,
        channel: Channel,
        playlist: Playlist,
        pipelines: list[PipelineConfig],
        all_ignore_lists: dict[str, list[str]],  # list_id -> [values]
        default_playlist_id: str,
        default_playlist_title: str,
        dry_run: bool = False,
        on_progress=None,
    ):
        self.settings = settings
        self.youtube = youtube
        self.db_con = db_con
        self.channel = channel
        self.playlist = playlist
        self.pipelines = pipelines
        self.all_ignore_lists = all_ignore_lists
        self.default_playlist_id = default_playlist_id
        self.default_playlist_title = default_playlist_title
        self.dry_run = dry_run
        self.on_progress = on_progress

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    # ── Phase 1: Data Collection ──────────────────────────────────

    def _collect_activities(self, subscriptions: list) -> dict[str, list[Activity]]:
        """Fetch activities for all subscriptions and cache in DB.
        Returns dict of sub_id -> [Activity] for convenience."""
        cached: dict[str, list[Activity]] = {}
        for sub in subscriptions:
            pub_after = self._compute_published_after(sub)
            try:
                activities = self.youtube.get_subscription_activity(
                    sub.channel_id, published_after=pub_after
                )
            except Exception as e:
                log.error("Failed to fetch activity for %s: %s", sub.title, e)
                continue

            activity_objects = []
            for a in activities:
                obj = Activity(
                    video_id=a.video_id,
                    title=a.title,
                    published_at=a.published_at,
                    video_type=a.video_type,
                    description=getattr(a, "description", ""),
                )
                activity_objects.append(obj)
                v.cache_activity(
                    self.db_con,
                    sub.id,
                    obj.video_id,
                    obj.title,
                    sub.channel_id,
                    sub.title,
                    obj.video_type,
                    obj.description,
                    obj.published_at,
                )
            cached[sub.id] = activity_objects
            log.info(
                "%sCached %d activities for %s",
                "[DRY-RUN] " if self.dry_run else "",
                len(activity_objects),
                sub.title,
            )
        return cached

    def _compute_published_after(self, sub) -> str:
        """Compute the earliest time we need data for this subscription
        across all pipelines."""
        min_ts = pl.get_min_tracking_for_subscription(self.db_con, sub.id)
        candidates = []
        if self.settings.published_after:
            candidates.append(self.settings.published_after)
        if min_ts:
            candidates.append(min_ts)
        if candidates:
            return min(candidates)
        return (datetime.now(timezone.utc) - timedelta(weeks=52)).isoformat()

    # ── Phase 2: Pipeline Processing ──────────────────────────────

    def _run(self) -> PipelineSummary:
        now_iso = self._now_iso()
        summary = PipelineSummary(started_at=now_iso)
        start = time.time()

        # Fetch subscriptions once
        try:
            subscriptions = self.youtube.get_subscriptions()
        except Exception as e:
            summary.status = "failed"
            summary.error_message = str(e)
            summary.errors = 1
            return summary

        # Phase 1: Collect all activities into cache
        activity_cache = self._collect_activities(subscriptions)

        # Phase 2: Process each pipeline
        for pipeline in self.pipelines:
            if not pipeline.enabled:
                continue
            summary.pipelines_invoked += 1
            pipeline_errors = 0

            # Resolve ignore lists for this pipeline
            pipeline_ignore_list_ids = pl.get_pipeline_ignore_list_ids(
                self.db_con, pipeline.id
            )
            ignore_videos: list[str] = []
            ignore_words: list[str] = []
            ignore_subs: list[str] = []
            for lid in pipeline_ignore_list_ids:
                entries = self.all_ignore_lists.get(lid, [])
                list_type = self._get_list_type(lid)
                if list_type == "video":
                    ignore_videos.extend(entries)
                elif list_type == "word":
                    ignore_words.extend(entries)
                elif list_type == "subscription":
                    ignore_subs.extend(entries)

            # Resolve selectors
            selector_rows = pl.get_pipeline_selectors(self.db_con, pipeline.id)
            selectors = [
                PipelineSelector(
                    id=r["id"],
                    pipeline_id=r["pipeline_id"],
                    field=r["field"],
                    operator=r["operator"],
                    pattern=r["pattern"],
                    combine_operator=r.get("combine_operator", "AND"),
                )
                for r in selector_rows
            ]

            # Determine subscription scope
            if pipeline.subscription_scope == "selected":
                selected_ids = pl.get_pipeline_subscription_ids(
                    self.db_con, pipeline.id
                )
                target_subs = [s for s in subscriptions if s.id in selected_ids]
            else:
                target_subs = subscriptions

            for sub in target_subs:
                # 2.1: Subscription ignore list
                if sub.title in ignore_subs:
                    summary.subscriptions_skipped += 1
                    skip = dict(
                        subscription_title=sub.title,
                        reason="ignored",
                        reason_detail="Subscription in pipeline ignore list",
                    )
                    summary.subscription_skips.append(skip)
                    if self.on_progress:
                        self.on_progress(skip, summary)
                    continue

                # 2.2: Reprocess window
                last_processed = pl.get_pipeline_tracking(
                    self.db_con, pipeline.id, sub.id
                )
                if last_processed:
                    threshold = datetime.now(timezone.utc) - timedelta(
                        days=self.settings.reprocess_days
                    )
                    try:
                        last_dt = datetime.fromisoformat(last_processed)
                        if last_dt.replace(tzinfo=timezone.utc) > threshold:
                            summary.subscriptions_skipped += 1
                            skip = dict(
                                subscription_title=sub.title,
                                reason="already_up_to_date",
                                reason_detail=f"Checked within {self.settings.reprocess_days}d reprocess window",
                            )
                            summary.subscription_skips.append(skip)
                            if self.on_progress:
                                self.on_progress(skip, summary)
                            continue
                    except ValueError:
                        pass

                # 2.3: Get cached activities
                activities = activity_cache.get(sub.id, [])
                if not activities:
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

                    result = self._process_activity(
                        activity,
                        sub,
                        pipeline,
                        ignore_videos,
                        ignore_words,
                        selectors,
                    )
                    summary.video_results.append(result)
                    if result.added:
                        summary.videos_added += 1
                    elif result.filter_result and not result.filter_result.passed:
                        summary.videos_skipped += 1
                    if result.error:
                        summary.errors += 1
                        pipeline_errors += 1
                    if self.on_progress:
                        self.on_progress(result, summary)

                    # Per-activity watermark
                    if not self.dry_run:
                        pl.upsert_pipeline_tracking(
                            self.db_con,
                            pipeline.id,
                            sub.id,
                            activity.published_at,
                        )

                    if activity_count < len(activities):
                        time.sleep(self.settings.playlist_sleep)

                # End-activity loop — final watermark to ensure we captured
                # the latest activity even if it was the last one
                if activities and not self.dry_run:
                    pl.upsert_pipeline_tracking(
                        self.db_con,
                        pipeline.id,
                        sub.id,
                        activities[-1].published_at,
                    )

            # End-subscription loop
            if pipeline_errors > 0:
                summary.pipelines_with_errors += 1

            if (
                self.settings.subscription_limit > 0
                and summary.subscriptions_processed >= self.settings.subscription_limit
            ):
                break

        # Phase 3: Finalize
        summary.finished_at = self._now_iso()
        summary.status = "completed" if summary.errors == 0 else "completed_with_errors"

        if not self.dry_run:
            duration = time.time() - start
            metrics.pipeline_duration_seconds.observe(duration)
            metrics.subscriptions_processed_total.inc(summary.subscriptions_processed)
            metrics.subscriptions_skipped_total.inc(summary.subscriptions_skipped)
            metrics.videos_added_total.inc(summary.videos_added)
            metrics.videos_skipped_total.labels(reason="total").inc(
                summary.videos_skipped
            )
            metrics.errors_total.inc(summary.errors)
            metrics.last_pipeline_status.set(1 if summary.errors == 0 else 0)
            if self.youtube:
                metrics.quota_estimate.set(self.youtube.api_calls[0])

            # Clear activity cache
            v.clear_activity_cache(self.db_con)

        return summary

    def _process_activity(
        self,
        activity: Activity,
        sub,
        pipeline: PipelineConfig,
        ignore_videos: list[str],
        ignore_words: list[str],
        selectors: list[PipelineSelector],
    ) -> VideoResult:
        result = VideoResult(
            video_id=activity.video_id,
            title=activity.title,
            subscription_title=sub.title,
            subscription_id=sub.id,
            pipeline_id=pipeline.id,
            pipeline_name=pipeline.name,
        )

        # 2.3.2: Video_id ignore lists
        fr = ignore_list_filter(activity.video_id, ignore_videos)
        if not fr.passed:
            result.filter_result = fr
            return result

        # 2.3.3: Word ignore lists
        fr = word_filter(activity.title, ignore_words)
        if not fr.passed:
            result.filter_result = fr
            return result

        # 2.3.4: Per-pipeline DB exists
        if pipeline.check_db_exists:
            if v.video_exists_for_pipeline(self.db_con, activity.video_id, pipeline.id):
                result.filter_result = FilterResult(
                    passed=False,
                    reason="Already in DB for this pipeline",
                    skipped_by="db_exists",
                )
                return result

        # 2.3.5: Per-pipeline title similarity
        if pipeline.check_title_similarity:
            existing = v.get_all_video_titles_for_pipeline(self.db_con, pipeline.id)
            fr = title_similarity(activity.title, existing, pipeline.compare_distance)
            if not fr.passed:
                result.filter_result = fr
                return result

        # 2.3.6: Duration bounds
        video_length = 0
        try:
            video_length = self.youtube.get_video_duration(activity.video_id)
        except Exception as e:
            log.warning("Could not get duration for %s: %s", activity.video_id, e)

        if (
            pipeline.duration_min_seconds > 0
            and video_length < pipeline.duration_min_seconds
        ):
            result.filter_result = FilterResult(
                passed=False,
                reason=f"Duration {video_length}s below min {pipeline.duration_min_seconds}s",
                skipped_by="duration",
            )
            return result
        if (
            pipeline.duration_max_seconds > 0
            and video_length > pipeline.duration_max_seconds
        ):
            result.filter_result = FilterResult(
                passed=False,
                reason=f"Duration {video_length}s above max {pipeline.duration_max_seconds}s",
                skipped_by="duration",
            )
            return result

        # 2.3.7: Pipeline selectors
        fr = selector_filter(activity, sub.title, selectors, pipeline.selector_mode)
        if not fr.passed:
            result.filter_result = fr
            return result

        # All criteria met — add to playlist + DB
        route_result = RouteResult(
            playlist_id=pipeline.destination_playlist_id,
            playlist_title=pipeline.destination_playlist_title,
            rule_name=pipeline.name,
        )
        result.route_result = route_result

        if self.dry_run:
            result.added = True
            log.info(
                "[DRY-RUN] Would add: %s -> %s (%s via %s)",
                activity.title,
                route_result.playlist_title,
                route_result.rule_name,
                pipeline.name,
            )
        else:
            try:
                success = self.youtube.add_to_playlist(
                    route_result.playlist_id, activity.video_id
                )
                if success:
                    v.insert_video(
                        self.db_con,
                        activity.video_id,
                        datetime.now(timezone.utc).isoformat(),
                        activity.title,
                        sub.id,
                        route_result.playlist_id,
                        video_length,
                        route_result.rule_name,
                        pipeline.id,
                    )
                    result.added = True
                    log.info(
                        "Added: %s -> %s (%s via %s)",
                        activity.title,
                        route_result.playlist_title,
                        route_result.rule_name,
                        pipeline.name,
                    )
                else:
                    result.error = "add_to_playlist returned False"
            except Exception as e:
                result.error = str(e)
                log.error("Failed to add video %s: %s", activity.video_id, e)

        return result

    def _get_list_type(self, list_id: str) -> str:
        """Determine ignore list type from the list_id."""
        list_info = il.get_ignore_list(self.db_con, list_id)
        return list_info["list_type"] if list_info else ""

    def run(self) -> PipelineSummary:
        try:
            return self._run()
        except Exception as e:
            log.error("Pipeline run failed: %s", e)
            summary = PipelineSummary(started_at=self._now_iso())
            summary.status = "failed"
            summary.error_message = str(e)
            summary.errors = 1
            summary.finished_at = self._now_iso()
            if not self.dry_run:
                v.clear_activity_cache(self.db_con)
            return summary

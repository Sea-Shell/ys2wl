from ys2wl.api.deps import require_youtube
from ys2wl.core.pipeline import PipelineOrchestrator
from ys2wl.models.youtube import Channel, Playlist
from ys2wl.models.pipeline import PipelineConfig
from ys2wl.db import repository as repo
import logging
import sqlite3

log = logging.getLogger("ys2wl.core.pipeline_runner")


async def execute_pipeline(state, trigger="manual", dry_run=False, pipeline_id=None):
    require_youtube(state)
    run_id = repo.create_pipeline_run(state.db_con, trigger=trigger, dry_run=dry_run)
    if run_id is None:
        log.error("Failed to create pipeline run")
        return None
    try:
        channel_data = repo.get_channel(state.db_con)
        playlist_data = repo.get_playlist(state.db_con)
        if not channel_data:
            channels = state.youtube.get_channel_id()
            if channels:
                channel_data = {"id": channels[0].id, "title": channels[0].title}
                repo.insert_channel(
                    state.db_con, channel_data["id"], channel_data["title"]
                )
        channel_data = repo.get_channel(state.db_con)
        if not channel_data:
            log.error("No channel found, aborting pipeline run.")
            repo.finish_pipeline_run(
                state.db_con,
                run_id,
                {"status": "failed", "error_message": "No channel found."},
            )
            return None

        if not playlist_data:
            playlists = state.youtube.get_user_playlists(channel_data["id"])
            if playlists:
                playlist_data = {"id": playlists[0].id, "title": playlists[0].title}
                repo.insert_playlist(
                    state.db_con, playlist_data["id"], playlist_data["title"]
                )
        playlist_data = repo.get_playlist(state.db_con)
        if not playlist_data:
            log.error("No playlist found, aborting pipeline run.")
            repo.finish_pipeline_run(
                state.db_con,
                run_id,
                {"status": "failed", "error_message": "No playlist found."},
            )
            return None

        channel = Channel(id=channel_data["id"], title=channel_data["title"])
        playlist = Playlist(id=playlist_data["id"], title=playlist_data["title"])

        # Load pipelines
        db_pipelines = repo.get_pipelines(state.db_con)
        if pipeline_id is not None:
            db_pipelines = [p for p in db_pipelines if p["id"] == pipeline_id]
        pipelines = [
            PipelineConfig(
                id=p["id"],
                name=p["name"],
                enabled=bool(p["enabled"]),
                selector_mode=p["selector_mode"],
                duration_min_seconds=p["duration_min_seconds"],
                duration_max_seconds=p["duration_max_seconds"],
                check_db_exists=bool(p["check_db_exists"]),
                check_title_similarity=bool(p["check_title_similarity"]),
                compare_distance=p["compare_distance"],
                subscription_scope=p["subscription_scope"],
                destination_playlist_id=p["destination_playlist_id"],
                destination_playlist_title=p["destination_playlist_title"],
                created_at=p["created_at"],
                updated_at=p["updated_at"],
            )
            for p in db_pipelines
        ]

        # Load all ignore lists (entries pre-fetched)
        all_ignore_lists: dict[str, list[str]] = {}
        db_lists = repo.get_ignore_lists(state.db_con)
        for lst in db_lists:
            entries = repo.get_ignore_list_entries(state.db_con, lst["id"])
            all_ignore_lists[lst["id"]] = entries

        import asyncio

        def _build_progress_callback(tcon):
            """Return on_progress callback that saves decisions & counters immediately."""

            def _on_progress(decision, summary):
                try:
                    if isinstance(decision, dict):
                        d = {
                            "video_id": None,
                            "title": decision.get("subscription_title"),
                            "subscription_title": decision.get("subscription_title"),
                            "action": "subscription_skipped",
                            "reason": decision.get("reason"),
                            "reason_detail": decision.get("reason_detail"),
                            "routed_to": None,
                        }
                    else:
                        r = decision
                        if r.added:
                            action, reason, reason_detail = "added", None, None
                        elif r.error:
                            action, reason, reason_detail = (
                                "error",
                                "add_failed",
                                r.error,
                            )
                        elif r.filter_result:
                            action = "skipped"
                            reason = r.filter_result.skipped_by or "filter"
                            reason_detail = r.filter_result.reason
                        else:
                            action, reason, reason_detail = "skipped", "unknown", None
                        d = {
                            "video_id": r.video_id,
                            "title": r.title,
                            "subscription_title": r.subscription_title,
                            "action": action,
                            "reason": reason,
                            "reason_detail": reason_detail,
                            "routed_to": r.route_result.playlist_title
                            if r.route_result
                            else None,
                        }
                    repo.insert_run_decision(tcon, run_id, d)
                    repo.update_pipeline_run_progress(
                        tcon,
                        run_id,
                        {
                            "subscriptions_processed": summary.subscriptions_processed,
                            "subscriptions_skipped": summary.subscriptions_skipped,
                            "videos_added": summary.videos_added,
                            "videos_skipped": summary.videos_skipped,
                            "errors": summary.errors,
                            "pipelines_invoked": summary.pipelines_invoked,
                            "pipelines_with_errors": summary.pipelines_with_errors,
                        },
                    )
                except Exception as e:
                    log.error("on_progress callback failed for run %d: %s", run_id, e)

            return _on_progress

        def _run_orchestrator():
            tcon = sqlite3.connect(state.settings.database_file)
            tcon.row_factory = sqlite3.Row
            tcon.execute("PRAGMA journal_mode=WAL")
            try:
                orch = PipelineOrchestrator(
                    settings=state.settings,
                    youtube=state.youtube,
                    db_con=tcon,
                    channel=channel,
                    playlist=playlist,
                    pipelines=pipelines,
                    all_ignore_lists=all_ignore_lists,
                    default_playlist_id=playlist.id,
                    default_playlist_title=playlist.title,
                    dry_run=dry_run,
                    on_progress=_build_progress_callback(tcon),
                )
                return orch.run()
            finally:
                tcon.close()

        summary = await asyncio.to_thread(_run_orchestrator)

        # Decisions already saved incrementally — no bulk insert needed
        repo.cleanup_old_decisions(state.db_con)

        repo.finish_pipeline_run(
            state.db_con,
            run_id,
            {
                "status": summary.status,
                "videos_added": summary.videos_added,
                "videos_skipped": summary.videos_skipped,
                "subscriptions_processed": summary.subscriptions_processed,
                "subscriptions_skipped": summary.subscriptions_skipped,
                "errors": summary.errors,
                "error_message": summary.error_message,
                "pipelines_invoked": summary.pipelines_invoked,
                "pipelines_with_errors": summary.pipelines_with_errors,
            },
        )

        if summary.errors == 0:
            repo.set_last_run(state.db_con, summary.started_at)

    except Exception as e:
        log.error("Pipeline run %d failed: %s", run_id, e)
        repo.finish_pipeline_run(
            state.db_con,
            run_id,
            {"status": "failed", "error_message": str(e)},
        )
        return None

    return run_id

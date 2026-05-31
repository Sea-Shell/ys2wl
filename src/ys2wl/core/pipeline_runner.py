from ys2wl.api.deps import require_youtube
from ys2wl.core.pipeline import PipelineOrchestrator
from ys2wl.models.youtube import Channel, Playlist, RoutingRule
from ys2wl.db import repository as repo
import logging
import sqlite3

log = logging.getLogger("ys2wl.core.pipeline_runner")


async def execute_pipeline(state, trigger="manual"):
    require_youtube(state)
    run_id = repo.create_pipeline_run(state.db_con, trigger=trigger)
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
        channel_data = repo.get_channel(state.db_con)  # refetch in case inserted
        if not channel_data:
            log.error("No channel found or created, aborting pipeline run.")
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
        playlist_data = repo.get_playlist(state.db_con)  # refetch in case inserted
        if not playlist_data:
            log.error("No playlist found or created, aborting pipeline run.")
            repo.finish_pipeline_run(
                state.db_con,
                run_id,
                {"status": "failed", "error_message": "No playlist found."},
            )
            return None

        channel = Channel(id=channel_data["id"], title=channel_data["title"])
        playlist = Playlist(id=playlist_data["id"], title=playlist_data["title"])

        ignore_subs = [
            e["pattern"] for e in repo.get_ignore_entries(state.db_con, "subscription")
        ]
        ignore_vids = [
            e["pattern"] for e in repo.get_ignore_entries(state.db_con, "video")
        ]
        ignore_words = [
            e["pattern"] for e in repo.get_ignore_entries(state.db_con, "words")
        ]

        db_rules = repo.get_routing_rules(state.db_con)
        routing_rules = [
            RoutingRule(
                id=r["id"],
                name=r["name"],
                priority=r["priority"],
                field=r["field"],
                operator=r["operator"],
                pattern=r["pattern"],
                destination_playlist_id=r["destination_playlist_id"],
                destination_playlist_title=r.get("destination_playlist_title", ""),
                enabled=bool(r["enabled"]),
                minimum_length=r.get("minimum_length", "0s"),
                maximum_length=r.get("maximum_length", "0s"),
                catch_all=bool(r.get("catch_all", 0)),
            )
            for r in db_rules
        ]

        import asyncio

        def _run_orchestrator():
            tcon = sqlite3.connect(state.settings.database_file)
            tcon.row_factory = sqlite3.Row
            try:
                orch = PipelineOrchestrator(
                    settings=state.settings,
                    youtube=state.youtube,
                    db_con=tcon,
                    channel=channel,
                    playlist=playlist,
                    ignore_subscriptions=ignore_subs,
                    ignore_videos=ignore_vids,
                    ignore_words=ignore_words,
                    default_playlist_id=playlist.id,
                    default_playlist_title=playlist.title,
                    routing_rules=routing_rules,
                )
                return orch.run()
            finally:
                tcon.close()

        summary = await asyncio.to_thread(_run_orchestrator)

        # persist per-video decisions
        decisions = []
        for skip in summary.subscription_skips:
            decisions.append(
                {
                    "video_id": None,
                    "title": skip.get("subscription_title"),
                    "subscription_title": skip.get("subscription_title"),
                    "action": "subscription_skipped",
                    "reason": skip.get("reason"),
                    "reason_detail": skip.get("reason_detail"),
                    "routed_to": None,
                }
            )
        for r in summary.video_results:
            if r.added:
                action = "added"
                reason = None
                reason_detail = None
            elif r.error:
                action = "error"
                reason = "add_failed"
                reason_detail = r.error
            elif r.filter_result:
                action = "skipped"
                reason = r.filter_result.skipped_by or "filter"
                reason_detail = r.filter_result.reason
            else:
                action = "skipped"
                reason = "unknown"
                reason_detail = None
            decisions.append(
                {
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
            )
        if decisions:
            repo.insert_run_decisions(state.db_con, run_id, decisions)
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

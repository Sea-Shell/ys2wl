from typing import List
from fastapi import APIRouter, HTTPException, Request
from ys2wl.api.models import TriggerResponse, PipelineRunResponse
from ys2wl.core.pipeline import PipelineOrchestrator
from ys2wl.db import repository as repo
from ys2wl.models.youtube import Channel, Playlist, RoutingRule
import logging

log = logging.getLogger("ys2wl.api.pipeline")
router = APIRouter()


def _get_state(request: Request):
    return request.app.state.ys2wl


@router.post("/pipeline/trigger", response_model=TriggerResponse)
async def trigger_pipeline(request: Request):
    state = _get_state(request)
    run_id = repo.create_pipeline_run(state.db_con, trigger="manual")
    if run_id is None:
        raise HTTPException(status_code=500, detail="Failed to create pipeline run")

    try:
        channel_data = repo.get_channel(state.db_con)
        playlist_data = repo.get_playlist(state.db_con)
        if not channel_data:
            channels = state.youtube.get_channel_id()
            if channels:
                channel_data = {"id": channels[0].id, "title": channels[0].title}
                repo.insert_channel(state.db_con, channel_data["id"], channel_data["title"])
        if not playlist_data:
            playlists = state.youtube.get_user_playlists(channel_data["id"])
            if playlists:
                playlist_data = {"id": playlists[0].id, "title": playlists[0].title}
                repo.insert_playlist(state.db_con, playlist_data["id"], playlist_data["title"])

        channel = Channel(id=channel_data["id"], title=channel_data["title"])
        playlist = Playlist(id=playlist_data["id"], title=playlist_data["title"])

        ignore_subs = _load_file(state.settings.subscription_ignore_file)
        ignore_vids = _load_file(state.settings.video_ignore_file)
        ignore_words = _load_file(state.settings.words_ignore_file)

        db_rules = repo.get_routing_rules(state.db_con)
        routing_rules = [
            RoutingRule(
                id=r["id"], name=r["name"], priority=r["priority"], field=r["field"],
                operator=r["operator"], pattern=r["pattern"],
                destination_playlist_id=r["destination_playlist_id"],
                destination_playlist_title=r.get("destination_playlist_title", ""),
                enabled=bool(r["enabled"]),
            )
            for r in db_rules
        ]

        orchestrator = PipelineOrchestrator(
            settings=state.settings,
            youtube=state.youtube,
            db_con=state.db_con,
            channel=channel,
            playlist=playlist,
            ignore_subscriptions=ignore_subs,
            ignore_videos=ignore_vids,
            ignore_words=ignore_words,
            default_playlist_id=playlist.id,
            default_playlist_title=playlist.title,
            routing_rules=routing_rules,
        )

        summary = orchestrator.run()
        repo.finish_pipeline_run(state.db_con, run_id, {
            "status": summary.status,
            "videos_added": summary.videos_added,
            "videos_skipped": summary.videos_skipped,
            "subscriptions_processed": summary.subscriptions_processed,
            "subscriptions_skipped": summary.subscriptions_skipped,
            "errors": summary.errors,
            "error_message": summary.error_message,
        })

        if summary.errors == 0:
            repo.set_last_run(state.db_con, summary.started_at)

    except Exception as e:
        log.error("Pipeline run %d failed: %s", run_id, e)
        repo.finish_pipeline_run(state.db_con, run_id, {
            "status": "failed", "error_message": str(e),
        })
        raise HTTPException(status_code=500, detail=str(e))

    return TriggerResponse(run_id=run_id)


@router.get("/pipeline/runs", response_model=List[PipelineRunResponse])
async def list_runs(request: Request):
    state = _get_state(request)
    runs = repo.get_pipeline_runs(state.db_con)
    return [PipelineRunResponse(**r) for r in runs]


@router.get("/pipeline/runs/{run_id}", response_model=PipelineRunResponse)
async def get_run(run_id: int, request: Request):
    state = _get_state(request)
    run = repo.get_pipeline_run(state.db_con, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return PipelineRunResponse(**run)


def _load_file(filepath: str) -> list[str]:
    try:
        with open(filepath) as f:
            return [line.strip() for line in f if line.strip()]
    except (FileNotFoundError, OSError):
        return []

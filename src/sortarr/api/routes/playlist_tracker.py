import logging
from fastapi import APIRouter, HTTPException, Request
from sortarr.api.deps import get_state, require_youtube
from sortarr.core.playlist_tracker import PlaylistTracker
from sortarr.core.youtube import YouTubeAPIClient

log = logging.getLogger("sortarr.api.playlist_tracker")
router = APIRouter()


@router.post("/playlist-tracker/trigger")
async def trigger_playlist_tracker(request: Request):
    state = get_state(request)
    youtube = require_youtube(state)

    channel = state.db_con.execute("SELECT id FROM channel LIMIT 1").fetchone()
    channel_id = channel["id"] if channel else None
    if not channel_id:
        channels = youtube.get_channel_id()
        if channels:
            channel_id = channels[0].id
    if not channel_id:
        raise HTTPException(status_code=404, detail="No channel found")

    import asyncio
    import sqlite3

    def _run():
        tcon = sqlite3.connect(state.settings.database_file)
        tcon.row_factory = sqlite3.Row
        client = YouTubeAPIClient(credentials=state.credentials)
        try:
            tracker = PlaylistTracker(client, tcon, channel_id)
            return tracker.run()
        finally:
            client.close()
            tcon.close()

    result = await asyncio.to_thread(_run)
    return result

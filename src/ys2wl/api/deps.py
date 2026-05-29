import logging
from fastapi import HTTPException, Request

log = logging.getLogger("ys2wl.api")


def get_state(request: Request):
    return request.app.state.ys2wl


def require_youtube(state):
    if state.youtube is None:
        log.warning(
            "YouTube client unavailable — credentials not loaded or not yet authenticated"
        )
        raise HTTPException(
            status_code=503,
            detail="YouTube API not available — authenticate first",
        )
    return state.youtube

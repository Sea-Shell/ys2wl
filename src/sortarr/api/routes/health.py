from fastapi import APIRouter, Request
from sortarr.api.models import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request):
    state = request.app.state.sortarr
    next_run = None
    if state.scheduler:
        next_run = state.scheduler.next_run_time
    return HealthResponse(next_scheduled_run=next_run)

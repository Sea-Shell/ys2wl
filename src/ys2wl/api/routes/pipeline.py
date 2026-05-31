from typing import List
from fastapi import APIRouter, HTTPException, Request
from ys2wl.api.deps import get_state
from ys2wl.api.models import TriggerResponse, PipelineRunResponse, RunDecisionResponse
from ys2wl.core.pipeline_runner import execute_pipeline
from ys2wl.db import repository as repo
import logging

log = logging.getLogger("ys2wl.api.pipeline")
router = APIRouter()


@router.post("/pipeline/trigger", response_model=TriggerResponse)
async def trigger_pipeline(request: Request):
    state = get_state(request)
    run_id = await execute_pipeline(state, trigger="manual")
    if not run_id:
        raise HTTPException(status_code=500, detail="Pipeline run failed")
    return TriggerResponse(run_id=run_id)


@router.get("/pipeline/runs", response_model=List[PipelineRunResponse])
async def list_runs(request: Request):
    state = get_state(request)
    runs = repo.get_pipeline_runs(state.db_con)
    return [PipelineRunResponse(**r) for r in runs]


@router.get("/pipeline/runs/{run_id}", response_model=PipelineRunResponse)
async def get_run(run_id: int, request: Request):
    state = get_state(request)
    run = repo.get_pipeline_run(state.db_con, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return PipelineRunResponse(**run)


@router.get(
    "/pipeline/runs/{run_id}/decisions",
    response_model=List[RunDecisionResponse],
)
async def get_run_decisions(run_id: int, request: Request):
    state = get_state(request)
    decisions = repo.get_run_decisions(state.db_con, run_id)
    return [RunDecisionResponse(**d) for d in decisions]

from typing import List
from fastapi import APIRouter, HTTPException, Request
from ys2wl.api.models import RoutingRuleCreate, RoutingRuleUpdate, RoutingRuleResponse
from ys2wl.db import repository as repo

router = APIRouter()


def _get_state(request: Request):
    return request.app.state.ys2wl


@router.get("/rules", response_model=List[RoutingRuleResponse])
async def list_rules(request: Request):
    state = _get_state(request)
    rules = repo.get_routing_rules(state.db_con)
    return [RoutingRuleResponse(**r) for r in rules]


@router.post("/rules", response_model=RoutingRuleResponse, status_code=201)
async def create_rule(rule: RoutingRuleCreate, request: Request):
    state = _get_state(request)
    rid = repo.create_routing_rule(
        state.db_con, rule.name, rule.priority, rule.field,
        rule.operator, rule.pattern, rule.destination_playlist_id,
        rule.destination_playlist_title,
    )
    if rid is None:
        raise HTTPException(status_code=500, detail="Failed to create rule")
    con = state.db_con
    cursor = con.execute("SELECT * FROM routing_rules WHERE id = ?", (rid,))
    row = cursor.fetchone()
    return RoutingRuleResponse(**dict(row))


@router.put("/rules/{rule_id}", response_model=RoutingRuleResponse)
async def update_rule(rule_id: int, update: RoutingRuleUpdate, request: Request):
    state = _get_state(request)
    updates = {k: v for k, v in update.model_dump(exclude_none=True).items()}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    repo.update_routing_rule(state.db_con, rule_id, **updates)
    cursor = state.db_con.execute("SELECT * FROM routing_rules WHERE id = ?", (rule_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Rule not found")
    return RoutingRuleResponse(**dict(row))


@router.delete("/rules/{rule_id}", status_code=204)
async def delete_rule(rule_id: int, request: Request):
    state = _get_state(request)
    repo.delete_routing_rule(state.db_con, rule_id)

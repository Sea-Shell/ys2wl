import logging
from typing import List
from fastapi import APIRouter, HTTPException, Request
from ys2wl.api.models import RoutingRuleCreate, RoutingRuleUpdate, RoutingRuleResponse
from ys2wl.db import repository as repo

log = logging.getLogger("ys2wl.api.rules")
router = APIRouter()


def _get_state(request: Request):
    return request.app.state.ys2wl


@router.get("/rules", response_model=List[RoutingRuleResponse])
async def list_rules(request: Request):
    state = _get_state(request)
    rules = repo.get_routing_rules(state.db_con)
    log.info("list_rules: found %d rules", len(rules))
    for r in rules:
        log.info("  rule id=%d name=%s enabled=%s", r["id"], r["name"], r["enabled"])
    return [RoutingRuleResponse(**r) for r in rules]


@router.post("/rules", response_model=RoutingRuleResponse, status_code=201)
async def create_rule(rule: RoutingRuleCreate, request: Request):
    state = _get_state(request)
    log.info(
        "Creating rule: name=%s field=%s operator=%s pattern=%s playlist=%s",
        rule.name,
        rule.field,
        rule.operator,
        rule.pattern,
        rule.destination_playlist_id,
    )
    rid = repo.create_routing_rule(
        state.db_con,
        rule.name,
        rule.priority,
        rule.field,
        rule.operator,
        rule.pattern,
        rule.destination_playlist_id,
        rule.destination_playlist_title,
        minimum_length=rule.minimum_length,
        maximum_length=rule.maximum_length,
        catch_all=rule.catch_all,
    )
    if rid is None:
        log.error("Failed to create rule in database")
        raise HTTPException(status_code=500, detail="Failed to create rule")
    log.info("Rule created with id=%d", rid)
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
    log.info("Updating rule id=%d updates=%s", rule_id, updates)
    repo.update_routing_rule(state.db_con, rule_id, **updates)
    cursor = state.db_con.execute(
        "SELECT * FROM routing_rules WHERE id = ?", (rule_id,)
    )
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Rule not found")
    return RoutingRuleResponse(**dict(row))


@router.delete("/rules/{rule_id}", status_code=204)
async def delete_rule(rule_id: int, request: Request):
    state = _get_state(request)
    log.info("Deleting rule id=%d", rule_id)
    repo.delete_routing_rule(state.db_con, rule_id)
    log.info("Rule id=%d deleted", rule_id)

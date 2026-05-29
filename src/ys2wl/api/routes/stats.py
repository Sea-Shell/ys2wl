import logging
from typing import List
from fastapi import APIRouter, Request
from ys2wl.api.models import SubscriptionStat

log = logging.getLogger("ys2wl.api.stats")
router = APIRouter()


def _get_state(request: Request):
    return request.app.state.ys2wl


@router.get("/stats/subscriptions", response_model=List[SubscriptionStat])
async def get_subscription_stats(request: Request):
    state = _get_state(request)
    con = state.db_con

    try:
        rows = con.execute(
            """
            SELECT subscription_title,
                   COUNT(*) FILTER (WHERE action = 'added') AS videos_added,
                   MAX(CASE WHEN action = 'added' THEN created_at END) AS last_added_at,
                   COUNT(*) AS total_decisions
            FROM pipeline_run_decisions
            GROUP BY subscription_title
            ORDER BY videos_added DESC
            """
        ).fetchall()
    except Exception as e:
        log.error("Failed to query pipeline_run_decisions: %s", e)
        return []

    active_set = set()
    try:
        for r in con.execute(
            "SELECT DISTINCT title FROM subscription WHERE title IS NOT NULL"
        ):
            active_set.add(r[0])
    except Exception as e:
        log.error("Failed to query subscriptions: %s", e)

    ignore_path = state.settings.subscription_ignore_file
    ignored_set = set()
    try:
        with open(ignore_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    ignored_set.add(line)
    except (FileNotFoundError, OSError):
        pass

    result = []
    for row in rows:
        title = row[0]
        if title in active_set:
            status = "active"
        elif title in ignored_set:
            status = "ignored"
        else:
            status = "inactive"
        result.append(
            SubscriptionStat(
                subscription_title=title,
                videos_added=row[1] or 0,
                last_added_at=row[2],
                total_decisions=row[3] or 0,
                status=status,
            )
        )
    return result

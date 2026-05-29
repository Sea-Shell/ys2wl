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
            SELECT v.subscriptionId,
                   COALESCE(s.title, v.subscriptionId) AS title,
                   COUNT(v.videoId) AS videos_added
            FROM videos v
            LEFT JOIN subscription s ON v.subscriptionId = s.id
            GROUP BY v.subscriptionId
            ORDER BY videos_added DESC
            """
        ).fetchall()
    except Exception as e:
        log.error("Failed to query video stats: %s", e)
        return []

    if not rows:
        return []

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
        sub_id = row[0]
        title = row[1]
        if title in ignored_set:
            status = "ignored"
        elif _is_in_subscription_table(con, sub_id):
            status = "active"
        else:
            status = "inactive"
        result.append(
            SubscriptionStat(
                subscription_title=title,
                subscription_id=sub_id,
                videos_added=row[2] or 0,
                status=status,
            )
        )
    return result


def _is_in_subscription_table(con, sub_id: str) -> bool:
    try:
        r = con.execute(
            "SELECT 1 FROM subscription WHERE id = ? LIMIT 1", (sub_id,)
        ).fetchone()
        return r is not None
    except Exception:
        return False

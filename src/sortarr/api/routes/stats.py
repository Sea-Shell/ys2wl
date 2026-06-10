import logging
from typing import List
from fastapi import APIRouter, Request
from sortarr.api.models import SubscriptionStat
from sortarr.db.repository import config as repo

log = logging.getLogger("sortarr.api.stats")
router = APIRouter()


def _get_state(request: Request):
    return request.app.state.sortarr


@router.get("/stats/subscriptions", response_model=List[SubscriptionStat])
async def get_subscription_stats(request: Request):
    state = _get_state(request)
    con = state.db_con

    try:
        rows = con.execute(
            """
            SELECT v.subscriptionId,
                   COALESCE(s.title, v.subscriptionId) AS title,
                   COUNT(v.videoId) AS videos_added,
                   MAX(v.timestamp) AS last_added_at,
                   COALESCE(s.added_to_playlist_count, 0) AS added_to_playlist_count
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

    ignored_set = set()
    for entry in repo.get_ignore_entries(state.db_con, "subscription"):
        ignored_set.add(entry["pattern"])

    def _is_ignored(title: str, sub_id: str) -> bool:
        if title in ignored_set or sub_id in ignored_set:
            return True
        for entry in ignored_set:
            if entry in title or title in entry:
                return True
        return False

    result = []
    for row in rows:
        sub_id = row[0]
        title = row[1]
        if _is_ignored(title, sub_id):
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
                last_added_at=row[3],
                status=status,
                added_to_playlist_count=row[4] or 0,
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

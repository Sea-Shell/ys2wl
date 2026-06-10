import sqlite3
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger("sortarr.db.repository.pipeline_runs")

__all__ = [
    "create_pipeline_run",
    "finish_pipeline_run",
    "get_pipeline_runs",
    "get_pipeline_run",
    "insert_run_decisions",
    "get_run_decisions",
    "insert_run_decision",
    "update_pipeline_run_progress",
    "cleanup_old_decisions",
    "get_runs_by_video_id",
]


def create_pipeline_run(
    con: sqlite3.Connection, trigger: str = "scheduled", dry_run: bool = False
) -> Optional[int]:
    now = datetime.now(timezone.utc).isoformat()
    try:
        cursor = con.execute(
            "INSERT INTO pipeline_runs (started_at, status, trigger, dry_run) VALUES (?, 'running', ?, ?)",
            (now, trigger, int(dry_run)),
        )
        con.commit()
        return cursor.lastrowid
    except sqlite3.Error as err:
        log.error("Failed to create pipeline run: %s", err)
        return None


def finish_pipeline_run(con: sqlite3.Connection, run_id: int, summary: dict) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    try:
        con.execute(
            "UPDATE pipeline_runs SET finished_at = ?, status = ?, subscriptions_processed = ?, subscriptions_skipped = ?, "
            "videos_added = ?, videos_skipped = ?, errors = ?, error_message = ?, "
            "pipelines_invoked = ?, pipelines_with_errors = ? WHERE id = ?",
            (
                now,
                summary.get("status", "completed"),
                summary.get("subscriptions_processed", 0),
                summary.get("subscriptions_skipped", 0),
                summary.get("videos_added", 0),
                summary.get("videos_skipped", 0),
                summary.get("errors", 0),
                summary.get("error_message", ""),
                summary.get("pipelines_invoked", 0),
                summary.get("pipelines_with_errors", 0),
                run_id,
            ),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to finish pipeline run: %s", err)
        return False


def get_pipeline_runs(con: sqlite3.Connection, limit: int = 20) -> list[dict]:
    cursor = con.execute(
        "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT ?", (limit,)
    )
    return [dict(row) for row in cursor.fetchall()]


def get_pipeline_run(con: sqlite3.Connection, run_id: int) -> Optional[dict]:
    cursor = con.execute("SELECT * FROM pipeline_runs WHERE id = ?", (run_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def insert_run_decisions(
    con: sqlite3.Connection, run_id: int, decisions: list[dict]
) -> bool:
    try:
        now = datetime.now(timezone.utc).isoformat()
        con.executemany(
            "INSERT INTO pipeline_run_decisions (run_id, video_id, title, subscription_title, action, reason, reason_detail, routed_to, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    run_id,
                    d.get("video_id"),
                    d.get("title"),
                    d.get("subscription_title"),
                    d.get("action"),
                    d.get("reason"),
                    d.get("reason_detail"),
                    d.get("routed_to"),
                    now,
                )
                for d in decisions
            ],
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to insert run decisions: %s", err)
        return False


def get_run_decisions(
    con: sqlite3.Connection, run_id: int, limit: int = 1000
) -> list[dict]:
    cursor = con.execute(
        "SELECT id, video_id, title, subscription_title, action, reason, reason_detail, routed_to, created_at "
        "FROM pipeline_run_decisions WHERE run_id = ? ORDER BY id ASC LIMIT ?",
        (run_id, limit),
    )
    return [dict(row) for row in cursor.fetchall()]


def insert_run_decision(con: sqlite3.Connection, run_id: int, decision: dict) -> bool:
    try:
        now = datetime.now(timezone.utc).isoformat()
        con.execute(
            "INSERT INTO pipeline_run_decisions (run_id, video_id, title, subscription_id, subscription_title, channel_id, action, reason, reason_detail, routed_to, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                decision.get("video_id"),
                decision.get("title"),
                decision.get("subscription_id"),
                decision.get("subscription_title"),
                decision.get("channel_id"),
                decision.get("action"),
                decision.get("reason"),
                decision.get("reason_detail"),
                decision.get("routed_to"),
                now,
            ),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to insert run decision: %s", err)
        return False


def update_pipeline_run_progress(
    con: sqlite3.Connection, run_id: int, summary: dict
) -> bool:
    try:
        con.execute(
            "UPDATE pipeline_runs SET subscriptions_processed = ?, subscriptions_skipped = ?, "
            "videos_added = ?, videos_skipped = ?, errors = ?, "
            "pipelines_invoked = ?, pipelines_with_errors = ? WHERE id = ?",
            (
                summary.get("subscriptions_processed", 0),
                summary.get("subscriptions_skipped", 0),
                summary.get("videos_added", 0),
                summary.get("videos_skipped", 0),
                summary.get("errors", 0),
                summary.get("pipelines_invoked", 0),
                summary.get("pipelines_with_errors", 0),
                run_id,
            ),
        )
        con.commit()
        return True
    except sqlite3.Error as err:
        log.error("Failed to update pipeline run progress: %s", err)
        return False


def cleanup_old_decisions(con: sqlite3.Connection, days: int = 10) -> bool:
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cursor = con.execute(
            "DELETE FROM pipeline_run_decisions WHERE created_at < ?", (cutoff,)
        )
        con.commit()
        deleted = cursor.rowcount
        if deleted > 0:
            log.info("Cleaned up %d old pipeline run decisions", deleted)
        return True
    except sqlite3.Error as err:
        log.error("Failed to cleanup old decisions: %s", err)
        return False


def get_runs_by_video_id(
    con: sqlite3.Connection, video_id: str, limit: int = 50
) -> list[dict]:
    """Get pipeline runs where a specific video appeared."""
    cursor = con.execute(
        """
        SELECT DISTINCT pr.id, pr.started_at, pr.finished_at, pr.status, pr.trigger,
               pr.subscriptions_processed, pr.subscriptions_skipped, pr.videos_added,
               pr.videos_skipped, pr.errors, pr.dry_run
        FROM pipeline_runs pr
        JOIN pipeline_run_decisions prd ON pr.id = prd.run_id
        WHERE prd.video_id = ?
        ORDER BY pr.id DESC
        LIMIT ?
        """,
        (video_id, limit),
    )
    return [dict(row) for row in cursor.fetchall()]

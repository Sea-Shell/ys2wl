import sqlite3
import logging
from datetime import datetime, timezone

log = logging.getLogger("sortarr.db.migrations")

V1_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS videos (
    videoId TEXT NOT NULL PRIMARY KEY,
    timestamp TEXT,
    title TEXT,
    subscriptionId TEXT,
    playlistId TEXT,
    duration_seconds INTEGER,
    route_rule TEXT
);
CREATE TABLE IF NOT EXISTS channel (
    id TEXT NOT NULL PRIMARY KEY,
    title TEXT
);
CREATE TABLE IF NOT EXISTS playlist (
    id TEXT NOT NULL PRIMARY KEY,
    title TEXT
);
CREATE TABLE IF NOT EXISTS subscription (
    id TEXT NOT NULL PRIMARY KEY,
    title TEXT,
    timestamp TEXT
);
CREATE TABLE IF NOT EXISTS last_run (
    id NUMBER NOT NULL PRIMARY KEY,
    timestamp TEXT
);
CREATE TABLE IF NOT EXISTS routing_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    field TEXT,
    operator TEXT NOT NULL DEFAULT 'contains',
    pattern TEXT,
    destination_playlist_id TEXT NOT NULL,
    destination_playlist_title TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    minimum_length TEXT NOT NULL DEFAULT '0s',
    maximum_length TEXT NOT NULL DEFAULT '0s',
    catch_all INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    subscriptions_processed INTEGER DEFAULT 0,
    subscriptions_skipped INTEGER DEFAULT 0,
    videos_added INTEGER DEFAULT 0,
    videos_skipped INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    error_message TEXT,
    trigger TEXT DEFAULT 'scheduled',
    pipelines_invoked INTEGER DEFAULT 0,
    pipelines_with_errors INTEGER DEFAULT 0,
    dry_run INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS pipeline_run_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES pipeline_runs(id),
    video_id TEXT,
    title TEXT,
    subscription_id TEXT,
    subscription_title TEXT,
    channel_id TEXT,
    action TEXT NOT NULL,
    reason TEXT,
    reason_detail TEXT,
    routed_to TEXT,
    pipeline_id TEXT,
    pipeline_name TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS app_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ignore_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL CHECK(type IN ('subscription', 'video', 'words')),
    pattern TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ignore_type ON ignore_entries(type);
DELETE FROM app_config WHERE key IN ('minimum_length', 'maximum_length');
"""

V2_PIPELINE_SQL = """
CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    selector_mode TEXT NOT NULL DEFAULT 'AND',
    duration_min_seconds INTEGER NOT NULL DEFAULT 0,
    duration_max_seconds INTEGER NOT NULL DEFAULT 0,
    check_db_exists INTEGER NOT NULL DEFAULT 0,
    check_title_similarity INTEGER NOT NULL DEFAULT 0,
    compare_distance INTEGER NOT NULL DEFAULT 80,
    subscription_scope TEXT NOT NULL DEFAULT 'all',
    destination_playlist_id TEXT NOT NULL,
    destination_playlist_title TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ignore_lists (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    list_type TEXT NOT NULL CHECK(list_type IN ('word', 'video', 'subscription')),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ignore_list_entries (
    id TEXT PRIMARY KEY,
    ignore_list_id TEXT NOT NULL REFERENCES ignore_lists(id) ON DELETE CASCADE,
    value TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ile_ignore_list_id ON ignore_list_entries(ignore_list_id);

CREATE TABLE IF NOT EXISTS pipeline_ignore_lists (
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    ignore_list_id TEXT NOT NULL REFERENCES ignore_lists(id) ON DELETE CASCADE,
    PRIMARY KEY (pipeline_id, ignore_list_id)
);

CREATE TABLE IF NOT EXISTS pipeline_subscriptions (
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    subscription_id TEXT NOT NULL,
    PRIMARY KEY (pipeline_id, subscription_id)
);

CREATE TABLE IF NOT EXISTS pipeline_selectors (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    field TEXT NOT NULL,
    operator TEXT NOT NULL,
    pattern TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ps_pipeline_id ON pipeline_selectors(pipeline_id);

CREATE TABLE IF NOT EXISTS pipeline_subscription_tracking (
    pipeline_id TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    last_processed TEXT,
    PRIMARY KEY (pipeline_id, subscription_id)
);

CREATE TABLE IF NOT EXISTS activity_cache (
    subscription_id TEXT NOT NULL,
    video_id TEXT NOT NULL,
    title TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    channel_title TEXT NOT NULL,
    video_type TEXT,
    description TEXT,
    published_at TEXT,
    cached_at TEXT NOT NULL,
    PRIMARY KEY (subscription_id, video_id)
);
"""


def _run_migration_safe(con: sqlite3.Connection, sql: str) -> bool:
    try:
        con.execute(sql)
        return True
    except sqlite3.OperationalError:
        return False


def init_db(db_path: str) -> bool:
    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        con.executescript(V1_SCHEMA_SQL)
        # Legacy routing_rules column migration
        for stmt in [
            "ALTER TABLE routing_rules ADD COLUMN minimum_length TEXT NOT NULL DEFAULT '0s'",
            "ALTER TABLE routing_rules ADD COLUMN maximum_length TEXT NOT NULL DEFAULT '0s'",
            "ALTER TABLE routing_rules ADD COLUMN catch_all INTEGER NOT NULL DEFAULT 0",
        ]:
            _run_migration_safe(con, stmt)
        # V2: pipeline columns on existing tables
        for stmt in [
            "ALTER TABLE pipeline_runs ADD COLUMN pipelines_invoked INTEGER DEFAULT 0",
            "ALTER TABLE pipeline_runs ADD COLUMN pipelines_with_errors INTEGER DEFAULT 0",
            "ALTER TABLE pipeline_run_decisions ADD COLUMN pipeline_id TEXT",
            "ALTER TABLE pipeline_run_decisions ADD COLUMN pipeline_name TEXT",
            "ALTER TABLE videos ADD COLUMN pipeline_id TEXT",
        ]:
            _run_migration_safe(con, stmt)
        # V2: new tables
        con.executescript(V2_PIPELINE_SQL)
        # V3: dry_run column
        _run_migration_safe(
            con,
            "ALTER TABLE pipeline_runs ADD COLUMN dry_run INTEGER NOT NULL DEFAULT 0",
        )
        # V4: per-selector combine_operator
        _run_migration_safe(
            con,
            "ALTER TABLE pipeline_selectors ADD COLUMN combine_operator TEXT NOT NULL DEFAULT 'AND'",
        )
        # V5: add subscription_id and channel_id to pipeline_run_decisions
        for stmt in [
            "ALTER TABLE pipeline_run_decisions ADD COLUMN subscription_id TEXT",
            "ALTER TABLE pipeline_run_decisions ADD COLUMN channel_id TEXT",
        ]:
            _run_migration_safe(con, stmt)
        # Migrate ignore_entries → ignore_lists if not done
        rows = con.execute("SELECT COUNT(*) as cnt FROM ignore_lists").fetchone()
        if rows["cnt"] == 0:
            _migrate_v1_ignores(con)
        # Migrate routing_rules → pipelines if not done
        rows = con.execute("SELECT COUNT(*) as cnt FROM pipelines").fetchone()
        if rows["cnt"] == 0:
            _migrate_v1_rules(con)
        con.commit()
        con.close()
        return True
    except sqlite3.Error as err:
        log.error("Failed to initialize database: %s", err)
        return False


def _migrate_v1_ignores(con: sqlite3.Connection) -> None:
    now = datetime.now(timezone.utc).isoformat()
    type_map = {"subscription": "subscription", "video": "video", "words": "word"}
    for v1_type, v2_type in type_map.items():
        entries = con.execute(
            "SELECT pattern FROM ignore_entries WHERE type = ?", (v1_type,)
        ).fetchall()
        if not entries:
            continue
        list_id = f"migrated_{v1_type}"
        con.execute(
            "INSERT OR IGNORE INTO ignore_lists (id, name, list_type, created_at) VALUES (?, ?, ?, ?)",
            (list_id, f"Migrated {v1_type} list", v2_type, now),
        )
        for row in entries:
            eid = f"{list_id}_{abs(hash(row['pattern']))}"
            con.execute(
                "INSERT OR IGNORE INTO ignore_list_entries (id, ignore_list_id, value, created_at) VALUES (?, ?, ?, ?)",
                (eid, list_id, row["pattern"], now),
            )


def _migrate_v1_rules(con: sqlite3.Connection) -> None:
    now = datetime.now(timezone.utc).isoformat()
    import re

    def parse_duration(d: str) -> int:
        m = re.match(r"(\d+)s", d)
        return int(m.group(1)) if m else 0

    rows = con.execute("SELECT * FROM routing_rules").fetchall()
    for row in rows:
        pid = f"migrated_rule_{row['id']}"
        min_sec = parse_duration(
            row["minimum_length"] if row["minimum_length"] else "0s"
        )
        max_sec = parse_duration(
            row["maximum_length"] if row["maximum_length"] else "0s"
        )
        con.execute(
            "INSERT OR IGNORE INTO pipelines (id, name, enabled, selector_mode, duration_min_seconds, duration_max_seconds, "
            "check_db_exists, check_title_similarity, compare_distance, subscription_scope, "
            "destination_playlist_id, destination_playlist_title, created_at, updated_at) "
            "VALUES (?, ?, ?, 'AND', ?, ?, 1, 1, 80, 'all', ?, ?, ?, ?)",
            (
                pid,
                row["name"],
                row["enabled"],
                min_sec,
                max_sec,
                row["destination_playlist_id"],
                row["destination_playlist_title"]
                if row["destination_playlist_title"]
                else "",
                row["created_at"],
                now,
            ),
        )
        # Create selector if rule has field/pattern
        if row["field"] and row["pattern"]:
            sid = f"{pid}_sel_1"
            con.execute(
                "INSERT OR IGNORE INTO pipeline_selectors (id, pipeline_id, field, operator, pattern, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (sid, pid, row["field"], row["operator"], row["pattern"], now),
            )
        # Link all migrated ignore lists
        for list_type in ("subscription", "video", "word"):
            lid = f"migrated_{list_type}"
            con.execute(
                "INSERT OR IGNORE INTO pipeline_ignore_lists (pipeline_id, ignore_list_id) VALUES (?, ?)",
                (pid, lid),
            )
        # Initialize tracking for all subscriptions
        subs = con.execute("SELECT id FROM subscription").fetchall()
        for sub in subs:
            ts = con.execute(
                "SELECT timestamp FROM subscription WHERE id = ?", (sub["id"],)
            ).fetchone()
            last_ts = ts["timestamp"] if ts and ts["timestamp"] else None
            con.execute(
                "INSERT OR IGNORE INTO pipeline_subscription_tracking (pipeline_id, subscription_id, last_processed) VALUES (?, ?, ?)",
                (pid, sub["id"], last_ts),
            )
        # Backfill existing videos
        con.execute(
            "UPDATE videos SET pipeline_id = ? WHERE route_rule = ? OR (pipeline_id IS NULL AND route_rule IS NOT NULL)",
            (pid, row["name"]),
        )


def clear_activity_cache(con: sqlite3.Connection) -> None:
    con.execute("DELETE FROM activity_cache")
    con.commit()

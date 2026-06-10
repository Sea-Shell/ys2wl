import logging
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("sortarr.playlist_tracker")


class PlaylistTracker:
    def __init__(self, youtube_client: Any, db_con: Any, channel_id: str):
        self.youtube = youtube_client
        self.db_con = db_con
        self.channel_id = channel_id

    def _get_playlists(self) -> list[dict]:
        pipeline_ids = set()
        rows = self.db_con.execute(
            "SELECT destination_playlist_id FROM pipelines"
        ).fetchall()
        for row in rows:
            pipeline_ids.add(row["destination_playlist_id"])

        playlists = self.youtube.get_user_playlists(self.channel_id)
        excluded = 0
        result = []
        for p in playlists:
            if p.id in pipeline_ids:
                excluded += 1
                continue
            title_lower = p.title.lower()
            if "liked videos" in title_lower or "watch later" in title_lower:
                excluded += 1
                continue
            result.append({"id": p.id, "title": p.title})
        log.info("Found %d playlists (%d excluded)", len(result), excluded)
        return result

    def _get_playlist_items(self, playlist_id: str) -> list[dict]:
        items = self.youtube.get_playlist(playlist_id)
        result = []
        for item in items:
            snippet = item.get("snippet", {})
            resource_id = snippet.get("resourceId", {})
            video_id = resource_id.get("videoId", "")
            if not video_id:
                continue
            result.append(
                {
                    "video_id": video_id,
                    "channel_id": snippet.get("videoOwnerChannelId", ""),
                    "title": snippet.get("title", ""),
                    "published_at": snippet.get("publishedAt", ""),
                }
            )
        return result

    def _increment_count(
        self, video_subscription_id: str, video_id: str, source_playlist_id: str
    ) -> None:
        self.db_con.execute(
            "UPDATE playlist_video_tracking SET counted = 1 WHERE video_id = ? AND source_playlist_id = ?",
            (video_id, source_playlist_id),
        )
        self.db_con.execute(
            "UPDATE subscription SET added_to_playlist_count = COALESCE(added_to_playlist_count, 0) + 1 WHERE id = ?",
            (video_subscription_id,),
        )
        self.db_con.commit()

    def _process_video(
        self, video_id: str, channel_id: str, source_playlist_id: str
    ) -> bool:
        now = datetime.now(timezone.utc).isoformat()

        existing = self.db_con.execute(
            "SELECT counted FROM playlist_video_tracking WHERE video_id = ? AND source_playlist_id = ?",
            (video_id, source_playlist_id),
        ).fetchone()

        if existing:
            if existing["counted"] == 1:
                log.info(
                    "Video %s already counted for playlist %s",
                    video_id,
                    source_playlist_id,
                )
                return False
            video = self.db_con.execute(
                "SELECT subscriptionId FROM videos WHERE videoId = ? LIMIT 1",
                (video_id,),
            ).fetchone()
            if video:
                self._increment_count(
                    video["subscriptionId"], video_id, source_playlist_id
                )
                log.info(
                    "Upgraded video %s to counted=1 for playlist %s",
                    video_id,
                    source_playlist_id,
                )
                return True
            return False

        video = self.db_con.execute(
            "SELECT subscriptionId FROM videos WHERE videoId = ? LIMIT 1",
            (video_id,),
        ).fetchone()

        if video:
            self.db_con.execute(
                "INSERT INTO playlist_video_tracking (video_id, source_playlist_id, counted, created_at) VALUES (?, ?, 1, ?)",
                (video_id, source_playlist_id, now),
            )
            self._increment_count(video["subscriptionId"], video_id, source_playlist_id)
            log.info(
                "Video %s newly counted for playlist %s", video_id, source_playlist_id
            )
            return True
        else:
            self.db_con.execute(
                "INSERT INTO playlist_video_tracking (video_id, source_playlist_id, counted, created_at) VALUES (?, ?, 0, ?)",
                (video_id, source_playlist_id, now),
            )
            self.db_con.commit()
            log.info(
                "Video %s not in videos table, tracked as uncounted for playlist %s",
                video_id,
                source_playlist_id,
            )
            return False

    def run(self) -> dict:
        log.info("Starting playlist tracking run for channel %s", self.channel_id)
        playlists_count = 0
        videos_found = 0
        videos_newly_counted = 0
        subscriptions_set = set()

        try:
            playlists = self._get_playlists()
        except Exception as e:
            log.warning("Failed to get playlists: %s", e)
            return {
                "playlists_processed": 0,
                "videos_found": 0,
                "videos_newly_counted": 0,
                "subscriptions_updated": 0,
            }

        for pl in playlists:
            playlists_count += 1
            try:
                items = self._get_playlist_items(pl["id"])
            except Exception as e:
                log.warning("Failed to get items for playlist %s: %s", pl["id"], e)
                continue
            for item in items:
                videos_found += 1
                try:
                    if self._process_video(
                        item["video_id"], item["channel_id"], pl["id"]
                    ):
                        videos_newly_counted += 1
                        if item["channel_id"]:
                            subscriptions_set.add(item["channel_id"])
                except Exception as e:
                    log.warning(
                        "Failed to process video %s in playlist %s: %s",
                        item["video_id"],
                        pl["id"],
                        e,
                    )

        summary = {
            "playlists_processed": playlists_count,
            "videos_found": videos_found,
            "videos_newly_counted": videos_newly_counted,
            "subscriptions_updated": len(subscriptions_set),
        }
        log.info("Playlist tracking run complete: %s", summary)
        return summary

import logging
from datetime import datetime, timezone
from typing import Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

log = logging.getLogger("sortarr.scheduler")


class PipelineScheduler:
    def __init__(
        self,
        cron_expression: str,
        pipeline_fn: Callable,
        playlist_tracker_cron: str | None = None,
        playlist_tracker_fn: Callable | None = None,
    ):
        self.cron_expression = cron_expression
        self.pipeline_fn = pipeline_fn
        self.playlist_tracker_cron = playlist_tracker_cron
        self.playlist_tracker_fn = playlist_tracker_fn
        self.scheduler = AsyncIOScheduler()

    def start(self) -> None:
        trigger = CronTrigger.from_crontab(self.cron_expression)
        self.scheduler.add_job(
            self.pipeline_fn, trigger, id="pipeline", name="YouTube Pipeline"
        )
        if self.playlist_tracker_cron and self.playlist_tracker_fn:
            pt_trigger = CronTrigger.from_crontab(self.playlist_tracker_cron)
            self.scheduler.add_job(
                self.playlist_tracker_fn,
                pt_trigger,
                id="playlist_tracker",
                name="Playlist Video Tracker",
            )
        self.scheduler.start()
        log.info("Scheduler started with cron: %s", self.cron_expression)
        if self.playlist_tracker_cron:
            log.info(
                "Playlist tracker scheduled with cron: %s",
                self.playlist_tracker_cron,
            )

    async def run_once(self) -> None:
        log.info(
            "Manual pipeline trigger at %s", datetime.now(timezone.utc).isoformat()
        )
        await self.pipeline_fn()

    async def run_playlist_tracker_once(self) -> None:
        if self.playlist_tracker_fn:
            log.info(
                "Manual playlist tracker trigger at %s",
                datetime.now(timezone.utc).isoformat(),
            )
            await self.playlist_tracker_fn()

    @property
    def next_run_time(self) -> str | None:
        pipeline_job = self.scheduler.get_job("pipeline")
        tracker_job = self.scheduler.get_job("playlist_tracker")
        times = []
        if pipeline_job and pipeline_job.next_run_time:
            times.append(pipeline_job.next_run_time)
        if tracker_job and tracker_job.next_run_time:
            times.append(tracker_job.next_run_time)
        if not times:
            return None
        earliest = min(times)
        return earliest.isoformat()

    def stop(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            log.info("Scheduler stopped")

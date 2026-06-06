import logging
from datetime import datetime, timezone
from typing import Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

log = logging.getLogger("sortarr.scheduler")


class PipelineScheduler:
    def __init__(self, cron_expression: str, pipeline_fn: Callable):
        self.cron_expression = cron_expression
        self.pipeline_fn = pipeline_fn
        self.scheduler = AsyncIOScheduler()

    def start(self) -> None:
        trigger = CronTrigger.from_crontab(self.cron_expression)
        self.scheduler.add_job(
            self.pipeline_fn, trigger, id="pipeline", name="YouTube Pipeline"
        )
        self.scheduler.start()
        log.info("Scheduler started with cron: %s", self.cron_expression)

    async def run_once(self) -> None:
        log.info(
            "Manual pipeline trigger at %s", datetime.now(timezone.utc).isoformat()
        )
        await self.pipeline_fn()

    @property
    def next_run_time(self) -> str | None:
        job = self.scheduler.get_job("pipeline")
        if job and job.next_run_time:
            return job.next_run_time.isoformat()
        return None

    def stop(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            log.info("Scheduler stopped")

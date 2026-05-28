import pytest
from ys2wl.core.scheduler import PipelineScheduler


@pytest.mark.asyncio
async def test_scheduler_start_stop():
    called = False

    async def fake_pipeline():
        nonlocal called
        called = True

    scheduler = PipelineScheduler("*/5 * * * *", fake_pipeline)
    assert not scheduler.scheduler.running
    scheduler.start()
    assert scheduler.scheduler.running
    scheduler.stop()


@pytest.mark.asyncio
async def test_run_once_invokes_pipeline():
    called = False

    async def fake_pipeline():
        nonlocal called
        called = True

    scheduler = PipelineScheduler("0 */2 * * *", fake_pipeline)
    await scheduler.run_once()
    assert called

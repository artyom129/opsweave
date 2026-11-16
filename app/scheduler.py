from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from .engine import WorkflowEngine
from .repository import Repository

logger = logging.getLogger("opsweave.scheduler")


class Scheduler:
    def __init__(self, repo: Repository, engine: WorkflowEngine) -> None:
        self.repo = repo
        self.engine = engine
        self._task: asyncio.Task[Any] | None = None
        self._stopped = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopped.clear()
            self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        while not self._stopped.is_set():
            try:
                self.tick()
            except Exception:
                # One bad tick (a locked database, a broken row) must not stop
                # every scheduled workflow until the next restart.
                logger.exception("Scheduler tick failed; retrying on the next tick")
            await asyncio.sleep(5)

    def tick(self, now: datetime | None = None) -> list[int]:
        now = now or datetime.now(timezone.utc)
        started_runs: list[int] = []
        for workflow in self.repo.list_workflows():
            if not workflow["is_enabled"] or workflow["trigger_type"] != "schedule" or not workflow.get("schedule_seconds"):
                continue
            last = self.repo.last_run_for_workflow(workflow["id"])
            if last and last["status"] in {"queued", "running"}:
                # A slow run must finish before the next one starts, otherwise
                # runs pile up behind each other.
                continue
            due = True
            if last:
                started = datetime.fromisoformat(last["started_at"])
                elapsed = (now - started).total_seconds()
                due = elapsed >= int(workflow["schedule_seconds"])
            if due:
                run_id = self.repo.create_run(workflow["id"], "schedule", {"scheduled": True})
                self.engine.enqueue(run_id)
                started_runs.append(run_id)
        return started_runs

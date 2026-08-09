from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from .engine import WorkflowEngine
from .repository import Repository


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
            for workflow in self.repo.list_workflows():
                if not workflow["is_enabled"] or workflow["trigger_type"] != "schedule" or not workflow.get("schedule_seconds"):
                    continue
                last = self.repo.last_run_for_workflow(workflow["id"])
                due = True
                if last:
                    started = datetime.fromisoformat(last["started_at"])
                    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
                    due = elapsed >= int(workflow["schedule_seconds"])
                if due:
                    run_id = self.repo.create_run(workflow["id"], "schedule", {"scheduled": True})
                    self.engine.enqueue(run_id)
            await asyncio.sleep(5)

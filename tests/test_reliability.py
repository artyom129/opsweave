from __future__ import annotations

import asyncio
import importlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.crypto import SecretBox
from app.db import Database
from app.engine import WorkflowEngine
from app.models import WorkflowCreate, WorkflowDefinition, WorkflowPatch
from app.repository import Repository
from app.scheduler import Scheduler


def trigger_only() -> WorkflowDefinition:
    return WorkflowDefinition.model_validate({"nodes": [{"id": "t", "type": "trigger", "label": "Trigger"}], "edges": []})


def make_repo(tmp_path: Path) -> Repository:
    return Repository(Database(tmp_path / "test.db"))


class RecordingEngine:
    def __init__(self) -> None:
        self.enqueued: list[int] = []

    def enqueue(self, run_id: int) -> None:
        self.enqueued.append(run_id)


def test_deleting_a_workflow_removes_its_runs_steps_and_dead_letters(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    wf = repo.create_workflow(WorkflowCreate(name="cleanup", definition=trigger_only()))
    run_id = repo.create_run(wf["id"], "manual", {})
    repo.create_step(run_id, "t", "trigger", "failed", 1, {})
    repo.create_dead_letter(run_id, "boom", {})
    assert repo.delete_workflow(wf["id"]) is True
    assert repo.stats()["total_runs"] == 0
    assert repo.db.fetchone("SELECT COUNT(*) c FROM step_runs")["c"] == 0
    assert repo.db.fetchone("SELECT COUNT(*) c FROM dead_letters")["c"] == 0


def test_cycle_is_rejected_when_the_workflow_is_saved(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    cyclic = WorkflowDefinition.model_validate({
        "nodes": [{"id": "a", "type": "set", "label": "A"}, {"id": "b", "type": "set", "label": "B"}],
        "edges": [{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
    })
    with pytest.raises(ValueError, match="cycle"):
        repo.create_workflow(WorkflowCreate(name="loop", definition=cyclic))


def test_trigger_settings_and_unique_slug(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    with pytest.raises(ValueError, match="webhook_slug"):
        repo.create_workflow(WorkflowCreate(name="hook", trigger_type="webhook", definition=trigger_only()))
    with pytest.raises(ValueError, match="schedule_seconds"):
        repo.create_workflow(WorkflowCreate(name="cron", trigger_type="schedule", definition=trigger_only()))
    first = repo.create_workflow(WorkflowCreate(name="hook one", trigger_type="webhook", webhook_slug="orders", definition=trigger_only()))
    with pytest.raises(ValueError, match="already used"):
        repo.create_workflow(WorkflowCreate(name="hook two", trigger_type="webhook", webhook_slug="orders", definition=trigger_only()))
    # Saving a workflow with its own slug is fine.
    assert repo.update_workflow(first["id"], WorkflowPatch(webhook_slug="orders", description="same")) is not None


def test_patch_with_null_definition_keeps_the_definition(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    wf = repo.create_workflow(WorkflowCreate(name="keep", definition=trigger_only()))
    updated = repo.update_workflow(wf["id"], WorkflowPatch.model_validate({"definition": None, "description": "x"}))
    assert updated is not None
    assert updated["definition"]["nodes"][0]["id"] == "t"


def test_broken_stored_definition_fails_the_run_instead_of_hanging(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    wf = repo.create_workflow(WorkflowCreate(name="broken", definition=trigger_only()))
    repo.db.execute("UPDATE workflows SET definition_json=? WHERE id=?", (json.dumps({"nodes": "oops"}), wf["id"]))
    run_id = repo.create_run(wf["id"], "manual", {})
    asyncio.run(WorkflowEngine(repo, SecretBox("k"), tmp_path / "out").execute_run(run_id))
    run = repo.get_run(run_id)
    assert run["status"] == "failed"
    assert "Invalid workflow definition" in run["error"]
    assert len(repo.list_dead_letters()) == 1


def test_secret_stored_with_another_key_gives_a_clear_error(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    repo.upsert_secret("API_TOKEN", SecretBox("old-key").encrypt("t0k3n"))
    definition = WorkflowDefinition.model_validate({
        "nodes": [{"id": "s", "type": "set", "label": "Use secret", "config": {"values": {"token": "${SECRET:API_TOKEN}"}}}],
        "edges": [],
    })
    wf = repo.create_workflow(WorkflowCreate(name="secret", definition=definition))
    run_id = repo.create_run(wf["id"], "manual", {})
    asyncio.run(WorkflowEngine(repo, SecretBox("new-key"), tmp_path / "out").execute_run(run_id))
    assert "OPSWEAVE_SECRET_KEY" in repo.get_run(run_id)["error"]


def test_interrupted_runs_are_failed_and_sent_to_dead_letters(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    wf = repo.create_workflow(WorkflowCreate(name="restart", definition=trigger_only()))
    queued = repo.create_run(wf["id"], "manual", {"a": 1})
    running = repo.create_run(wf["id"], "manual", {"b": 2})
    repo.set_run_status(running, "running")
    step = repo.create_step(running, "t", "trigger", "running", 1, {})
    done = repo.create_run(wf["id"], "manual", {})
    repo.set_run_status(done, "succeeded", output={})

    assert sorted(repo.recover_interrupted_runs()) == sorted([queued, running])
    assert repo.get_run(queued)["status"] == "failed"
    assert repo.get_run(done)["status"] == "succeeded"
    assert repo.db.fetchone("SELECT status FROM step_runs WHERE id=?", (step,))["status"] == "failed"
    payloads = sorted(json.dumps(item["payload"]) for item in repo.list_dead_letters())
    assert payloads == ['{"a": 1}', '{"b": 2}']


def test_dead_letter_can_be_replayed_only_once(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    wf = repo.create_workflow(WorkflowCreate(name="replay", definition=trigger_only()))
    run_id = repo.create_run(wf["id"], "manual", {"x": 1})
    repo.create_dead_letter(run_id, "boom", {"x": 1})
    item = repo.list_dead_letters()[0]
    first = repo.replay_dead_letter(item["id"], wf["id"], item["payload"])
    assert first is not None
    assert repo.replay_dead_letter(item["id"], wf["id"], item["payload"]) is None
    assert repo.get_dead_letter(item["id"])["replayed_run_id"] == first


def test_scheduler_waits_for_the_previous_run_and_survives_errors(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    wf = repo.create_workflow(WorkflowCreate(name="every minute", trigger_type="schedule", schedule_seconds=60, definition=trigger_only()))
    engine = RecordingEngine()
    scheduler = Scheduler(repo, engine)  # type: ignore[arg-type]

    first = scheduler.tick()
    assert len(first) == 1
    # Still queued and long overdue: no second run is stacked on top.
    assert scheduler.tick(datetime.now(timezone.utc) + timedelta(minutes=10)) == []
    repo.set_run_status(first[0], "succeeded", output={})
    assert scheduler.tick() == []
    assert len(scheduler.tick(datetime.now(timezone.utc) + timedelta(seconds=61))) == 1
    assert wf["id"]

    calls = {"n": 0}

    def flaky_tick(now=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("database is locked")
        scheduler._stopped.set()
        return []

    scheduler.tick = flaky_tick  # type: ignore[method-assign]

    async def run_loop() -> None:
        original_sleep = asyncio.sleep

        async def fast_sleep(_seconds: float) -> None:
            await original_sleep(0)

        asyncio.sleep = fast_sleep  # type: ignore[assignment]
        try:
            await scheduler._loop()
        finally:
            asyncio.sleep = original_sleep  # type: ignore[assignment]

    asyncio.run(run_loop())
    assert calls["n"] == 2


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OPSWEAVE_DB_PATH", str(tmp_path / "api.db"))
    monkeypatch.delenv("OPSWEAVE_WRITE_API_KEY", raising=False)
    import app.config
    import app.main

    importlib.reload(app.config)
    main = importlib.reload(app.main)
    with TestClient(main.app) as test_client:
        yield test_client, main


def test_web_pages_render(client) -> None:
    test_client, main = client
    wf = main.REPO.create_workflow(WorkflowCreate(name="page flow", definition=trigger_only()))
    run_id = main.REPO.create_run(wf["id"], "manual", {})
    for path in ["/", "/workflows", f"/workflows/{wf['id']}", f"/runs/{run_id}", "/dead-letters", "/audit", "/secrets"]:
        response = test_client.get(path)
        assert response.status_code == 200, path


def test_api_returns_400_and_409_instead_of_500(client) -> None:
    test_client, main = client
    created = test_client.post("/api/workflows", json={
        "name": "hook", "trigger_type": "webhook", "webhook_slug": "orders",
        "definition": {"nodes": [{"id": "t", "type": "trigger", "label": "T"}], "edges": []},
    })
    assert created.status_code == 200
    duplicate = test_client.post("/api/workflows", json={
        "name": "hook 2", "trigger_type": "webhook", "webhook_slug": "orders",
        "definition": {"nodes": [{"id": "t", "type": "trigger", "label": "T"}], "edges": []},
    })
    assert duplicate.status_code == 400
    cyclic = test_client.patch(f"/api/workflows/{created.json()['id']}", json={"definition": {
        "nodes": [{"id": "a", "type": "set", "label": "A"}, {"id": "b", "type": "set", "label": "B"}],
        "edges": [{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
    }})
    assert cyclic.status_code == 400

    run_id = main.REPO.create_run(created.json()["id"], "manual", {})
    main.REPO.set_run_status(run_id, "failed", error="x")
    main.REPO.create_dead_letter(run_id, "x", {})
    dead_letter_id = main.REPO.list_dead_letters()[0]["id"]
    assert test_client.post(f"/api/dead-letters/{dead_letter_id}/replay").status_code == 200
    assert test_client.post(f"/api/dead-letters/{dead_letter_id}/replay").status_code == 409

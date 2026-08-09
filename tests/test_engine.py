from __future__ import annotations

import asyncio
from pathlib import Path

from app.crypto import SecretBox
from app.db import Database
from app.engine import WorkflowEngine
from app.models import WorkflowCreate, WorkflowDefinition
from app.repository import Repository


def make_repo(tmp_path: Path) -> Repository:
    return Repository(Database(tmp_path / "test.db"))


def test_successful_branching_run(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    definition = WorkflowDefinition.model_validate({
        "nodes": [
            {"id":"t","type":"trigger","label":"Trigger"},
            {"id":"x","type":"transform","label":"Normalize","config":{"operations":[{"source":"$.amount","target":"amount","op":"float"}]}},
            {"id":"c","type":"condition","label":"High?","config":{"field":"amount","op":"gte","value":100}},
            {"id":"yes","type":"set","label":"Yes","config":{"values":{"route":"high"}}},
            {"id":"no","type":"set","label":"No","config":{"values":{"route":"low"}}}
        ],
        "edges":[
            {"source":"t","target":"x"},
            {"source":"x","target":"c"},
            {"source":"c","target":"yes","when":True},
            {"source":"c","target":"no","when":False}
        ]
    })
    wf = repo.create_workflow(WorkflowCreate(name="test flow", definition=definition))
    engine = WorkflowEngine(repo, SecretBox("test"), tmp_path / "out")
    run_id = repo.create_run(wf["id"], "manual", {"amount":"125"})
    asyncio.run(engine.execute_run(run_id))
    run = repo.get_run(run_id)
    assert run is not None
    assert run["status"] == "succeeded"
    assert run["output"]["route"] == "high"
    assert any(step["status"] == "skipped" for step in run["steps"])


def test_validation_failure_creates_dead_letter(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    definition = WorkflowDefinition.model_validate({
        "nodes":[
            {"id":"t","type":"trigger","label":"Trigger"},
            {"id":"v","type":"validate","label":"Validate","retries":1,"config":{"required":["email"],"rules":[{"field":"email","regex":"^[^@]+@[^@]+$"}]}}
        ],
        "edges":[{"source":"t","target":"v"}]
    })
    wf = repo.create_workflow(WorkflowCreate(name="failure flow", definition=definition))
    engine = WorkflowEngine(repo, SecretBox("test"), tmp_path / "out")
    run_id = repo.create_run(wf["id"], "manual", {"email":"bad"})
    asyncio.run(engine.execute_run(run_id))
    run = repo.get_run(run_id)
    assert run is not None and run["status"] == "failed"
    assert len(repo.list_dead_letters()) == 1
    failed_attempts = [s for s in run["steps"] if s["node_id"] == "v"]
    assert len(failed_attempts) == 2


def test_secret_box_round_trip() -> None:
    box = SecretBox("local-master-key")
    encrypted = box.encrypt("super-secret-token")
    assert encrypted != "super-secret-token"
    assert box.decrypt(encrypted) == "super-secret-token"

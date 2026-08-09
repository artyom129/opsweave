from pathlib import Path

from app.db import Database
from app.models import WorkflowCreate, WorkflowDefinition, WorkflowPatch
from app.repository import Repository


def definition() -> WorkflowDefinition:
    return WorkflowDefinition.model_validate({"nodes":[{"id":"t","type":"trigger","label":"Trigger"}],"edges":[]})


def test_workflow_crud(tmp_path: Path) -> None:
    repo = Repository(Database(tmp_path / "crud.db"))
    created = repo.create_workflow(WorkflowCreate(name="demo flow", description="x", definition=definition()))
    assert created["name"] == "demo flow"
    updated = repo.update_workflow(created["id"], WorkflowPatch(description="updated", is_enabled=False))
    assert updated is not None
    assert updated["description"] == "updated"
    assert updated["is_enabled"] is False
    assert repo.delete_workflow(created["id"]) is True
    assert repo.get_workflow(created["id"]) is None

def test_idempotency_key_round_trip(tmp_path: Path) -> None:
    repo = Repository(Database(tmp_path / "idem.db"))
    created = repo.create_workflow(WorkflowCreate(name="idempotent flow", definition=definition()))
    run_id = repo.create_run(created["id"], "webhook", {"x": 1})
    repo.store_idempotency_key(created["id"], "evt-123", run_id)
    assert repo.get_idempotent_run(created["id"], "evt-123") == run_id

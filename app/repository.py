from __future__ import annotations

import json
from typing import Any

from .db import Database, utcnow
from .models import WorkflowCreate, WorkflowPatch


class Repository:
    def __init__(self, db: Database) -> None:
        self.db = db

    @staticmethod
    def _decode_workflow(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        item = dict(row)
        item["is_enabled"] = bool(item["is_enabled"])
        item["definition"] = json.loads(item.pop("definition_json"))
        return item

    @staticmethod
    def _decode_run(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        item = dict(row)
        for source, target in [("input_json", "input"), ("output_json", "output")]:
            raw = item.pop(source, None)
            item[target] = json.loads(raw) if raw else None
        return item

    @staticmethod
    def _decode_step(row: dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        for source, target in [("input_json", "input"), ("output_json", "output")]:
            raw = item.pop(source, None)
            item[target] = json.loads(raw) if raw else None
        return item

    def create_workflow(self, data: WorkflowCreate) -> dict[str, Any]:
        data.definition.validate_graph()
        now = utcnow()
        workflow_id = self.db.execute(
            """INSERT INTO workflows(name, description, trigger_type, webhook_slug, schedule_seconds,
               is_enabled, definition_json, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                data.name,
                data.description,
                data.trigger_type,
                data.webhook_slug,
                data.schedule_seconds,
                1 if data.is_enabled else 0,
                data.definition.model_dump_json(),
                now,
                now,
            ),
        )
        self.db.audit("workflow.created", "workflow", workflow_id, {"name": data.name})
        return self.get_workflow(workflow_id)  # type: ignore[return-value]

    def update_workflow(self, workflow_id: int, patch: WorkflowPatch) -> dict[str, Any] | None:
        current = self.get_workflow(workflow_id)
        if not current:
            return None
        values = patch.model_dump(exclude_unset=True)
        if "definition" in values and patch.definition is not None:
            patch.definition.validate_graph()
            values["definition_json"] = patch.definition.model_dump_json()
            values.pop("definition")
        if "is_enabled" in values:
            values["is_enabled"] = 1 if values["is_enabled"] else 0
        if not values:
            return current
        values["updated_at"] = utcnow()
        sets = ", ".join(f"{key}=?" for key in values)
        self.db.execute(f"UPDATE workflows SET {sets} WHERE id=?", tuple(values.values()) + (workflow_id,))
        self.db.audit("workflow.updated", "workflow", workflow_id, {"fields": list(values.keys())})
        return self.get_workflow(workflow_id)

    def get_workflow(self, workflow_id: int) -> dict[str, Any] | None:
        return self._decode_workflow(self.db.fetchone("SELECT * FROM workflows WHERE id=?", (workflow_id,)))

    def get_workflow_by_slug(self, slug: str) -> dict[str, Any] | None:
        return self._decode_workflow(self.db.fetchone("SELECT * FROM workflows WHERE webhook_slug=?", (slug,)))

    def list_workflows(self) -> list[dict[str, Any]]:
        return [self._decode_workflow(row) for row in self.db.fetchall("SELECT * FROM workflows ORDER BY updated_at DESC")]  # type: ignore[list-item]

    def delete_workflow(self, workflow_id: int) -> bool:
        if not self.get_workflow(workflow_id):
            return False
        self.db.execute("DELETE FROM workflows WHERE id=?", (workflow_id,))
        self.db.audit("workflow.deleted", "workflow", workflow_id, {})
        return True

    def create_run(self, workflow_id: int, trigger_type: str, payload: dict[str, Any]) -> int:
        run_id = self.db.execute(
            "INSERT INTO runs(workflow_id,status,trigger_type,input_json,started_at) VALUES(?,?,?,?,?)",
            (workflow_id, "queued", trigger_type, json.dumps(payload), utcnow()),
        )
        self.db.audit("run.queued", "run", run_id, {"workflow_id": workflow_id, "trigger_type": trigger_type})
        return run_id

    def set_run_status(self, run_id: int, status: str, output: dict[str, Any] | None = None, error: str | None = None) -> None:
        finished = utcnow() if status in {"succeeded", "failed", "cancelled"} else None
        self.db.execute(
            "UPDATE runs SET status=?, output_json=?, error=?, finished_at=COALESCE(?,finished_at) WHERE id=?",
            (status, json.dumps(output) if output is not None else None, error, finished, run_id),
        )

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        run = self._decode_run(self.db.fetchone("SELECT * FROM runs WHERE id=?", (run_id,)))
        if not run:
            return None
        run["steps"] = [self._decode_step(row) for row in self.db.fetchall("SELECT * FROM step_runs WHERE run_id=? ORDER BY id", (run_id,))]
        return run

    def list_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.db.fetchall(
            """SELECT r.*, w.name workflow_name FROM runs r JOIN workflows w ON w.id=r.workflow_id
               ORDER BY r.id DESC LIMIT ?""",
            (limit,),
        )
        return [self._decode_run(row) for row in rows]  # type: ignore[list-item]

    def last_run_for_workflow(self, workflow_id: int) -> dict[str, Any] | None:
        return self._decode_run(self.db.fetchone("SELECT * FROM runs WHERE workflow_id=? ORDER BY id DESC LIMIT 1", (workflow_id,)))

    def create_step(self, run_id: int, node_id: str, node_type: str, status: str, attempt: int, payload: dict[str, Any] | None = None) -> int:
        return self.db.execute(
            """INSERT INTO step_runs(run_id,node_id,node_type,status,attempt,input_json,started_at)
               VALUES(?,?,?,?,?,?,?)""",
            (run_id, node_id, node_type, status, attempt, json.dumps(payload) if payload is not None else None, utcnow()),
        )

    def finish_step(self, step_id: int, status: str, output: dict[str, Any] | None, error: str | None, input_hash: str | None, output_hash: str | None, duration_ms: int) -> None:
        self.db.execute(
            """UPDATE step_runs SET status=?, output_json=?, error=?, input_hash=?, output_hash=?, duration_ms=?, finished_at=?
               WHERE id=?""",
            (status, json.dumps(output) if output is not None else None, error, input_hash, output_hash, duration_ms, utcnow(), step_id),
        )

    def create_dead_letter(self, run_id: int, reason: str, payload: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO dead_letters(run_id,reason,payload_json,created_at) VALUES(?,?,?,?)",
            (run_id, reason, json.dumps(payload), utcnow()),
        )
        self.db.audit("dead_letter.created", "run", run_id, {"reason": reason})

    def list_dead_letters(self) -> list[dict[str, Any]]:
        rows = self.db.fetchall(
            """SELECT d.*, w.name workflow_name FROM dead_letters d
               JOIN runs r ON r.id=d.run_id JOIN workflows w ON w.id=r.workflow_id
               ORDER BY d.id DESC"""
        )
        for row in rows:
            row["payload"] = json.loads(row.pop("payload_json"))
        return rows

    def resolve_dead_letter(self, dead_letter_id: int, replayed_run_id: int) -> None:
        self.db.execute(
            "UPDATE dead_letters SET replayed_run_id=?, resolved_at=? WHERE id=?",
            (replayed_run_id, utcnow(), dead_letter_id),
        )

    def get_dead_letter(self, dead_letter_id: int) -> dict[str, Any] | None:
        row = self.db.fetchone("SELECT * FROM dead_letters WHERE id=?", (dead_letter_id,))
        if not row:
            return None
        row["payload"] = json.loads(row.pop("payload_json"))
        return row

    def upsert_secret(self, name: str, ciphertext: str) -> None:
        now = utcnow()
        existing = self.db.fetchone("SELECT id FROM secrets WHERE name=?", (name,))
        if existing:
            self.db.execute("UPDATE secrets SET ciphertext=?, updated_at=? WHERE name=?", (ciphertext, now, name))
            action = "secret.updated"
        else:
            self.db.execute("INSERT INTO secrets(name,ciphertext,created_at,updated_at) VALUES(?,?,?,?)", (name, ciphertext, now, now))
            action = "secret.created"
        self.db.audit(action, "secret", name, {})

    def get_secret_ciphertext(self, name: str) -> str | None:
        row = self.db.fetchone("SELECT ciphertext FROM secrets WHERE name=?", (name,))
        return str(row["ciphertext"]) if row else None

    def list_secrets(self) -> list[dict[str, Any]]:
        return self.db.fetchall("SELECT name, created_at, updated_at FROM secrets ORDER BY name")


    def list_audit_logs(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.db.fetchall("SELECT * FROM audit_logs ORDER BY id DESC LIMIT ?", (limit,))
        for row in rows:
            row["details"] = json.loads(row.pop("details_json"))
        return rows

    def get_idempotent_run(self, workflow_id: int, key: str) -> int | None:
        row = self.db.fetchone("SELECT run_id FROM idempotency_keys WHERE workflow_id=? AND key=?", (workflow_id, key))
        return int(row["run_id"]) if row else None

    def store_idempotency_key(self, workflow_id: int, key: str, run_id: int) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO idempotency_keys(workflow_id,key,run_id,created_at) VALUES(?,?,?,?)",
            (workflow_id, key, run_id, utcnow()),
        )

    def stats(self) -> dict[str, Any]:
        total = self.db.fetchone("SELECT COUNT(*) c FROM runs") or {"c": 0}
        failed = self.db.fetchone("SELECT COUNT(*) c FROM runs WHERE status='failed'") or {"c": 0}
        success = self.db.fetchone("SELECT COUNT(*) c FROM runs WHERE status='succeeded'") or {"c": 0}
        active = self.db.fetchone("SELECT COUNT(*) c FROM workflows WHERE is_enabled=1") or {"c": 0}
        avg = self.db.fetchone("SELECT AVG(duration_ms) avg_ms FROM step_runs WHERE status='succeeded'") or {"avg_ms": 0}
        return {
            "total_runs": total["c"],
            "failed_runs": failed["c"],
            "successful_runs": success["c"],
            "active_workflows": active["c"],
            "avg_step_ms": round(avg["avg_ms"] or 0),
            "success_rate": round((success["c"] / total["c"] * 100), 1) if total["c"] else 0,
        }

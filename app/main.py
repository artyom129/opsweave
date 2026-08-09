from __future__ import annotations

import asyncio
import csv
import io
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import settings
from .crypto import SecretBox
from .db import Database
from .engine import WorkflowEngine
from .models import RunRequest, SecretCreate, WorkflowCreate, WorkflowPatch
from .repository import Repository
from .scheduler import Scheduler


BASE_DIR = Path(__file__).resolve().parent
DB = Database(settings.db_path)
REPO = Repository(DB)
SECRET_BOX = SecretBox(settings.secret_key)
ENGINE = WorkflowEngine(REPO, SECRET_BOX, Path("data/outputs"))
SCHEDULER = Scheduler(REPO, ENGINE)
TEMPLATES = Jinja2Templates(directory=str(BASE_DIR / "templates"))


async def require_write_key(x_opsweave_key: str | None = Header(default=None)) -> None:
    if settings.write_api_key and x_opsweave_key != settings.write_api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing X-OpsWeave-Key")


@asynccontextmanager
async def lifespan(_: FastAPI):
    SCHEDULER.start()
    yield
    await SCHEDULER.stop()


app = FastAPI(
    title="OpsWeave",
    description="Local-first workflow orchestration, reliability and data automation platform.",
    version="1.0.0",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "service": "opsweave", "version": "1.0.0"}


@app.get("/api/stats")
def stats() -> dict[str, Any]:
    return REPO.stats()


@app.get("/api/workflows")
def list_workflows() -> list[dict[str, Any]]:
    return REPO.list_workflows()


@app.post("/api/workflows", dependencies=[Depends(require_write_key)])
def create_workflow(data: WorkflowCreate) -> dict[str, Any]:
    try:
        return REPO.create_workflow(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/workflows/{workflow_id}")
def get_workflow(workflow_id: int) -> dict[str, Any]:
    workflow = REPO.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return workflow


@app.patch("/api/workflows/{workflow_id}", dependencies=[Depends(require_write_key)])
def patch_workflow(workflow_id: int, patch: WorkflowPatch) -> dict[str, Any]:
    workflow = REPO.update_workflow(workflow_id, patch)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return workflow


@app.delete("/api/workflows/{workflow_id}", dependencies=[Depends(require_write_key)])
def delete_workflow(workflow_id: int) -> dict[str, bool]:
    return {"deleted": REPO.delete_workflow(workflow_id)}


@app.post("/api/workflows/{workflow_id}/run", dependencies=[Depends(require_write_key)])
async def run_workflow(workflow_id: int, request: RunRequest) -> dict[str, Any]:
    workflow = REPO.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    run_id = REPO.create_run(workflow_id, "manual", request.payload)
    ENGINE.enqueue(run_id)
    return {"run_id": run_id, "status": "queued"}


@app.post("/api/webhooks/{slug}")
async def webhook(slug: str, payload: dict[str, Any], x_idempotency_key: str | None = Header(default=None)) -> dict[str, Any]:
    workflow = REPO.get_workflow_by_slug(slug)
    if not workflow or not workflow["is_enabled"]:
        raise HTTPException(status_code=404, detail="Webhook workflow not found")
    if x_idempotency_key:
        existing = REPO.get_idempotent_run(workflow["id"], x_idempotency_key)
        if existing is not None:
            return {"accepted": True, "run_id": existing, "deduplicated": True}
    run_id = REPO.create_run(workflow["id"], "webhook", payload)
    if x_idempotency_key:
        REPO.store_idempotency_key(workflow["id"], x_idempotency_key, run_id)
    ENGINE.enqueue(run_id)
    return {"accepted": True, "run_id": run_id, "deduplicated": False}


@app.get("/api/runs")
def list_runs(limit: int = 100) -> list[dict[str, Any]]:
    return REPO.list_runs(min(limit, 500))


@app.get("/api/runs/{run_id}")
def get_run(run_id: int) -> dict[str, Any]:
    run = REPO.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@app.post("/api/runs/{run_id}/replay", dependencies=[Depends(require_write_key)])
async def replay_run(run_id: int) -> dict[str, Any]:
    run = REPO.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    new_run_id = REPO.create_run(run["workflow_id"], "replay", run["input"] or {})
    ENGINE.enqueue(new_run_id)
    return {"run_id": new_run_id, "replayed_from": run_id}


@app.get("/api/dead-letters")
def dead_letters() -> list[dict[str, Any]]:
    return REPO.list_dead_letters()


@app.post("/api/dead-letters/{dead_letter_id}/replay", dependencies=[Depends(require_write_key)])
async def replay_dead_letter(dead_letter_id: int) -> dict[str, Any]:
    item = REPO.get_dead_letter(dead_letter_id)
    if not item:
        raise HTTPException(status_code=404, detail="Dead letter not found")
    run = REPO.get_run(item["run_id"])
    if not run:
        raise HTTPException(status_code=404, detail="Original run not found")
    new_run_id = REPO.create_run(run["workflow_id"], "dead-letter-replay", item["payload"])
    REPO.resolve_dead_letter(dead_letter_id, new_run_id)
    ENGINE.enqueue(new_run_id)
    return {"run_id": new_run_id}


@app.get("/api/secrets")
def list_secrets() -> list[dict[str, Any]]:
    return REPO.list_secrets()


@app.post("/api/secrets", dependencies=[Depends(require_write_key)])
def save_secret(secret: SecretCreate) -> dict[str, str]:
    REPO.upsert_secret(secret.name, SECRET_BOX.encrypt(secret.value))
    return {"name": secret.name, "status": "stored"}


@app.get("/api/runs-export.csv")
def export_runs() -> StreamingResponse:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["run_id", "workflow", "status", "trigger", "started_at", "finished_at", "error"])
    for run in REPO.list_runs(500):
        writer.writerow([run["id"], run.get("workflow_name"), run["status"], run["trigger_type"], run["started_at"], run["finished_at"], run["error"]])
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=opsweave-runs.csv"})


@app.websocket("/ws/runs/{run_id}")
async def run_events(websocket: WebSocket, run_id: int) -> None:
    await websocket.accept()
    queue = ENGINE.events.subscribe(run_id)
    try:
        current = REPO.get_run(run_id)
        if current:
            await websocket.send_json({"type": "snapshot", "run": current})
        while True:
            event = await queue.get()
            await websocket.send_json(event)
    except WebSocketDisconnect:
        pass
    finally:
        ENGINE.events.unsubscribe(run_id, queue)


@app.get("/api/audit")
def audit_logs(limit: int = 100) -> list[dict[str, Any]]:
    return REPO.list_audit_logs(min(limit, 500))


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return TEMPLATES.TemplateResponse("dashboard.html", {"request": request, "stats": REPO.stats(), "workflows": REPO.list_workflows()[:5], "runs": REPO.list_runs(10)})


@app.get("/workflows", response_class=HTMLResponse)
def workflows_page(request: Request) -> HTMLResponse:
    return TEMPLATES.TemplateResponse("workflows.html", {"request": request, "workflows": REPO.list_workflows()})


@app.get("/workflows/{workflow_id}", response_class=HTMLResponse)
def workflow_page(request: Request, workflow_id: int) -> HTMLResponse:
    workflow = REPO.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    runs = [run for run in REPO.list_runs(100) if run["workflow_id"] == workflow_id][:20]
    return TEMPLATES.TemplateResponse("workflow.html", {"request": request, "workflow": workflow, "runs": runs})


@app.post("/ui/workflows/{workflow_id}/run")
async def ui_run_workflow(workflow_id: int) -> RedirectResponse:
    workflow = REPO.get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    run_id = REPO.create_run(workflow_id, "manual", {"source": "dashboard", "order_id": "DEMO-1042", "supplier": "Northstar Components", "amount": 7425.50, "currency": "USD", "contact_email": " OPS@NORTHSTAR.EXAMPLE "})
    ENGINE.enqueue(run_id)
    return RedirectResponse(url=f"/runs/{run_id}", status_code=303)


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_page(request: Request, run_id: int) -> HTMLResponse:
    run = REPO.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    workflow = REPO.get_workflow(run["workflow_id"])
    return TEMPLATES.TemplateResponse("run.html", {"request": request, "run": run, "workflow": workflow})


@app.get("/dead-letters", response_class=HTMLResponse)
def dead_letters_page(request: Request) -> HTMLResponse:
    return TEMPLATES.TemplateResponse("dead_letters.html", {"request": request, "items": REPO.list_dead_letters()})


@app.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request) -> HTMLResponse:
    return TEMPLATES.TemplateResponse("audit.html", {"request": request, "items": REPO.list_audit_logs(200)})


@app.get("/secrets", response_class=HTMLResponse)
def secrets_page(request: Request) -> HTMLResponse:
    return TEMPLATES.TemplateResponse("secrets.html", {"request": request, "secrets": REPO.list_secrets()})

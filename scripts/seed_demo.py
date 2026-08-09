from __future__ import annotations

from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings
from app.db import Database
from app.models import WorkflowCreate, WorkflowDefinition
from app.repository import Repository


def seed() -> None:
    repo = Repository(Database(settings.db_path))
    if repo.list_workflows():
        print("Workflows already exist; skipping seed.")
        return

    vendor_guard = WorkflowCreate(
        name="Vendor Order Intake Guard",
        description="Webhook-driven supplier order intake with normalization, validation, branching, persistence and recovery.",
        trigger_type="webhook",
        webhook_slug="vendor-orders",
        definition=WorkflowDefinition.model_validate({
            "nodes": [
                {"id":"trigger","type":"trigger","label":"Incoming vendor order"},
                {"id":"normalize","type":"transform","label":"Normalize fields","config":{"operations":[
                    {"source":"$.contact_email","target":"contact_email","op":"lower"},
                    {"source":"$.amount","target":"amount","op":"float"},
                    {"source":"$.currency","target":"currency","op":"upper"}
                ]}},
                {"id":"validate","type":"validate","label":"Validate contract","retries":1,"config":{"required":["order_id","supplier","amount","currency","contact_email"],"rules":[
                    {"field":"contact_email","regex":"^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$","message":"contact_email is not valid"},
                    {"field":"amount","min":0,"message":"amount must be positive"},
                    {"field":"currency","in":["USD","EUR","GBP"],"message":"unsupported currency"}
                ]}},
                {"id":"high_value","type":"condition","label":"High-value order?","config":{"field":"amount","op":"gte","value":5000}},
                {"id":"notify","type":"notify","label":"Flag for finance review","config":{"channel":"finance","message":"High-value vendor order requires approval"}},
                {"id":"standard","type":"set","label":"Mark standard route","config":{"values":{"route":"standard"}}},
                {"id":"persist","type":"json_store","label":"Persist normalized payload","config":{"filename":"vendor-order-latest.json"}}
            ],
            "edges": [
                {"source":"trigger","target":"normalize"},
                {"source":"normalize","target":"validate"},
                {"source":"validate","target":"high_value"},
                {"source":"high_value","target":"notify","when":True},
                {"source":"high_value","target":"standard","when":False},
                {"source":"notify","target":"persist"},
                {"source":"standard","target":"persist"}
            ]
        })
    )
    repo.create_workflow(vendor_guard)

    nightly_quality = WorkflowCreate(
        name="Nightly Data Quality Gate",
        description="Scheduled validation gate that demonstrates automatic execution and persisted audit output.",
        trigger_type="schedule",
        schedule_seconds=3600,
        definition=WorkflowDefinition.model_validate({
            "nodes": [
                {"id":"trigger","type":"trigger","label":"Scheduler tick"},
                {"id":"defaults","type":"set","label":"Build health payload","config":{"values":{"dataset":"orders.csv","rows":12480,"invalid_rows":3,"freshness_minutes":17}}},
                {"id":"freshness","type":"validate","label":"Check freshness","config":{"required":["dataset","rows"],"rules":[{"field":"freshness_minutes","max":60,"message":"dataset freshness SLA breached"}]}},
                {"id":"persist","type":"json_store","label":"Write quality snapshot","config":{"filename":"nightly-quality-latest.json"}}
            ],
            "edges":[
                {"source":"trigger","target":"defaults"},
                {"source":"defaults","target":"freshness"},
                {"source":"freshness","target":"persist"}
            ]
        })
    )
    repo.create_workflow(nightly_quality)

    api_watch = WorkflowCreate(
        name="API Sync Reliability Watch",
        description="Reusable sync watchdog with retry-aware validation and failure capture into the dead-letter queue.",
        trigger_type="manual",
        definition=WorkflowDefinition.model_validate({
            "nodes":[
                {"id":"trigger","type":"trigger","label":"Manual sync check"},
                {"id":"defaults","type":"set","label":"Prepare sync metrics","config":{"values":{"source":"crm-api","records_seen":220,"records_written":220,"lag_seconds":4}}},
                {"id":"validate","type":"validate","label":"Validate sync result","retries":2,"config":{"required":["source","records_seen","records_written"],"rules":[{"field":"lag_seconds","max":30,"message":"sync lag exceeded 30 seconds"}]}},
                {"id":"persist","type":"json_store","label":"Store sync snapshot","config":{"filename":"api-sync-latest.json"}}
            ],
            "edges":[
                {"source":"trigger","target":"defaults"},
                {"source":"defaults","target":"validate"},
                {"source":"validate","target":"persist"}
            ]
        })
    )
    repo.create_workflow(api_watch)
    print("Seeded 3 demo workflows.")


if __name__ == "__main__":
    seed()

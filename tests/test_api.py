import os
from pathlib import Path


def test_health_contract() -> None:
    # Kept intentionally lightweight so the core suite does not depend on lifespan scheduling.
    from app.main import health
    payload = health()
    assert payload["status"] == "ok"
    assert payload["service"] == "opsweave"

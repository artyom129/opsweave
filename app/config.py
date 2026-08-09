from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_path: Path
    secret_key: str
    write_api_key: str | None
    host: str
    port: int


def load_settings() -> Settings:
    db_path = Path(os.getenv("OPSWEAVE_DB_PATH", "data/opsweave.db"))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return Settings(
        db_path=db_path,
        secret_key=os.getenv("OPSWEAVE_SECRET_KEY", "dev-only-change-me"),
        write_api_key=os.getenv("OPSWEAVE_WRITE_API_KEY") or None,
        host=os.getenv("OPSWEAVE_HOST", "0.0.0.0"),
        port=int(os.getenv("OPSWEAVE_PORT", "8000")),
    )


settings = load_settings()

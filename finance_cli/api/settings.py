"""Settings for the Finance CLI FastAPI application."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ApiSettings:
    """Filesystem and database settings for the API application."""

    base_dir: Path
    database_path: Path

    @classmethod
    def from_env(cls) -> "ApiSettings":
        base_dir = Path(os.getenv("FINANCE_CLI_BASE_DIR", ".")).expanduser().resolve(strict=False)
        database_value = Path(os.getenv("FINANCE_CLI_DB_PATH", "state/finance_api.db")).expanduser()
        if not database_value.is_absolute():
            database_value = (base_dir / database_value).resolve(strict=False)
        return cls(base_dir=base_dir, database_path=database_value)

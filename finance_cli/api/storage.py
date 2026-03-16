"""SQLite-backed state store for the Finance CLI API."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from finance_cli.models import DatasetConfig


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SqliteStateStore:
    """Thin repository layer for API metadata and job state."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS datasets (
                    dataset_id TEXT PRIMARY KEY,
                    label TEXT NOT NULL,
                    path TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    refresh_provider TEXT,
                    refresh_symbol TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    indicator_type TEXT NOT NULL,
                    months INTEGER NOT NULL,
                    rule TEXT NOT NULL,
                    status TEXT NOT NULL,
                    output_path TEXT NOT NULL,
                    error_text TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    dataset_id TEXT,
                    status TEXT NOT NULL,
                    output_path TEXT NOT NULL,
                    error_text TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    owner_type TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(owner_type, owner_id)
                );
                """
            )

    def list_datasets(self) -> list[dict[str, Any]]:
        return self._fetchall("SELECT * FROM datasets ORDER BY dataset_id")

    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None:
        return self._fetchone(
            "SELECT * FROM datasets WHERE dataset_id = ?",
            (dataset_id,),
        )

    def upsert_dataset(self, dataset: DatasetConfig, *, source_type: str) -> dict[str, Any]:
        existing = self.get_dataset(dataset.id)
        refresh_provider = None if dataset.refresh is None else dataset.refresh.provider
        refresh_symbol = None if dataset.refresh is None else dataset.refresh.symbol
        created_at = utc_now_iso() if existing is None else str(existing["created_at"])
        updated_at = created_at
        if existing is not None:
            existing_values = (
                existing["label"],
                existing["path"],
                existing["source_type"],
                existing["refresh_provider"],
                existing["refresh_symbol"],
            )
            current_values = (
                dataset.label,
                dataset.path,
                source_type,
                refresh_provider,
                refresh_symbol,
            )
            updated_at = str(existing["updated_at"]) if existing_values == current_values else utc_now_iso()

        self._execute(
            """
            INSERT INTO datasets (
                dataset_id, label, path, source_type, refresh_provider, refresh_symbol, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dataset_id) DO UPDATE SET
                label = excluded.label,
                path = excluded.path,
                source_type = excluded.source_type,
                refresh_provider = excluded.refresh_provider,
                refresh_symbol = excluded.refresh_symbol,
                updated_at = excluded.updated_at
            """,
            (
                dataset.id,
                dataset.label,
                dataset.path,
                source_type,
                refresh_provider,
                refresh_symbol,
                created_at,
                updated_at,
            ),
        )
        return self.get_dataset(dataset.id) or {}

    def delete_dataset(self, dataset_id: str) -> None:
        self._execute("DELETE FROM datasets WHERE dataset_id = ?", (dataset_id,))

    def delete_missing_datasets(self, active_dataset_ids: set[str]) -> None:
        if not active_dataset_ids:
            self._execute("DELETE FROM datasets")
            return
        placeholders = ", ".join("?" for _ in active_dataset_ids)
        self._execute(
            f"DELETE FROM datasets WHERE dataset_id NOT IN ({placeholders})",
            tuple(sorted(active_dataset_ids)),
        )

    def create_run(
        self,
        *,
        run_id: str,
        dataset_id: str,
        indicator_type: str,
        months: int,
        rule: str,
        status: str,
        output_path: str,
        error_text: str = "",
    ) -> dict[str, Any]:
        now = utc_now_iso()
        self._execute(
            """
            INSERT INTO runs (
                run_id, dataset_id, indicator_type, months, rule, status, output_path, error_text, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, dataset_id, indicator_type, months, rule, status, output_path, error_text, now, now),
        )
        return self.get_run(run_id) or {}

    def update_run(self, run_id: str, *, status: str, output_path: str, error_text: str) -> dict[str, Any]:
        self._execute(
            """
            UPDATE runs
            SET status = ?, output_path = ?, error_text = ?, updated_at = ?
            WHERE run_id = ?
            """,
            (status, output_path, error_text, utc_now_iso(), run_id),
        )
        return self.get_run(run_id) or {}

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._fetchone("SELECT * FROM runs WHERE run_id = ?", (run_id,))

    def create_job(
        self,
        *,
        job_id: str,
        job_type: str,
        dataset_id: str | None,
        status: str,
        output_path: str,
        error_text: str = "",
    ) -> dict[str, Any]:
        now = utc_now_iso()
        self._execute(
            """
            INSERT INTO jobs (
                job_id, job_type, dataset_id, status, output_path, error_text, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (job_id, job_type, dataset_id, status, output_path, error_text, now, now),
        )
        return self.get_job(job_id) or {}

    def update_job(self, job_id: str, *, status: str, output_path: str, error_text: str) -> dict[str, Any]:
        self._execute(
            """
            UPDATE jobs
            SET status = ?, output_path = ?, error_text = ?, updated_at = ?
            WHERE job_id = ?
            """,
            (status, output_path, error_text, utc_now_iso(), job_id),
        )
        return self.get_job(job_id) or {}

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        return self._fetchone("SELECT * FROM jobs WHERE job_id = ?", (job_id,))

    def upsert_artifact(
        self,
        *,
        artifact_id: str,
        owner_type: str,
        owner_id: str,
        path: str,
        kind: str,
    ) -> dict[str, Any]:
        existing = self.get_artifact(owner_type=owner_type, owner_id=owner_id)
        created_at = utc_now_iso() if existing is None else str(existing["created_at"])
        self._execute(
            """
            INSERT INTO artifacts (
                artifact_id, owner_type, owner_id, path, kind, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_type, owner_id) DO UPDATE SET
                artifact_id = excluded.artifact_id,
                path = excluded.path,
                kind = excluded.kind
            """,
            (artifact_id, owner_type, owner_id, path, kind, created_at),
        )
        return self.get_artifact(owner_type=owner_type, owner_id=owner_id) or {}

    def get_artifact(self, *, owner_type: str, owner_id: str) -> dict[str, Any] | None:
        return self._fetchone(
            "SELECT * FROM artifacts WHERE owner_type = ? AND owner_id = ?",
            (owner_type, owner_id),
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        with self._connect() as connection:
            connection.execute(query, params)
            connection.commit()

    def _fetchone(self, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(query, params).fetchone()
        return None if row is None else dict(row)

    def _fetchall(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in rows]

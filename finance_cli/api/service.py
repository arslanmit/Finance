"""Application service layer for the Finance CLI API."""

from __future__ import annotations

import io
import shutil
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from fastapi import HTTPException, UploadFile, status

from finance_cli.analysis import AnalysisConfig
from finance_cli.catalog import (
    discover_datasets,
    get_dataset,
    import_dataset,
    remove_dataset,
)
from finance_cli.create import create_symbol_dataset
from finance_cli.errors import CatalogError, CreationError, FinanceCliError
from finance_cli.matrix import build_matrix_jobs, run_matrix_jobs, write_matrix_manifest
from finance_cli.models import DatasetConfig
from finance_cli.refresh import refresh_selected_source
from finance_cli.run_workflow import execute_analysis
from finance_cli.sources import resolve_dataset_source

from .models import (
    CreateDatasetFromSymbolRequest,
    DatasetResponse,
    DeleteDatasetResponse,
    JobResponse,
    RefreshMetadataResponse,
    RunCreateRequest,
    RunResponse,
)
from .settings import ApiSettings
from .storage import SqliteStateStore
from .worker import JobWorker


@dataclass(frozen=True)
class ApiContext:
    """Shared application context for route handlers and worker jobs."""

    settings: ApiSettings
    storage: SqliteStateStore
    worker: JobWorker


def list_datasets(context: ApiContext) -> list[DatasetResponse]:
    datasets = discover_datasets(context.settings.base_dir)
    active_ids = {dataset.id for dataset in datasets}
    context.storage.delete_missing_datasets(active_ids)
    for dataset in datasets:
        source_type = _infer_source_type(dataset, context.storage.get_dataset(dataset.id))
        context.storage.upsert_dataset(dataset, source_type=source_type)
    return [_dataset_response_for_dataset(context, dataset) for dataset in datasets]


def upload_dataset(
    context: ApiContext,
    *,
    upload_file: UploadFile,
    refresh_symbol: str | None,
) -> DatasetResponse:
    filename = Path(upload_file.filename or "").name
    if not filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="A CSV filename is required.")

    upload_root = context.settings.base_dir / "tmp" / "uploads" / uuid4().hex
    upload_root.mkdir(parents=True, exist_ok=True)
    upload_path = upload_root / filename
    upload_path.write_bytes(upload_file.file.read())
    try:
        dataset = import_dataset(
            source_path=upload_path,
            refresh_symbol=refresh_symbol,
            base_dir=context.settings.base_dir,
        )
    except FinanceCliError as exc:
        raise _http_error_from_exception(exc) from exc
    finally:
        shutil.rmtree(upload_root, ignore_errors=True)

    context.storage.upsert_dataset(dataset, source_type="upload")
    return _dataset_response_for_dataset(context, dataset)


def create_dataset_from_symbol(
    context: ApiContext,
    request: CreateDatasetFromSymbolRequest,
) -> DatasetResponse:
    try:
        dataset = create_symbol_dataset(
            request.symbol,
            base_dir=context.settings.base_dir,
        )
    except FinanceCliError as exc:
        raise _http_error_from_exception(exc) from exc

    context.storage.upsert_dataset(dataset, source_type="symbol")
    return _dataset_response_for_dataset(context, dataset)


def delete_dataset_entry(context: ApiContext, dataset_id: str) -> DeleteDatasetResponse:
    _require_dataset(context, dataset_id)
    try:
        remove_dataset(dataset_id, base_dir=context.settings.base_dir)
    except FinanceCliError as exc:
        raise _http_error_from_exception(exc) from exc
    context.storage.delete_dataset(dataset_id)
    return DeleteDatasetResponse(id=dataset_id)


def create_run(context: ApiContext, request: RunCreateRequest) -> RunResponse:
    dataset = _require_dataset(context, request.dataset_id)
    run_id = uuid4().hex
    output_relative_path = Path("output") / "runs" / run_id / f"{dataset.id}_processed.csv"
    context.storage.create_run(
        run_id=run_id,
        dataset_id=dataset.id,
        indicator_type=request.indicator_type,
        months=request.months,
        rule=request.rule,
        status="running",
        output_path=output_relative_path.as_posix(),
    )

    try:
        source = resolve_dataset_source(dataset)
        with redirect_stdout(io.StringIO()):
            execute_analysis(
                source,
                config=AnalysisConfig(
                    months=request.months,
                    indicator_type=request.indicator_type,
                    rule=request.rule,
                ),
                output_path=context.settings.base_dir / output_relative_path,
                refresh_requested=request.refresh_requested,
                backup_dir=context.settings.base_dir / "tmp" / "refresh_backups",
            )
        context.storage.upsert_artifact(
            artifact_id=uuid4().hex,
            owner_type="run",
            owner_id=run_id,
            path=output_relative_path.as_posix(),
            kind="analysis_output",
        )
        context.storage.update_run(
            run_id,
            status="success",
            output_path=output_relative_path.as_posix(),
            error_text="",
        )
    except FinanceCliError as exc:
        context.storage.update_run(
            run_id,
            status="error",
            output_path=output_relative_path.as_posix(),
            error_text=str(exc),
        )
        raise _http_error_from_exception(exc) from exc

    return get_run(context, run_id)


def get_run(context: ApiContext, run_id: str) -> RunResponse:
    row = context.storage.get_run(run_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Run '{run_id}' was not found.")
    artifact = context.storage.get_artifact(owner_type="run", owner_id=run_id)
    return RunResponse(
        id=str(row["run_id"]),
        dataset_id=str(row["dataset_id"]),
        indicator_type=str(row["indicator_type"]),
        months=int(row["months"]),
        rule=str(row["rule"]),
        status=str(row["status"]),
        output_path=str(row["output_path"]),
        error=str(row["error_text"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        artifact_url=None if artifact is None else f"/runs/{run_id}/artifact",
    )


def queue_refresh_job(context: ApiContext, dataset_id: str) -> JobResponse:
    dataset = _require_dataset(context, dataset_id)
    context.storage.upsert_dataset(dataset, source_type=_infer_source_type(dataset, context.storage.get_dataset(dataset.id)))
    job_id = uuid4().hex
    context.storage.create_job(
        job_id=job_id,
        job_type="refresh",
        dataset_id=dataset.id,
        status="queued",
        output_path=dataset.path,
    )
    context.worker.submit(lambda: _run_refresh_job(context, job_id, dataset.id))
    return get_job(context, job_id)


def queue_matrix_job(context: ApiContext) -> JobResponse:
    datasets = discover_datasets(context.settings.base_dir)
    if not datasets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No generated datasets are available for matrix execution.",
        )
    job_id = uuid4().hex
    output_relative_path = (Path("output") / "matrix" / job_id / "manifest.csv").as_posix()
    context.storage.create_job(
        job_id=job_id,
        job_type="matrix",
        dataset_id=None,
        status="queued",
        output_path=output_relative_path,
    )
    context.worker.submit(lambda: _run_matrix_job(context, job_id))
    return get_job(context, job_id)


def get_job(context: ApiContext, job_id: str) -> JobResponse:
    row = context.storage.get_job(job_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' was not found.")
    artifact = context.storage.get_artifact(owner_type="job", owner_id=job_id)
    return JobResponse(
        id=str(row["job_id"]),
        job_type=str(row["job_type"]),
        dataset_id=None if row["dataset_id"] is None else str(row["dataset_id"]),
        status=str(row["status"]),
        output_path=str(row["output_path"]),
        error=str(row["error_text"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        artifact_url=None if artifact is None else f"/jobs/{job_id}/artifact",
    )


def resolve_artifact_path(
    context: ApiContext,
    *,
    owner_type: str,
    owner_id: str,
) -> Path:
    artifact = context.storage.get_artifact(owner_type=owner_type, owner_id=owner_id)
    if artifact is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Artifact was not found.")

    artifact_path = context.settings.base_dir / str(artifact["path"])
    if not artifact_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Artifact file was not found.")
    return artifact_path


def _dataset_response_for_dataset(context: ApiContext, dataset: DatasetConfig) -> DatasetResponse:
    row = context.storage.get_dataset(dataset.id)
    if row is None:
        source_type = _infer_source_type(dataset, None)
        row = context.storage.upsert_dataset(dataset, source_type=source_type)
    refresh = None
    if row["refresh_provider"] and row["refresh_symbol"]:
        refresh = RefreshMetadataResponse(
            provider=str(row["refresh_provider"]),
            symbol=str(row["refresh_symbol"]),
        )

    return DatasetResponse(
        id=dataset.id,
        label=dataset.label,
        path=dataset.path,
        source_type=str(row["source_type"]),
        refresh=refresh,
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _require_dataset(context: ApiContext, dataset_id: str) -> DatasetConfig:
    datasets = discover_datasets(context.settings.base_dir)
    try:
        return get_dataset(dataset_id, datasets)
    except CatalogError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset '{dataset_id}' was not found.",
        ) from exc


def _infer_source_type(dataset: DatasetConfig, existing_row: dict[str, object] | None) -> str:
    if existing_row is not None:
        return str(existing_row["source_type"])
    return "symbol" if dataset.supports_refresh else "upload"


def _run_refresh_job(context: ApiContext, job_id: str, dataset_id: str) -> None:
    row = context.storage.get_job(job_id)
    output_path = "" if row is None else str(row["output_path"])
    context.storage.update_job(job_id, status="running", output_path=output_path, error_text="")
    try:
        dataset = _require_dataset(context, dataset_id)
        summary = refresh_selected_source(
            resolve_dataset_source(dataset),
            backup_dir=context.settings.base_dir / "tmp" / "refresh_backups",
        )
        context.storage.upsert_artifact(
            artifact_id=uuid4().hex,
            owner_type="job",
            owner_id=job_id,
            path=dataset.path,
            kind="refreshed_dataset",
        )
        context.storage.update_job(
            job_id,
            status="success",
            output_path=dataset.path,
            error_text="",
        )
        context.storage.upsert_dataset(dataset, source_type=_infer_source_type(dataset, context.storage.get_dataset(dataset.id)))
        _ = summary
    except Exception as exc:
        context.storage.update_job(
            job_id,
            status="error",
            output_path=output_path,
            error_text=str(exc),
        )


def _run_matrix_job(context: ApiContext, job_id: str) -> None:
    row = context.storage.get_job(job_id)
    output_path = "" if row is None else str(row["output_path"])
    context.storage.update_job(job_id, status="running", output_path=output_path, error_text="")
    try:
        datasets = discover_datasets(context.settings.base_dir)
        if not datasets:
            raise ValueError("No generated datasets are available for matrix execution.")
        output_dir = context.settings.base_dir / "output" / "matrix" / job_id
        records = run_matrix_jobs(datasets, build_matrix_jobs(), output_dir)
        manifest_path = write_matrix_manifest(records, output_dir)
        manifest_relative_path = manifest_path.relative_to(context.settings.base_dir).as_posix()
        context.storage.upsert_artifact(
            artifact_id=uuid4().hex,
            owner_type="job",
            owner_id=job_id,
            path=manifest_relative_path,
            kind="matrix_manifest",
        )
        context.storage.update_job(
            job_id,
            status="success",
            output_path=manifest_relative_path,
            error_text="",
        )
    except Exception as exc:
        context.storage.update_job(
            job_id,
            status="error",
            output_path=output_path,
            error_text=str(exc),
        )


def _http_error_from_exception(exc: Exception) -> HTTPException:
    message = str(exc)
    lowered = message.lower()
    if "already exists" in lowered:
        return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=message)
    if "not found" in lowered:
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message)
    if isinstance(exc, (CatalogError, CreationError)):
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    if isinstance(exc, FinanceCliError):
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)
    return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message)

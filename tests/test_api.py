from __future__ import annotations

import io
import sqlite3
import time
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from finance_cli.matrix import MatrixJob


def monthly_frame(row_count: int = 4) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=row_count, freq="MS")
    open_values = [float(10 + index) for index in range(row_count)]
    return pd.DataFrame(
        {
            "date": dates,
            "open": open_values,
            "high": [value + 1.0 for value in open_values],
            "low": [value - 1.0 for value in open_values],
            "close": [value + 0.5 for value in open_values],
            "volume": [100 + index for index in range(row_count)],
        }
    )


def write_dataset(base_dir: Path, dataset_id: str, *, symbol: str | None = None) -> Path:
    dataframe = monthly_frame()
    output = base_dir / "data" / "generated" / f"{dataset_id}.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    if symbol is not None:
        dataframe.insert(0, "symbol", symbol)
    dataframe.to_csv(output, index=False, date_format="%Y-%m-%d")
    return output


def dataframe_to_upload_bytes(*, symbol: str | None = None) -> bytes:
    dataframe = monthly_frame()
    if symbol is not None:
        dataframe.insert(0, "symbol", symbol)
    buffer = io.StringIO()
    dataframe.to_csv(buffer, index=False, date_format="%Y-%m-%d")
    return buffer.getvalue().encode("utf-8")


def fetch_scalar(database_path: Path, query: str, params: tuple[object, ...] = ()) -> object:
    connection = sqlite3.connect(database_path)
    try:
        row = connection.execute(query, params).fetchone()
    finally:
        connection.close()
    return None if row is None else row[0]


def wait_for_job(client: TestClient, job_id: str, *, timeout_seconds: float = 5.0) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    last_payload: dict[str, object] | None = None

    while time.time() < deadline:
        response = client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        last_payload = response.json()
        if last_payload["status"] in {"success", "error"}:
            return last_payload
        time.sleep(0.05)

    raise AssertionError(f"Job {job_id} did not finish in time. Last payload: {last_payload}")


@pytest.fixture
def api_settings(tmp_path: Path):
    from finance_cli.api_app import ApiSettings

    return ApiSettings(
        base_dir=tmp_path,
        database_path=tmp_path / "state" / "finance_api.db",
    )


@pytest.fixture
def client(api_settings):
    from finance_cli.api_app import create_app

    app = create_app(api_settings)
    with TestClient(app) as test_client:
        yield test_client


def test_healthz_reports_ok(client: TestClient) -> None:
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_dataset_upload_list_and_delete_persist_state(
    client: TestClient,
    api_settings,
) -> None:
    upload_response = client.post(
        "/datasets/upload",
        data={"refresh_symbol": "SPY"},
        files={"file": ("custom_upload.csv", dataframe_to_upload_bytes(), "text/csv")},
    )

    assert upload_response.status_code == 201
    created_dataset = upload_response.json()
    assert created_dataset["id"] == "custom_upload"
    assert created_dataset["source_type"] == "upload"
    assert created_dataset["refresh"]["symbol"] == "SPY"
    assert (api_settings.base_dir / created_dataset["path"]).exists()

    list_response = client.get("/datasets")
    assert list_response.status_code == 200
    assert list_response.json() == [created_dataset]

    assert fetch_scalar(
        api_settings.database_path,
        "SELECT source_type FROM datasets WHERE dataset_id = ?",
        ("custom_upload",),
    ) == "upload"

    delete_response = client.delete("/datasets/custom_upload")
    assert delete_response.status_code == 200
    assert delete_response.json()["id"] == "custom_upload"
    assert not (api_settings.base_dir / created_dataset["path"]).exists()
    assert fetch_scalar(
        api_settings.database_path,
        "SELECT COUNT(*) FROM datasets WHERE dataset_id = ?",
        ("custom_upload",),
    ) == 0


def test_create_from_symbol_uses_yahoo_fetcher(monkeypatch, client: TestClient, api_settings) -> None:
    monkeypatch.setattr(
        "finance_cli.create.fetch_full_history_monthly_source",
        lambda symbol: monthly_frame(),
    )

    response = client.post("/datasets/create-from-symbol", json={"symbol": "NVDA"})

    assert response.status_code == 201
    payload = response.json()
    assert payload["id"] == "nvda"
    assert payload["source_type"] == "symbol"
    assert payload["refresh"]["symbol"] == "NVDA"
    assert (api_settings.base_dir / "data" / "generated" / "nvda.csv").exists()


def test_run_endpoint_persists_run_and_serves_artifact(client: TestClient, api_settings) -> None:
    write_dataset(api_settings.base_dir, "spy", symbol="SPY")

    response = client.post(
        "/runs",
        json={"dataset_id": "spy", "months": 2, "indicator_type": "sma", "rule": "indicator > open"},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["dataset_id"] == "spy"
    assert payload["status"] == "success"
    assert payload["artifact_url"] == f"/runs/{payload['id']}/artifact"
    assert payload["output_path"].endswith(".csv")
    assert (api_settings.base_dir / payload["output_path"]).exists()

    persisted = client.get(f"/runs/{payload['id']}")
    assert persisted.status_code == 200
    assert persisted.json() == payload

    artifact_response = client.get(payload["artifact_url"])
    assert artifact_response.status_code == 200
    assert "Moving_Average" in artifact_response.text

    assert fetch_scalar(
        api_settings.database_path,
        "SELECT status FROM runs WHERE run_id = ?",
        (payload["id"],),
    ) == "success"
    assert fetch_scalar(
        api_settings.database_path,
        "SELECT kind FROM artifacts WHERE owner_type = 'run' AND owner_id = ?",
        (payload["id"],),
    ) == "analysis_output"


def test_run_endpoint_returns_not_found_for_unknown_dataset(client: TestClient) -> None:
    response = client.post("/runs", json={"dataset_id": "missing", "months": 2})

    assert response.status_code == 404
    assert response.json()["detail"] == "Dataset 'missing' was not found."


def test_refresh_job_updates_dataset_and_serves_artifact(
    monkeypatch,
    client: TestClient,
    api_settings,
) -> None:
    write_dataset(api_settings.base_dir, "spy", symbol="SPY")
    refreshed_frame = monthly_frame(row_count=5)
    monkeypatch.setattr(
        "finance_cli.refresh.fetch_full_history_monthly_source",
        lambda symbol: refreshed_frame,
    )

    response = client.post("/datasets/spy/refresh")

    assert response.status_code == 202
    payload = response.json()
    assert payload["job_type"] == "refresh"

    completed = wait_for_job(client, payload["id"])
    assert completed["status"] == "success"
    assert completed["artifact_url"] == f"/jobs/{payload['id']}/artifact"

    artifact_response = client.get(completed["artifact_url"])
    assert artifact_response.status_code == 200
    assert "2024-05-01" in artifact_response.text
    assert fetch_scalar(
        api_settings.database_path,
        "SELECT status FROM jobs WHERE job_id = ?",
        (payload["id"],),
    ) == "success"


def test_matrix_job_writes_manifest_and_serves_artifact(
    monkeypatch,
    client: TestClient,
    api_settings,
) -> None:
    write_dataset(api_settings.base_dir, "spy", symbol="SPY")
    monkeypatch.setattr(
        "finance_cli.api.service.build_matrix_jobs",
        lambda: [MatrixJob(months=2, indicator="sma", rule="indicator > open")],
    )

    response = client.post("/jobs/matrix")

    assert response.status_code == 202
    payload = response.json()
    assert payload["job_type"] == "matrix"

    completed = wait_for_job(client, payload["id"])
    assert completed["status"] == "success"
    assert completed["artifact_url"] == f"/jobs/{payload['id']}/artifact"

    artifact_response = client.get(completed["artifact_url"])
    assert artifact_response.status_code == 200
    assert "dataset_id" in artifact_response.text
    assert "status" in artifact_response.text
    assert fetch_scalar(
        api_settings.database_path,
        "SELECT kind FROM artifacts WHERE owner_type = 'job' AND owner_id = ?",
        (payload["id"],),
    ) == "matrix_manifest"

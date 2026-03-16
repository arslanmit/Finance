from __future__ import annotations

import io
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pandas as pd
import pytest


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _sample_csv_bytes() -> bytes:
    dataframe = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
            "open": [10.0, 12.0, 11.0],
            "high": [11.0, 13.0, 12.0],
            "low": [9.0, 11.0, 10.0],
            "close": [10.5, 12.5, 11.5],
            "volume": [100, 110, 120],
        }
    )
    buffer = io.StringIO()
    dataframe.to_csv(buffer, index=False, date_format="%Y-%m-%d")
    return buffer.getvalue().encode("utf-8")


def test_docker_assets_exist() -> None:
    assert Path("Dockerfile").exists()
    assert Path("docker-compose.yml").exists()


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker is not installed")
def test_dockerized_api_smoke(tmp_path: Path) -> None:
    if subprocess.run(
        ["docker", "info"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode != 0:
        pytest.skip("docker daemon is not available")

    compose_file = Path("docker-compose.yml")
    assert compose_file.exists()

    generated_dir = tmp_path / "data" / "generated"
    output_dir = tmp_path / "output"
    tmp_dir = tmp_path / "tmp"
    state_dir = tmp_path / "state"
    generated_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    port = _free_tcp_port()
    project_name = f"finance-api-{uuid4().hex[:8]}"
    environment = os.environ | {
        "COMPOSE_PROJECT_NAME": project_name,
        "FINANCE_API_PORT": str(port),
        "GENERATED_DATA_DIR": str(generated_dir),
        "OUTPUT_DIR": str(output_dir),
        "TMP_DIR": str(tmp_dir),
        "STATE_DIR": str(state_dir),
    }
    compose_command = ["docker", "compose", "-f", str(compose_file)]

    subprocess.run(
        [*compose_command, "up", "-d", "--build"],
        check=True,
        cwd=Path.cwd(),
        env=environment,
    )

    try:
        client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=5.0)
        deadline = time.time() + 60
        last_error: str | None = None
        while time.time() < deadline:
            try:
                health_response = client.get("/healthz")
                if health_response.status_code == 200:
                    break
                last_error = health_response.text
            except httpx.HTTPError as exc:
                last_error = str(exc)
            time.sleep(1)
        else:
            raise AssertionError(f"API did not become healthy in time. Last error: {last_error}")

        upload_response = client.post(
            "/datasets/upload",
            files={"file": ("spy.csv", _sample_csv_bytes(), "text/csv")},
        )
        assert upload_response.status_code == 201
        assert upload_response.json()["id"] == "spy"

        run_response = client.post("/runs", json={"dataset_id": "spy", "months": 2})
        assert run_response.status_code == 201
        artifact_response = client.get(run_response.json()["artifact_url"])
        assert artifact_response.status_code == 200
        assert "Moving_Average" in artifact_response.text
    finally:
        subprocess.run(
            [*compose_command, "down", "-v", "--remove-orphans"],
            check=False,
            cwd=Path.cwd(),
            env=environment,
        )

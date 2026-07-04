from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
LAUNCHER_PATH = ROOT / "scripts" / "nvidia-codex"


def test_nvidia_codex_launcher_loads_key_and_forwards_arguments(tmp_path: Path) -> None:
    assert LAUNCHER_PATH.exists(), "NVIDIA Codex launcher is missing"
    assert os.access(LAUNCHER_PATH, os.X_OK), "NVIDIA Codex launcher is not executable"

    project_root = _copy_launcher_project(tmp_path)
    (project_root / ".env").write_text("NVIDIA_API_KEY=test-key\n", encoding="utf-8")
    capture_dir = tmp_path / "capture"
    capture_dir.mkdir()
    fake_bin = _write_fake_codex(tmp_path)
    environment = os.environ | {
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "CAPTURE_DIR": str(capture_dir),
    }

    result = subprocess.run(
        [str(project_root / "scripts" / "nvidia-codex"), "exec", "review this repo"],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert (capture_dir / "key").read_text(encoding="utf-8") == "test-key\n"
    assert (capture_dir / "args").read_text(encoding="utf-8").splitlines() == [
        "-c",
        'model="nvidia/nemotron-3-super-120b-a12b"',
        "-c",
        'model_provider="nvidia"',
        "-c",
        'model_providers.nvidia.name="NVIDIA NIM"',
        "-c",
        'model_providers.nvidia.base_url="https://integrate.api.nvidia.com/v1"',
        "-c",
        'model_providers.nvidia.env_key="NVIDIA_API_KEY"',
        "exec",
        "review this repo",
    ]
    assert Path((capture_dir / "cwd").read_text(encoding="utf-8").strip()) == project_root


def test_nvidia_codex_launcher_rejects_missing_key(tmp_path: Path) -> None:
    assert LAUNCHER_PATH.exists(), "NVIDIA Codex launcher is missing"

    project_root = _copy_launcher_project(tmp_path)
    (project_root / ".env").write_text("OTHER_VALUE=set\n", encoding="utf-8")

    result = subprocess.run(
        [str(project_root / "scripts" / "nvidia-codex")],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "NVIDIA_API_KEY" in result.stderr


def _copy_launcher_project(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True)
    copied_launcher = scripts_dir / "nvidia-codex"
    shutil.copy2(LAUNCHER_PATH, copied_launcher)
    return project_root


def _write_fake_codex(tmp_path: Path) -> Path:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_codex = fake_bin / "codex"
    fake_codex.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$NVIDIA_API_KEY\" > \"$CAPTURE_DIR/key\"\n"
        "printf '%s\\n' \"$@\" > \"$CAPTURE_DIR/args\"\n"
        "pwd > \"$CAPTURE_DIR/cwd\"\n",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    return fake_bin

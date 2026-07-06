from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "opencode.json"
LAUNCHER_PATH = ROOT / "scripts" / "nvidia-agent"
SMOKE_PATH = ROOT / "scripts" / "smoke-nvidia-agent"
LEGACY_LAUNCHER_PATH = ROOT / "scripts" / "nvidia-codex"
README_PATH = ROOT / "README.md"
MODEL_ID = "nvidia/nemotron-3-super-120b-a12b"
MODEL_SELECTOR = f"nvidia-nim/{MODEL_ID}"


def test_opencode_config_uses_nvidia_nim_with_least_privilege() -> None:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

    assert config["model"] == MODEL_SELECTOR
    assert config["small_model"] == MODEL_SELECTOR

    provider = config["provider"]["nvidia-nim"]
    assert provider["npm"] == "@ai-sdk/openai-compatible"
    assert provider["options"]["baseURL"] == "https://integrate.api.nvidia.com/v1"
    assert provider["options"]["apiKey"] == "{env:NVIDIA_API_KEY}"

    model = provider["models"][MODEL_ID]
    assert model["limit"] == {"context": 1_000_000, "output": 32_768}
    assert model["options"]["chat_template_kwargs"] == {
        "force_nonempty_content": True,
    }

    assert config["agent"]["build"] == {
        "temperature": 1.0,
        "top_p": 0.95,
        "max_tokens": 32_000,
    }
    assert config["agent"]["plan"] == config["agent"]["build"]

    permission = config["permission"]
    assert permission["read"] == "allow"
    assert permission["glob"] == "allow"
    assert permission["grep"] == "allow"
    assert permission["list"] == "allow"
    assert permission["edit"] == "ask"
    assert permission["external_directory"] == "deny"
    assert permission["webfetch"] == "deny"
    assert permission["websearch"] == "deny"

    bash = permission["bash"]
    assert bash["*"] == "ask"
    assert bash["pwd"] == "allow"
    assert bash["python3 dynamic_range_average.py run --dataset spy --months 6"] == "allow"
    assert bash["rm *"] == "deny"
    assert bash["git push *"] == "deny"
    assert bash["git reset --hard*"] == "deny"


def test_nvidia_agent_launcher_loads_key_and_forwards_arguments(tmp_path: Path) -> None:
    assert LAUNCHER_PATH.exists(), "NVIDIA OpenCode launcher is missing"
    assert os.access(LAUNCHER_PATH, os.X_OK), "NVIDIA OpenCode launcher is not executable"

    project_root = _copy_launcher_project(tmp_path)
    (project_root / ".env").write_text("NVIDIA_API_KEY=test-key\n", encoding="utf-8")
    capture_dir = tmp_path / "capture"
    capture_dir.mkdir()
    fake_opencode = _write_capture_executable(tmp_path, "opencode")
    environment = os.environ | {
        "CAPTURE_DIR": str(capture_dir),
        "NVIDIA_OPENCODE_BIN": str(fake_opencode),
    }

    result = subprocess.run(
        [str(project_root / "scripts" / "nvidia-agent"), "run", "review this repo"],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert (capture_dir / "key").read_text(encoding="utf-8") == "test-key\n"
    assert (capture_dir / "args").read_text(encoding="utf-8").splitlines() == [
        "run",
        "review this repo",
    ]
    assert Path((capture_dir / "cwd").read_text(encoding="utf-8").strip()) == project_root


def test_nvidia_agent_launcher_uses_pinned_opencode_package(tmp_path: Path) -> None:
    project_root = _copy_launcher_project(tmp_path)
    (project_root / ".env").write_text("NVIDIA_API_KEY=test-key\n", encoding="utf-8")
    capture_dir = tmp_path / "capture"
    capture_dir.mkdir()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _write_capture_executable(fake_bin, "npx")
    environment = {
        key: value
        for key, value in os.environ.items()
        if key != "NVIDIA_OPENCODE_BIN"
    } | {
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "CAPTURE_DIR": str(capture_dir),
    }

    result = subprocess.run(
        [str(project_root / "scripts" / "nvidia-agent"), "run", "say NVIDIA_OK"],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert (capture_dir / "args").read_text(encoding="utf-8").splitlines() == [
        "-y",
        "opencode-ai@1.17.13",
        "run",
        "say NVIDIA_OK",
    ]


def test_nvidia_agent_launcher_rejects_missing_key(tmp_path: Path) -> None:
    project_root = _copy_launcher_project(tmp_path)
    (project_root / ".env").write_text("OTHER_VALUE=set\n", encoding="utf-8")

    result = subprocess.run(
        [str(project_root / "scripts" / "nvidia-agent")],
        check=False,
        capture_output=True,
        text=True,
        env={key: value for key, value in os.environ.items() if key != "NVIDIA_API_KEY"},
    )

    assert result.returncode != 0
    assert "NVIDIA_API_KEY" in result.stderr


def test_nvidia_agent_smoke_script_covers_real_tool_loop_contract() -> None:
    assert SMOKE_PATH.exists(), "NVIDIA live smoke script is missing"
    assert os.access(SMOKE_PATH, os.X_OK), "NVIDIA live smoke script is not executable"

    script = SMOKE_PATH.read_text(encoding="utf-8")
    assert "--pure --format json --model" in script
    assert MODEL_SELECTOR in script
    assert '"type":"tool_use"' in script
    assert "NVIDIA_OK" in script
    assert "PWD_OK" in script
    assert "TOOL_LOOP_OK" in script
    assert "python3 dynamic_range_average.py run --dataset spy --months 6" in script
    assert "output/spy_processed.csv" in script
    assert "EDIT_OK" in script
    assert "tmp/nvidia-agent-smoke.txt" in script
    assert "PUSH_DENIED_OK" in script
    assert "git push origin HEAD" in script
    assert "NVIDIA_AGENT_LAUNCHER" in script
    assert "NVIDIA_AGENT_SMOKE_DIR" in script


def test_readme_documents_nvidia_agent_and_legacy_launcher_warns() -> None:
    readme = README_PATH.read_text(encoding="utf-8")
    assert "./scripts/nvidia-agent" in readme
    assert './scripts/nvidia-agent run "' in readme
    assert "./scripts/smoke-nvidia-agent" in readme
    assert "NVIDIA_API_KEY" in readme
    assert "opencode-ai@1.17.13" in readme

    legacy_launcher = LEGACY_LAUNCHER_PATH.read_text(encoding="utf-8")
    assert "deprecated" in legacy_launcher.lower()
    assert "scripts/nvidia-agent" in legacy_launcher


def _copy_launcher_project(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True)
    shutil.copy2(LAUNCHER_PATH, scripts_dir / "nvidia-agent")
    return project_root


def _write_capture_executable(parent: Path, name: str) -> Path:
    executable = parent / name
    executable.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$NVIDIA_API_KEY\" > \"$CAPTURE_DIR/key\"\n"
        "printf '%s\\n' \"$@\" > \"$CAPTURE_DIR/args\"\n"
        "pwd > \"$CAPTURE_DIR/cwd\"\n",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return executable

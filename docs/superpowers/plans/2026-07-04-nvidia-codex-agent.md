# NVIDIA Codex Agent Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repo-local launcher that runs Codex with an NVIDIA Build model and the ignored `.env` credential.

**Architecture:** Pass provider metadata as Codex CLI configuration overrides because custom providers are not accepted from project-local config. Load the secret at runtime through a shell launcher, then delegate all agent behavior to the installed Codex CLI.

**Tech Stack:** Codex CLI, TOML, POSIX shell, Python, pytest

---

## Chunk 1: Provider And Launcher

### Task 1: Lock the provider and launcher contract

**Files:**
- Create: `tests/test_nvidia_agent.py`
- Create: `scripts/nvidia-codex`

- [x] **Step 1: Write failing tests**

Execute the launcher with a fake `codex` command to require the NVIDIA provider
name, endpoint, environment-key name, model ID, secret loading, and exact user
argument forwarding. Add a missing-key error test.

- [x] **Step 2: Verify RED**

Run: `.venv/bin/python -m pytest tests/test_nvidia_agent.py -q`

Expected: failure because the launcher does not exist.

- [x] **Step 3: Add minimal implementation**

Create `scripts/nvidia-codex` with strict error handling, `.env` loading, custom
provider CLI overrides, and `exec codex ... "$@"` delegation.

- [x] **Step 4: Verify GREEN**

Run: `.venv/bin/python -m pytest tests/test_nvidia_agent.py -q`

Expected: all tests pass.

- [x] **Step 5: Run regression checks**

Run: `.venv/bin/python -m pytest -q --ignore=tests/test_repo_csv_only.py`

Expected: existing functional suite passes; the known repository-hygiene failures
remain outside this change.

- [x] **Step 6: Verify the live credential and hand off**

Load `.env` and call `https://integrate.api.nvidia.com/v1/models` without printing
the key. Report the launcher command and any limitation encountered when nesting
Codex inside the current Codex Desktop process.

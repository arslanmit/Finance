# NVIDIA OpenCode Agent Implementation Plan

> **For Codex:** Execute this plan task by task with test-driven development and verify every completion claim.

**Goal:** Run `nvidia/nemotron-3-super-120b-a12b` reliably as a tool-using coding agent in this repository through OpenCode's native Chat Completions integration.

**Architecture:** A repository-local `opencode.json` defines the NVIDIA NIM provider, model, and least-privilege permissions. A small POSIX launcher loads the repository `.env`, pins `opencode-ai@1.17.13`, starts from the repository root, and forwards CLI arguments. Contract tests cover launcher behavior and configuration; a separate opt-in smoke test verifies the real NVIDIA endpoint and tool loop without making network calls part of the default test suite.

**Tech Stack:** POSIX shell, OpenCode 1.17.13, NVIDIA NIM Chat Completions, pytest, Python standard library.

---

### Task 1: Lock down the local configuration contract

**Files:**
- Create: `tests/test_nvidia_opencode.py`
- Create: `opencode.json`

1. Add a failing test that loads `opencode.json` and asserts the pinned NVIDIA provider, API base URL, environment-key reference, model selector, model limits, and least-privilege permission rules.
2. Run `pytest tests/test_nvidia_opencode.py -q` and confirm it fails because the configuration is missing.
3. Add the smallest valid `opencode.json` that satisfies the contract. Permit read-only repository tools and narrowly allow `pwd` plus the Finance six-month SPY command; ask for general shell/edit access and deny destructive Git/filesystem commands, external directories, and web access.
4. Re-run the focused test and confirm it passes.

### Task 2: Build the pinned repository launcher

**Files:**
- Modify: `tests/test_nvidia_opencode.py`
- Create: `scripts/nvidia-agent`

1. Add failing launcher tests for executable presence, `.env` loading, missing-key rejection, repository-root working directory, exact argument forwarding, and the pinned `npx -y opencode-ai@1.17.13` invocation. Support `NVIDIA_OPENCODE_BIN` only as a test/advanced override.
2. Run the focused tests and confirm launcher cases fail.
3. Implement the minimal POSIX launcher: resolve repository root, load `.env`, validate `NVIDIA_API_KEY`, change to the root, use the override when present, otherwise execute the pinned npm package, and forward all arguments unchanged.
4. Re-run the focused tests and confirm they pass.

### Task 3: Add an explicit live acceptance harness

**Files:**
- Modify: `tests/test_nvidia_opencode.py`
- Create: `scripts/smoke-nvidia-agent`

1. Add failing contract tests for an executable smoke script that performs explicit-model OpenCode runs, uses JSON event output, checks a plain-text response, checks `pwd`, checks a multi-turn tool task, runs the Finance CLI for SPY over six months, and verifies the output file path. Keep it outside default pytest network execution.
2. Run the focused tests and confirm the smoke contract fails.
3. Implement the smoke script with `--pure`, `--format json`, and the explicit `nvidia-nim/nvidia/nemotron-3-super-120b-a12b` selector. Store transient logs under `.cache/nvidia-agent-smoke`; do not parse model-emitted textual JSON as a tool call and do not use another model as fallback.
4. Run the focused tests and confirm they pass.
5. Run the real smoke script. If standard OpenCode emits structured tool events and all checks pass, retain the standard provider config. If NVIDIA rejects a tool-follow-up request, capture the exact request/response evidence and add only the provider-specific `force_nonempty_content` body option supported by OpenCode/AI SDK; do not introduce a generic proxy. Re-run the smoke test after that single compatibility change.

### Task 4: Document the supported entry point and deprecate the fallback

**Files:**
- Modify: `README.md`
- Modify: `scripts/nvidia-codex`
- Modify: `tests/test_nvidia_opencode.py`

1. Add a failing documentation/deprecation test requiring README commands for interactive and non-interactive use and a warning from the legacy Codex launcher.
2. Run the focused tests and confirm failure.
3. Document `./scripts/nvidia-agent` and `./scripts/nvidia-agent run "..."`, required `.env`, approval behavior, and the explicit live smoke command. Add a concise deprecation warning to `scripts/nvidia-codex` while preserving its current behavior.
4. Re-run the focused test, then run the complete repository test suite.
5. Check `git diff --check` and `git status --short`; do not commit, push, or overwrite unrelated user changes.

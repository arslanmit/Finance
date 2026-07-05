# NVIDIA Codex Agent Design

## Goal

Run Codex against NVIDIA Build's hosted model endpoint with working shell-tool
round trips while keeping the API key outside version control.

## Design

NVIDIA's Responses endpoint accepts a basic Codex turn but rejects a follow-up
turn containing tool output. Current Codex releases no longer support the Chat
Completions wire API, so the launcher installs and pins Codex `0.80.0`, the last
verified compatible release for this repository, and sets `wire_api="chat"`.

The repository launcher passes `nvidia/nemotron-3-super-120b-a12b` and an
`nvidia` custom provider to that binary as CLI configuration overrides. It uses
an isolated repo-local cache and Codex home so global plugins and MCP servers do
not enter the NVIDIA tool schema. Authentication is resolved from the
`NVIDIA_API_KEY` environment variable; the secret is never copied into tracked
configuration.

A small `scripts/nvidia-codex` launcher resolves the repository root, loads the
ignored `.env` file, validates that `NVIDIA_API_KEY` is non-empty, bootstraps the
pinned binary when needed, changes to the repository root, and then replaces
itself with the compatible `codex` process. All user CLI arguments are forwarded
unchanged after the provider overrides.

## Error Handling

The launcher exits with a clear message when `.env`, `NVIDIA_API_KEY`, npm, the
platform binary, or macOS code signing support is missing. It never prints the
key.

## Verification

Tests execute the launcher with a fake `codex` binary and package archive. They
verify provider overrides, isolated home selection, pinned bootstrap, argument
forwarding, and the missing-key failure path without contacting NVIDIA or
exposing credentials.

An authenticated `GET /v1/models` request is used separately as the live
credential check.

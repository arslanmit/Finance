# NVIDIA Codex Agent Design

## Goal

Run the installed Codex CLI against NVIDIA Build's hosted model endpoint while
keeping the API key outside version control.

## Design

The repository launcher passes `nvidia/nemotron-3-super-120b-a12b` and an
`nvidia` custom provider to Codex as CLI configuration overrides. Codex does not
allow custom provider definitions in project-local config, so this preserves
global user configuration while still using NVIDIA's OpenAI-compatible endpoint.
Authentication is resolved from the `NVIDIA_API_KEY` environment variable; the
secret is never copied into tracked configuration.

A small `scripts/nvidia-codex` launcher resolves the repository root, loads the
ignored `.env` file, validates that `NVIDIA_API_KEY` is non-empty, changes to the
repository root, and then replaces itself with the installed `codex` process.
All user CLI arguments are forwarded unchanged after the provider overrides.

## Error Handling

The launcher exits with a clear message when `.env`, `NVIDIA_API_KEY`, or the
`codex` executable is missing. It never prints the key.

## Verification

Tests execute the launcher with a fake `codex` binary. They verify provider
overrides, environment loading, argument forwarding, and the missing-key failure
path without contacting NVIDIA or exposing credentials.

An authenticated `GET /v1/models` request is used separately as the live
credential check.

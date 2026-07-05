# NVIDIA OpenCode Agent Design

## Goal

Run `nvidia/nemotron-3-super-120b-a12b` as a reliable coding agent with
multi-turn tool execution, without requiring the OpenAI Codex Responses API.

## Decision

Use OpenCode as the agent harness and NVIDIA NIM's OpenAI-compatible Chat
Completions endpoint as the model transport.

The implementation will not build a Codex Responses-to-Chat compatibility
proxy. Preserving the Codex user interface is not a requirement; reliable tool
execution is the priority.

## Evidence

The existing Codex integration fails at two protocol boundaries:

1. Current Codex releases require the Responses API. NVIDIA accepts the first
   request and may emit a tool call, but rejects the follow-up request containing
   the tool result with HTTP 400 and `InputParam` schema errors.
2. Codex `0.80.0` can still use Chat Completions, but Nemotron sometimes emits a
   JSON command in assistant content rather than a structured `tool_calls`
   response. Codex therefore displays the JSON instead of executing the tool.

A LiteLLM bridge was also rejected. Its Responses-to-Chat conversion forwarded
Codex `namespace` tools to NVIDIA, which only accepted `function` tools on the
Chat Completions endpoint.

NVIDIA's Nemotron model card documents OpenCode as a supported coding-agent
integration. OpenCode's custom-provider documentation explicitly uses
`@ai-sdk/openai-compatible` for `/v1/chat/completions` providers.

## Alternatives Considered

### Codex compatibility proxy

Translate Codex Responses requests, streaming events, namespace tools, tool
results, and model metadata into NVIDIA Chat Completions.

Rejected because it introduces a custom protocol gateway with high maintenance
cost and multiple stateful translation boundaries. This is only justified when
the Codex interface itself is a hard requirement.

### JSON command broker

Parse command-shaped JSON from model text and execute it outside the agent
harness.

Rejected because model text is not a trusted tool-call channel. The broker would
need its own command validation, approval, result correlation, and multi-turn
state machine, duplicating agent-runtime responsibilities.

### OpenCode native Chat Completions

Use OpenCode's existing OpenAI-compatible provider and tool runtime.

Selected because it matches NVIDIA's documented transport, already implements
the tool loop, supports interactive and programmatic modes, and exposes explicit
permission controls.

## Architecture

### Project configuration

Create a repo-local `opencode.json` containing:

- provider id: `nvidia-nim`
- provider package: `@ai-sdk/openai-compatible`
- base URL: `https://integrate.api.nvidia.com/v1`
- API key: `{env:NVIDIA_API_KEY}`
- model id: `nvidia/nemotron-3-super-120b-a12b`
- default model selector:
  `nvidia-nim/nvidia/nemotron-3-super-120b-a12b`
- model context limit: `1000000`
- model output limit: `32768`
- request and streaming timeouts appropriate for long reasoning turns

The configuration must not contain the API key value.

### Launcher

Create `scripts/nvidia-agent` as the supported entrypoint.

The launcher will:

1. Resolve the repository root.
2. Load the ignored `.env` file.
3. Require a non-empty `NVIDIA_API_KEY`.
4. Run a pinned `opencode-ai@1.17.13` package.
5. Change to the repository root before starting OpenCode.
6. Forward all arguments unchanged.

Supported modes:

```bash
./scripts/nvidia-agent
./scripts/nvidia-agent run "Inspect the repository and run the documented smoke test"
```

The no-argument form starts the OpenCode TUI. The `run` form is suitable for
Codex shell invocation and automation.

### Permissions

Default permissions will follow least privilege:

- allow repository reads, listing, globbing, and searching;
- ask before edits;
- ask before arbitrary shell commands;
- deny external-directory access by default;
- deny destructive shell patterns such as `rm *`;
- deny `git push *` and equivalent publication commands;
- keep web access denied unless explicitly required.

Interactive sessions can approve an action at execution time. Programmatic runs
must not use blanket auto-approval by default. A future explicit automation mode
may add a narrow command allowlist after separate review.

### NVIDIA-specific request behavior

The first implementation will use the standard OpenCode
`@ai-sdk/openai-compatible` request shape without a custom provider package.

NVIDIA recommends `chat_template_kwargs.force_nonempty_content=true` for coding
agents. During the implementation spike, inspect the actual OpenCode request and
determine whether model `options` pass the required provider-specific body field
through the AI SDK. If they do, configure it in `opencode.json`. If they do not,
add the smallest possible custom OpenCode provider adapter that only injects this
field. Do not introduce a general-purpose proxy.

### Legacy launcher

Keep `scripts/nvidia-codex` temporarily, but change its documentation status to
deprecated after the OpenCode acceptance suite passes.

The deprecated launcher must direct users to `scripts/nvidia-agent`. Remove it
only in a separate cleanup change after the new path has been used successfully.

## Data Flow

```text
User or Codex shell
    -> scripts/nvidia-agent
    -> OpenCode agent runtime
    -> POST /v1/chat/completions with function tools
    -> NVIDIA Nemotron structured tool call
    -> OpenCode permission check
    -> OpenCode executes the tool
    -> OpenCode sends role=tool result to NVIDIA
    -> NVIDIA returns the final assistant response
```

OpenCode owns tool-call parsing, execution, result correlation, retries, and
conversation state. The launcher owns only environment loading, version pinning,
working-directory selection, and process delegation.

## Error Handling

The launcher exits with a concise error when:

- `.env` is missing;
- `NVIDIA_API_KEY` is missing or empty;
- Node.js or npm is unavailable;
- the pinned OpenCode package cannot be installed or executed.

Runtime errors must preserve the OpenCode exit code. Secrets must never be
printed in diagnostics, configuration, tests, or documentation.

If NVIDIA returns assistant content containing command JSON instead of a
structured tool call, treat the run as failed. Do not parse and execute the text
as a fallback command.

## Testing

### Unit and contract tests

Add launcher tests using a fake `opencode` executable to verify:

- `.env` loading;
- API-key propagation without printing the key;
- pinned package/version selection;
- repository working directory;
- exact argument forwarding;
- missing-key and missing-runtime failures.

Validate `opencode.json` against the published schema and assert:

- the provider uses `@ai-sdk/openai-compatible`;
- the base URL is correct;
- the API key uses environment interpolation;
- the selected model id is correct;
- unsafe permissions are not enabled.

### Live acceptance suite

Run the following checks against NVIDIA:

1. Text response: return a fixed token such as `NVIDIA_OK`.
2. Single tool call: run `pwd` and report the exact path.
3. Multi-turn tool loop: run `pwd`, consume the tool result, and return a final
   natural-language response.
4. Repository command: run
   `python3 dynamic_range_average.py run --dataset spy --months 6` and report
   `output/spy_processed.csv`.
5. Temporary edit: create and modify a file inside a disposable test directory,
   then verify the expected content.
6. Safety: confirm a denied command such as `git push` is not executed.

Success requires all six checks to complete without manual command extraction,
text-JSON execution, protocol errors, or fallback to another model.

Live tests must not be part of the default offline pytest suite. Expose them
through an explicit smoke-test command and skip with a clear message when
`NVIDIA_API_KEY` is unavailable.

## Rollout

1. Add the OpenCode config and launcher behind the existing `.env` credential.
2. Run offline contract tests.
3. Run the complete live acceptance suite.
4. Update README usage and troubleshooting.
5. Mark the Codex launcher deprecated only after acceptance passes.

## Non-Goals

- Reimplementing the Codex Responses API.
- Supporting arbitrary LLM providers in the new launcher.
- Automatically executing command-shaped assistant text.
- Enabling global MCP servers during the initial rollout.
- Publishing, committing, or pushing changes automatically.

## Sources

- NVIDIA Nemotron model card:
  https://build.nvidia.com/nvidia/nemotron-3-super-120b-a12b/modelcard
- NVIDIA tool-calling documentation:
  https://docs.nvidia.com/nim/large-language-models/latest/advanced-use-cases/tool-calling-and-mcp.html
- OpenCode custom-provider documentation:
  https://opencode.ai/docs/providers
- OpenCode permissions documentation:
  https://opencode.ai/docs/permissions
- OpenCode CLI documentation:
  https://dev.opencode.ai/docs/cli/
- AI SDK OpenAI-compatible provider documentation:
  https://ai-sdk.dev/providers/openai-compatible-providers

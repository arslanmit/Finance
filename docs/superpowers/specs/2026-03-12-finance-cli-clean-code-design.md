# Finance CLI Clean Code Design

## Goal

Clean the full codebase without changing the user-facing contract unless an existing behavior is clearly incorrect.

## Constraints

- Keep command names, flags, dataset formats, and default output semantics stable.
- Use the existing test suite as the primary regression guard.
- Favor smaller, purpose-built modules over broad utility layers.
- Avoid speculative abstractions and unnecessary rewrites.

## Current Problems

- [`finance_cli/cli.py`](/Users/Development/Finance/.worktrees/clean-code-overhaul/finance_cli/cli.py) mixes parser construction, command dispatch, wizard interaction, matrix orchestration, refresh flows, and presentation.
- [`finance_cli/analysis.py`](/Users/Development/Finance/.worktrees/clean-code-overhaul/finance_cli/analysis.py) couples indicator registration, rule parsing, dataframe preparation, analysis execution, and output shaping.
- Repo hygiene is inconsistent because generated junk such as `.DS_Store` and `__pycache__` artifacts live inside tracked directories.
- Some behavior is implemented more than once, especially around running analyses from standard CLI flow, wizard flow, and matrix execution.

## Design

### 1. CLI Boundary Cleanup

Keep [`finance_cli/cli.py`](/Users/Development/Finance/.worktrees/clean-code-overhaul/finance_cli/cli.py) as a thin public entrypoint.

Extract the following responsibilities into focused modules:

- parser construction
- command handlers
- wizard prompting and selection
- matrix job generation and execution
- console formatting and status output

The entrypoint should coordinate these modules, not implement their internal workflows.

### 2. Analysis Boundary Cleanup

Split analysis responsibilities into focused modules while preserving the current public API:

- indicator registry and calculators
- rule parsing and evaluation
- dataframe preparation and validation
- output formatting and column ordering
- high-level analysis orchestration

`analyze_dataframe()` and `analyze_dataframe_with_config()` should remain the stable integration points for callers.

### 3. Shared Workflow Reuse

Make one analysis execution path the canonical workflow:

1. resolve input source
2. optionally refresh it
3. load and normalize the dataframe
4. prepare the dataframe for the selected window
5. analyze using `AnalysisConfig`
6. render terminal output
7. write CSV output

The `run` command, wizard flow, and matrix execution should reuse the same underlying workflow primitives rather than each reimplementing the same steps.

### 4. Repo Hygiene Cleanup

Apply codebase-wide consistency improvements after structural refactors settle:

- remove tracked junk files and strengthen ignores where needed
- normalize helper names where they are vague or misleading
- reduce redundant comments and overlong docstrings
- align error handling with the existing domain exception types
- keep files focused and avoid broad grab-bag helpers

## Error Handling

- Keep domain errors routed through `FinanceCliError` subclasses.
- Preserve user-readable CLI error output from `main()`.
- Narrow broad `except Exception` blocks when responsibility can be localized.
- In batch workflows such as matrix execution, continue recording per-job failures instead of aborting the entire run.

## Testing Strategy

- Preserve the current full-suite green baseline before any refactor.
- Add targeted regression tests before moving behavior behind new boundaries.
- For each extracted responsibility, prefer narrow unit tests over only broad integration coverage.
- Re-run focused tests during each refactor slice, then re-run the full suite after each phase.

## Non-Goals

- No new user-facing features.
- No command or file format redesign.
- No replacement of pandas or the current CLI stack.
- No speculative plugin architecture or framework migration.

## Implementation Phases

### Phase 1

Split the CLI layer into parser, handlers, wizard, matrix, and presentation modules.

### Phase 2

Split the analysis layer into calculators, rules, preparation, output, and orchestration modules while keeping the public API stable.

### Phase 3

Clean supporting modules only where boundaries are genuinely unclear or code duplication remains.

### Phase 4

Apply repo-wide consistency cleanup, remove tracked junk, and update docs if file locations or internal architecture descriptions need adjustment.

# Finance CLI Clean Code Overhaul Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Clean the full Finance CLI codebase by splitting overloaded modules, removing tracked junk, and tightening consistency without changing the user-facing contract.

**Architecture:** Keep the existing public API stable while extracting focused modules behind it. Make the CLI entrypoint thin, consolidate analysis workflows behind reusable primitives, and preserve behavior through targeted regression tests plus the full suite.

**Tech Stack:** Python 3, argparse, pandas, pytest

---

## Chunk 1: Safety Net And Repo Hygiene

### Task 1: Add refactor-guard tests for CLI workflow seams

**Files:**
- Modify: `tests/test_cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing test**

Add focused tests that lock in the current command-routing behavior that will be touched by the refactor, such as:

```python
def test_main_without_args_runs_wizard(monkeypatch) -> None:
    called = {"wizard": False}

    def fake_run_wizard() -> None:
        called["wizard"] = True

    monkeypatch.setattr(cli, "run_wizard", fake_run_wizard)

    assert cli.main([]) == 0
    assert called["wizard"] is True
```

```python
def test_dispatch_command_routes_known_commands(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(cli, "handle_run_command", lambda args: calls.append("run"))
    args = argparse.Namespace(command="run")

    assert cli.dispatch_command(args) == 0
    assert calls == ["run"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_cli.py -q`
Expected: FAIL because the new assertions exercise workflow seams not yet covered or require light adaptation in the current module.

- [ ] **Step 3: Write minimal implementation**

Only adjust tests or current code enough to make the current behavior explicit without starting structural extraction yet.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_cli.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_cli.py
git commit -m "test: lock cli workflow seams"
```

### Task 2: Remove tracked junk and lock repo hygiene

**Files:**
- Delete: `finance_cli/.DS_Store`
- Delete: `tests/.DS_Store`
- Modify: `.gitignore`
- Test: `tests/test_repo_csv_only.py`

- [ ] **Step 1: Write the failing test**

Extend the repo hygiene tests so tracked OS junk is rejected.

```python
def test_repo_has_no_os_junk_files() -> None:
    junk = list(ROOT.rglob(".DS_Store"))
    assert junk == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_repo_csv_only.py -q`
Expected: FAIL because `.DS_Store` files are currently tracked.

- [ ] **Step 3: Write minimal implementation**

Delete tracked `.DS_Store` files and keep the ignore rules in place.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_repo_csv_only.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add .gitignore tests/test_repo_csv_only.py
git rm --cached --ignore-unmatch finance_cli/.DS_Store tests/.DS_Store
git commit -m "chore: remove tracked os junk"
```

## Chunk 2: CLI Decomposition

### Task 3: Extract console formatting helpers

**Files:**
- Create: `finance_cli/presentation.py`
- Modify: `finance_cli/cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing test**

Add tests that call the new presentation helpers directly for dataset and refresh output formatting.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_cli.py -q`
Expected: FAIL because `finance_cli.presentation` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Move `print_refresh_summary()`, `print_dataset_refresh_summary()`, and `print_dataset_list()` into `finance_cli/presentation.py` and update `cli.py` imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_cli.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add finance_cli/presentation.py finance_cli/cli.py tests/test_cli.py
git commit -m "refactor: extract cli presentation helpers"
```

### Task 4: Extract shared run workflow

**Files:**
- Create: `finance_cli/run_workflow.py`
- Modify: `finance_cli/cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing test**

Add tests for a shared run workflow function that handles refresh, load, prepare, analyze, and save in one place.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_cli.py -q`
Expected: FAIL because the shared workflow module does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Extract `execute_analysis()` and `refresh_generated_datasets()` into `finance_cli/run_workflow.py`, then update `cli.py` to delegate to them.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_cli.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add finance_cli/run_workflow.py finance_cli/cli.py tests/test_cli.py
git commit -m "refactor: extract run workflow"
```

### Task 5: Extract wizard and matrix modules

**Files:**
- Create: `finance_cli/wizard.py`
- Create: `finance_cli/matrix.py`
- Modify: `finance_cli/cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing test**

Add direct tests for `build_wizard_menu_items()`, `sort_datasets_for_display()`, `build_matrix_jobs()`, and `build_matrix_output_path()` from their new modules.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_cli.py -q`
Expected: FAIL because `finance_cli.wizard` and `finance_cli.matrix` do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Move the wizard dataclasses and prompt helpers into `finance_cli/wizard.py`. Move matrix dataclasses and matrix orchestration into `finance_cli/matrix.py`. Keep `cli.py` as the integration entrypoint.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_cli.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add finance_cli/wizard.py finance_cli/matrix.py finance_cli/cli.py tests/test_cli.py
git commit -m "refactor: split wizard and matrix flows"
```

### Task 6: Extract parser and command handler modules

**Files:**
- Create: `finance_cli/cli_parser.py`
- Create: `finance_cli/cli_handlers.py`
- Modify: `finance_cli/cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing test**

Add tests that import `build_parser()` from `finance_cli.cli_parser` and `dispatch_command()` from `finance_cli.cli_handlers`.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_cli.py -q`
Expected: FAIL because the extracted modules do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Move parser construction to `finance_cli/cli_parser.py` and command dispatch/handlers to `finance_cli/cli_handlers.py`. Keep `finance_cli/cli.py` as a thin facade around these pieces.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_cli.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add finance_cli/cli_parser.py finance_cli/cli_handlers.py finance_cli/cli.py tests/test_cli.py
git commit -m "refactor: thin cli entrypoint"
```

## Chunk 3: Analysis Decomposition

### Task 7: Extract indicator registry and calculators

**Files:**
- Create: `finance_cli/analysis_indicators.py`
- Modify: `finance_cli/analysis.py`
- Test: `tests/test_indicator_calculators.py`
- Test: `tests/test_indicator_registry.py`

- [ ] **Step 1: Write the failing test**

Add direct imports from `finance_cli.analysis_indicators` for the registry and calculators.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_indicator_calculators.py tests/test_indicator_registry.py -q`
Expected: FAIL because `finance_cli.analysis_indicators` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Move `IndicatorCalculator`, `IndicatorRegistry`, registry initialization, `get_indicator_registry()`, calculator functions, indicator column naming, and indicator result validation into `finance_cli/analysis_indicators.py`. Re-export the current public functions from `finance_cli/analysis.py`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_indicator_calculators.py tests/test_indicator_registry.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add finance_cli/analysis_indicators.py finance_cli/analysis.py tests/test_indicator_calculators.py tests/test_indicator_registry.py
git commit -m "refactor: extract indicator registry"
```

### Task 8: Extract rule parsing and evaluation

**Files:**
- Create: `finance_cli/analysis_rules.py`
- Modify: `finance_cli/analysis.py`
- Test: `tests/test_analysis.py`

- [ ] **Step 1: Write the failing test**

Add direct imports from `finance_cli.analysis_rules` for `parse_rule()`, `format_rule()`, and `evaluate_rule()`.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_analysis.py -q`
Expected: FAIL because `finance_cli.analysis_rules` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Move rule parsing and evaluation logic into `finance_cli/analysis_rules.py` and re-export the public helpers from `finance_cli/analysis.py`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_analysis.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add finance_cli/analysis_rules.py finance_cli/analysis.py tests/test_analysis.py
git commit -m "refactor: extract rule helpers"
```

### Task 9: Extract dataframe preparation and output shaping

**Files:**
- Create: `finance_cli/analysis_prepare.py`
- Create: `finance_cli/analysis_output.py`
- Modify: `finance_cli/analysis.py`
- Test: `tests/test_analysis.py`

- [ ] **Step 1: Write the failing test**

Add direct imports from `finance_cli.analysis_prepare` and `finance_cli.analysis_output` for `prepare_dataframe()`, `ordered_output_columns()`, and `get_trailing_derived_columns()`.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_analysis.py -q`
Expected: FAIL because the new modules do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Move dataframe preparation and output shaping logic into dedicated modules. Keep `analyze_dataframe()` and `analyze_dataframe_with_config()` in `finance_cli/analysis.py` as the stable orchestration layer.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_analysis.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add finance_cli/analysis_prepare.py finance_cli/analysis_output.py finance_cli/analysis.py tests/test_analysis.py
git commit -m "refactor: split analysis internals"
```

## Chunk 4: Verification And Documentation

### Task 10: Run full verification and polish docs

**Files:**
- Modify: `README.md`
- Test: `tests/test_cli.py`
- Test: `tests/test_analysis.py`
- Test: `tests/test_repo_csv_only.py`

- [ ] **Step 1: Write the failing test**

If any documentation-sensitive paths or examples changed during refactoring, add or adjust the narrowest regression test that captures the contract before editing docs.

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_cli.py tests/test_analysis.py tests/test_repo_csv_only.py -q`
Expected: FAIL only if a missing contract needs to be locked before docs updates. If no new contract test is needed, note that the existing suite already covers the behavior and move to Step 3.

- [ ] **Step 3: Write minimal implementation**

Update `README.md` only if it needs wording changes to reflect internal cleanup or repo hygiene constraints. Do not document internal module splits unless helpful to contributors.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add README.md finance_cli tests
git commit -m "refactor: complete clean code overhaul"
```

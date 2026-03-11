"""CLI entrypoint and wizard flow."""

from __future__ import annotations

import argparse
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

import pandas as pd

from .analysis import (
    analyze_dataframe,
    analyze_dataframe_with_config,
    build_default_output_path,
    format_rule,
    get_indicator_registry,
    parse_rule,
    prepare_dataframe,
    render_filtered_rows,
    save_dataframe,
)
from .catalog import discover_datasets, get_dataset, import_dataset, remove_dataset
from .create import create_symbol_dataset
from .errors import FinanceCliError
from .models import AnalysisConfig, DatasetConfig, RefreshSummary, ResolvedSource
from .refresh import refresh_selected_source
from .sources import ensure_symbol_column, load_dataframe, resolve_custom_source, resolve_dataset_source

MATRIX_MONTHS = (1, 3, 6, 12, 24)
MATRIX_RULE_OPERATORS = (">", "<", ">=", "<=")
MATRIX_RULE_COLUMNS = ("open", "high", "low", "close")
RULE_SLUG_OPERATOR_MAP = {">": "gt", "<": "lt", ">=": "gte", "<=": "lte"}


@dataclass(frozen=True)
class WizardSourceChoice:
    """Resolved source choice from the interactive wizard."""

    source: ResolvedSource
    created_now: bool = False


@dataclass(frozen=True)
class WizardMenuItem:
    """Selectable menu item shown in the guided wizard."""

    alias: str
    label: str
    action: str
    dataset: DatasetConfig | None = None


@dataclass(frozen=True)
class MatrixJob:
    """Single matrix execution job."""

    months: int
    indicator: str
    rule: str


@dataclass(frozen=True)
class MatrixRunRecord:
    """Manifest row for a matrix execution result."""

    dataset_id: str
    symbol: str
    input_path: str
    months: int
    indicator: str
    rule: str
    status: str
    output_path: str
    row_count: int | None
    condition_true_count: int | None
    error: str


def build_parser() -> argparse.ArgumentParser:
    available_indicators = ", ".join(get_indicator_registry().list_indicators())
    parser = argparse.ArgumentParser(
        description="Finance dataset analysis CLI with guided and command-based workflows."
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run configurable indicator analysis.")
    run_group = run_parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument("--dataset", help="Dataset id from data/generated.")
    run_group.add_argument("--file", help="Path to a CSV file.")
    run_parser.add_argument("--months", type=int, required=True, help="Indicator window size.")
    run_parser.add_argument(
        "--indicator",
        default="sma",
        help=f"Indicator type to calculate (available: {available_indicators}).",
    )
    run_parser.add_argument(
        "--rule",
        default="indicator > open",
        help="Screening rule in the form 'left_operand operator right_operand'.",
    )
    run_parser.add_argument("--output", help="Optional output CSV path.")
    run_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh a generated symbol-backed dataset before analysis.",
    )

    matrix_parser = subparsers.add_parser(
        "matrix",
        help="Run the fixed indicator/rule/month matrix across all generated datasets.",
    )
    matrix_parser.add_argument(
        "--output-dir",
        help="Optional directory for matrix outputs and manifest.",
    )

    datasets_parser = subparsers.add_parser("datasets", help="Manage generated datasets.")
    datasets_subparsers = datasets_parser.add_subparsers(dest="datasets_command", required=True)

    datasets_subparsers.add_parser("list", help="List all generated datasets.")

    add_parser = datasets_subparsers.add_parser(
        "add",
        help="Copy a CSV into data/generated using its filename.",
    )
    add_parser.add_argument("--path", required=True, help="Path to the dataset CSV file.")
    add_parser.add_argument(
        "--refresh-symbol",
        help="Yahoo Finance symbol for live refresh support.",
    )

    create_parser = datasets_subparsers.add_parser(
        "create",
        help="Create a new dataset from a Yahoo Finance symbol.",
    )
    create_parser.add_argument("--symbol", required=True, help="Yahoo Finance symbol, e.g. SPY or AAPL.")

    remove_parser = datasets_subparsers.add_parser("remove", help="Remove a generated dataset.")
    remove_parser.add_argument("--id", required=True, help="Dataset id to remove.")

    refresh_parser = datasets_subparsers.add_parser(
        "refresh",
        help="Refresh generated symbol-backed datasets from Yahoo Finance.",
    )
    refresh_group = refresh_parser.add_mutually_exclusive_group(required=True)
    refresh_group.add_argument("--id", help="Dataset id to refresh.")
    refresh_group.add_argument(
        "--all",
        action="store_true",
        help="Refresh all generated datasets that support live refresh.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)

    try:
        if not args_list:
            run_wizard()
            return 0

        parser = build_parser()
        args = parser.parse_args(args_list)
        return dispatch_command(args)
    except FinanceCliError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1


def dispatch_command(args: argparse.Namespace) -> int:
    if args.command == "run":
        handle_run_command(args)
        return 0
    if args.command == "matrix":
        handle_matrix_command(args)
        return 0
    if args.command == "datasets":
        handle_datasets_command(args)
        return 0
    raise FinanceCliError("Unknown command.")


def handle_run_command(args: argparse.Namespace) -> None:
    datasets = discover_datasets() if args.dataset else []
    source = resolve_run_source(args, datasets)
    output_path = Path(args.output).expanduser() if args.output else build_default_output_path(source.input_path)
    config = AnalysisConfig(
        months=args.months,
        indicator_type=args.indicator.strip().lower(),
        rule=args.rule,
    )
    execute_analysis(source, config=config, output_path=output_path, refresh_requested=args.refresh)


def handle_matrix_command(args: argparse.Namespace) -> None:
    datasets = discover_datasets()
    if not datasets:
        raise FinanceCliError("No generated datasets were found for matrix execution.")

    jobs = build_matrix_jobs()
    output_dir = build_matrix_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    total_jobs = len(datasets) * len(jobs)
    print(
        f"Matrix run starting: datasets={len(datasets)}, jobs_per_dataset={len(jobs)}, "
        f"total_jobs={total_jobs}, output_dir={output_dir}"
    )

    records = run_matrix_jobs(datasets, jobs, output_dir)
    manifest_path = write_matrix_manifest(records, output_dir)
    success_count = sum(record.status == "success" for record in records)
    failed_count = len(records) - success_count
    print(
        f"Matrix run complete: total_jobs={len(records)}, succeeded={success_count}, "
        f"failed={failed_count}, manifest={manifest_path}"
    )


def resolve_run_source(args: argparse.Namespace, datasets: list[DatasetConfig]) -> ResolvedSource:
    if args.dataset:
        dataset = get_dataset(args.dataset, datasets)
        return resolve_dataset_source(dataset)

    return resolve_custom_source(args.file)


def build_matrix_jobs() -> list[MatrixJob]:
    jobs: list[MatrixJob] = []
    indicators = get_indicator_registry().list_indicators()
    for months in MATRIX_MONTHS:
        for indicator in indicators:
            for operator in MATRIX_RULE_OPERATORS:
                for column in MATRIX_RULE_COLUMNS:
                    jobs.append(
                        MatrixJob(
                            months=months,
                            indicator=indicator,
                            rule=f"indicator {operator} {column}",
                        )
                    )
    return jobs


def build_matrix_output_dir(output_dir: str | None) -> Path:
    if output_dir:
        return Path(output_dir).expanduser()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return Path("output") / "matrix" / timestamp


def slugify_rule(rule: str) -> str:
    parsed_rule = parse_rule(rule)
    operator_slug = RULE_SLUG_OPERATOR_MAP[parsed_rule.operator]
    return f"{parsed_rule.left_operand.lower()}-{operator_slug}-{parsed_rule.right_operand.lower()}"


def build_matrix_output_path(output_dir: Path, dataset_id: str, job: MatrixJob) -> Path:
    file_name = f"{dataset_id}__m{job.months}__{job.indicator}__{slugify_rule(job.rule)}.csv"
    return output_dir / dataset_id / file_name


def build_matrix_record(
    *,
    dataset: DatasetConfig,
    input_path: Path,
    job: MatrixJob,
    output_path: Path,
    status: str,
    row_count: int | None,
    condition_true_count: int | None,
    error: str = "",
) -> MatrixRunRecord:
    return MatrixRunRecord(
        dataset_id=dataset.id,
        symbol=dataset.symbol or "",
        input_path=str(input_path),
        months=job.months,
        indicator=job.indicator,
        rule=job.rule,
        status=status,
        output_path=str(output_path),
        row_count=row_count,
        condition_true_count=condition_true_count,
        error=error,
    )


def run_matrix_jobs(
    datasets: list[DatasetConfig],
    jobs: list[MatrixJob],
    output_dir: Path,
) -> list[MatrixRunRecord]:
    records: list[MatrixRunRecord] = []
    jobs_by_month = {
        months: [job for job in jobs if job.months == months]
        for months in MATRIX_MONTHS
    }

    for index, dataset in enumerate(datasets, start=1):
        print(f"[{index}/{len(datasets)}] Running dataset '{dataset.id}'")
        source = resolve_dataset_source(dataset)
        input_path = source.input_path
        try:
            raw_dataframe = load_dataframe(input_path)
            raw_dataframe = ensure_symbol_column(raw_dataframe, dataset.symbol)
        except Exception as exc:
            for job in jobs:
                output_path = build_matrix_output_path(output_dir, dataset.id, job)
                records.append(
                    build_matrix_record(
                        dataset=dataset,
                        input_path=input_path,
                        job=job,
                        output_path=output_path,
                        status="error",
                        row_count=None,
                        condition_true_count=None,
                        error=str(exc),
                    )
                )
            continue

        for months in MATRIX_MONTHS:
            month_jobs = jobs_by_month[months]
            try:
                prepared_dataframe = prepare_dataframe(raw_dataframe, months)
            except Exception as exc:
                for job in month_jobs:
                    output_path = build_matrix_output_path(output_dir, dataset.id, job)
                    records.append(
                        build_matrix_record(
                            dataset=dataset,
                            input_path=input_path,
                            job=job,
                            output_path=output_path,
                            status="error",
                            row_count=None,
                            condition_true_count=None,
                            error=str(exc),
                        )
                    )
                continue

            for job in month_jobs:
                output_path = build_matrix_output_path(output_dir, dataset.id, job)
                config = AnalysisConfig(
                    months=job.months,
                    indicator_type=job.indicator,
                    rule=job.rule,
                )
                try:
                    analyzed_dataframe = analyze_dataframe_with_config(prepared_dataframe, config)
                    save_dataframe(analyzed_dataframe, output_path)
                    records.append(
                        build_matrix_record(
                            dataset=dataset,
                            input_path=input_path,
                            job=job,
                            output_path=output_path,
                            status="success",
                            row_count=len(analyzed_dataframe),
                            condition_true_count=int(analyzed_dataframe["condition"].sum()),
                        )
                    )
                except Exception as exc:
                    records.append(
                        build_matrix_record(
                            dataset=dataset,
                            input_path=input_path,
                            job=job,
                            output_path=output_path,
                            status="error",
                            row_count=None,
                            condition_true_count=None,
                            error=str(exc),
                        )
                    )

    return records


def write_matrix_manifest(records: list[MatrixRunRecord], output_dir: Path) -> Path:
    manifest_path = output_dir / "manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([asdict(record) for record in records]).to_csv(manifest_path, index=False)
    return manifest_path


def handle_datasets_command(args: argparse.Namespace) -> None:
    if args.datasets_command == "list":
        print_dataset_list(discover_datasets())
        return

    if args.datasets_command == "add":
        dataset = import_dataset(
            source_path=args.path,
            refresh_symbol=args.refresh_symbol,
        )
        print(f"Added dataset '{dataset.id}' -> {dataset.path}")
        return

    if args.datasets_command == "create":
        dataset = create_symbol_dataset(args.symbol)
        print(
            f"Created dataset '{dataset.id}' from symbol {dataset.refresh.symbol} -> {dataset.path}"
        )
        return

    if args.datasets_command == "remove":
        removed = remove_dataset(args.id)
        print(f"Removed dataset '{removed.id}'")
        return

    if args.datasets_command == "refresh":
        refreshed = refresh_generated_datasets(
            discover_datasets(),
            dataset_id=args.id,
            refresh_all=args.all,
        )
        for dataset, summary in refreshed:
            print_dataset_refresh_summary(dataset, summary)
        return

    raise FinanceCliError("Unknown datasets command.")


def run_wizard() -> None:
    datasets = discover_datasets()
    selection = prompt_for_source(datasets)
    source = selection.source
    months = prompt_for_months()
    indicator_type = prompt_for_indicator()
    rule = prompt_for_rule()
    refresh_requested = (
        not selection.created_now
        and source.dataset is not None
        and source.dataset.supports_refresh
        and prompt_yes_no("Refresh symbol-backed data first?", default=False)
    )

    default_output = build_default_output_path(source.input_path)
    output_path = prompt_for_output_path(default_output)
    config = AnalysisConfig(months=months, indicator_type=indicator_type, rule=rule)
    execute_analysis(source, config=config, output_path=output_path, refresh_requested=refresh_requested)


def prompt_for_source(datasets: list[DatasetConfig]) -> WizardSourceChoice:
    print("Choose a dataset:\n")
    menu_items = build_wizard_menu_items(datasets)

    for index, item in enumerate(menu_items, start=1):
        print(f"{index}. {item.label}")

    while True:
        response = input("\nEnter a dataset number or alias: ").strip()
        if not response:
            continue

        if response.isdigit():
            selection = int(response)
            if 1 <= selection <= len(menu_items):
                return select_wizard_menu_item(menu_items[selection - 1])

        for item in menu_items:
            if response.lower() == item.alias:
                return select_wizard_menu_item(item)

        print("Invalid selection. Choose a listed number, dataset alias, 'create', or 'custom'.")


def build_wizard_menu_items(datasets: list[DatasetConfig]) -> list[WizardMenuItem]:
    menu_items = [
        WizardMenuItem(
            alias="create",
            label="create - Create a new dataset from a Yahoo symbol",
            action="create",
        )
    ]
    menu_items.append(
        WizardMenuItem(
            alias="custom",
            label="custom - Use your own CSV file path",
            action="custom",
        )
    )
    menu_items.extend(
        WizardMenuItem(
            alias=dataset.id,
            label=dataset_menu_label(dataset),
            action="dataset",
            dataset=dataset,
        )
        for dataset in sort_datasets_for_display(datasets)
    )
    return menu_items


def dataset_menu_label(dataset: DatasetConfig) -> str:
    refresh_tag = " [refresh available]" if dataset.supports_refresh else ""
    return f"{dataset.id}{refresh_tag}"


def sort_datasets_for_display(datasets: list[DatasetConfig]) -> list[DatasetConfig]:
    return sorted(
        datasets,
        key=lambda dataset: (
            dataset.file_name.lower(),
            dataset.id.lower(),
        ),
    )


def select_wizard_menu_item(
    item: WizardMenuItem,
) -> WizardSourceChoice:
    if item.action == "dataset":
        if item.dataset is None:
            raise FinanceCliError("Wizard dataset selection is invalid.")
        return WizardSourceChoice(resolve_dataset_source(item.dataset))
    if item.action == "custom":
        return prompt_for_custom_source()
    if item.action == "create":
        return prompt_for_symbol_dataset()
    raise FinanceCliError("Unknown wizard menu item.")


def prompt_for_custom_source() -> WizardSourceChoice:
    while True:
        custom_path = input("Enter the CSV file path: ").strip()
        if not custom_path:
            print("Please enter a file path.")
            continue

        try:
            return WizardSourceChoice(resolve_custom_source(custom_path))
        except FinanceCliError as exc:
            print(f"Error: {exc}")


def prompt_for_symbol_dataset() -> WizardSourceChoice:
    while True:
        symbol = input("Enter the Yahoo Finance symbol (for example SPY, VOO, AAPL): ").strip()
        if not symbol:
            print("Please enter a symbol.")
            continue

        try:
            dataset = create_symbol_dataset(symbol)
            print(
                f"Created dataset '{dataset.id}' from symbol {dataset.refresh.symbol} -> {dataset.path}"
            )
            return WizardSourceChoice(resolve_dataset_source(dataset), created_now=True)
        except FinanceCliError as exc:
            print(f"Error: {exc}")


def prompt_for_months() -> int:
    while True:
        response = input("Enter the indicator window (examples: 3, 6, 12): ").strip()
        try:
            return int(response)
        except ValueError:
            print("Please enter a whole number, for example 3, 6, or 12.")


def prompt_for_indicator() -> str:
    available = get_indicator_registry().list_indicators()
    default_indicator = "sma"
    while True:
        response = input(
            f"Enter the indicator [{default_indicator}] (available: {', '.join(available)}): "
        ).strip()
        indicator = default_indicator if not response else response.lower()
        if indicator in available:
            return indicator
        print(f"Please choose one of: {', '.join(available)}.")


def prompt_for_rule() -> str:
    default_rule = "indicator > open"
    while True:
        response = input(
            f"Enter the screening rule [{default_rule}] (example: indicator > close): "
        ).strip()
        candidate = default_rule if not response else response
        try:
            return format_rule(parse_rule(candidate))
        except FinanceCliError as exc:
            print(f"Error: {exc}")


def prompt_for_output_path(default_output: Path) -> Path:
    response = input(
        f"Output path [{default_output}] (press Enter to accept): "
    ).strip()
    if not response:
        return default_output
    return Path(response).expanduser()


def prompt_yes_no(prompt: str, default: bool = False) -> bool:
    suffix = " [Y/n]: " if default else " [y/N]: "
    while True:
        response = input(f"{prompt}{suffix}").strip().lower()
        if not response:
            return default
        if response in {"y", "yes"}:
            return True
        if response in {"n", "no"}:
            return False
        print("Please answer with 'y' or 'n'.")


def execute_analysis(
    source: ResolvedSource,
    *,
    config: AnalysisConfig,
    output_path: Path,
    refresh_requested: bool,
) -> None:
    if refresh_requested:
        summary = refresh_selected_source(source)
        print_refresh_summary(summary)

    raw_dataframe = load_dataframe(source.input_path)
    raw_dataframe = ensure_symbol_column(
        raw_dataframe,
        None if source.dataset is None else source.dataset.symbol,
    )
    prepared_dataframe = prepare_dataframe(raw_dataframe, config.months)
    normalized_rule = format_rule(parse_rule(config.rule))
    use_legacy_defaults = config.indicator_type.strip().lower() == "sma" and normalized_rule == "indicator > open"
    if use_legacy_defaults:
        analyzed_dataframe = analyze_dataframe(prepared_dataframe, config.months)
    else:
        analyzed_dataframe = analyze_dataframe_with_config(prepared_dataframe, config)

    print(f"Indicator: {config.indicator_type.upper()} (window={config.months})")
    print(f"Rule: {normalized_rule}\n")
    print(render_filtered_rows(analyzed_dataframe))
    save_dataframe(analyzed_dataframe, output_path)
    print(f"\nProcessed data saved to: {output_path}")


def refresh_generated_datasets(
    datasets: list[DatasetConfig],
    *,
    dataset_id: str | None,
    refresh_all: bool,
) -> list[tuple[DatasetConfig, RefreshSummary]]:
    if refresh_all:
        targets = [dataset for dataset in datasets if dataset.supports_refresh]
        if not targets:
            raise FinanceCliError("No generated datasets support live refresh.")
    else:
        if dataset_id is None:
            raise FinanceCliError("A dataset id is required unless --all is used.")
        dataset = get_dataset(dataset_id, datasets)
        if not dataset.supports_refresh:
            raise FinanceCliError(f"Dataset '{dataset.id}' does not support live refresh.")
        targets = [dataset]

    refreshed: list[tuple[DatasetConfig, RefreshSummary]] = []
    for dataset in targets:
        summary = refresh_selected_source(resolve_dataset_source(dataset))
        refreshed.append((dataset, summary))
    return refreshed


def print_refresh_summary(summary: RefreshSummary) -> None:
    print(
        "Refresh summary: "
        f"symbol={summary.symbol}, "
        f"range={summary.min_date}..{summary.max_date}, "
        f"rows={summary.row_count}, "
        f"backup={summary.backup_path}"
    )


def print_dataset_refresh_summary(dataset: DatasetConfig, summary: RefreshSummary) -> None:
    print(
        f"Refreshed dataset '{dataset.id}': "
        f"symbol={summary.symbol}, "
        f"range={summary.min_date}..{summary.max_date}, "
        f"rows={summary.row_count}, "
        f"backup={summary.backup_path}"
    )


def print_dataset_list(datasets: list[DatasetConfig]) -> None:
    print("Available datasets:\n")
    for dataset in sort_datasets_for_display(datasets):
        refresh_text = "yes" if dataset.supports_refresh else "no"
        print(f"- {dataset.id} | file: {dataset.file_name} | refresh: {refresh_text}")

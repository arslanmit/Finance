"""CLI entrypoint and wizard flow."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from .analysis import build_default_output_path, get_indicator_registry
from .catalog import discover_datasets, get_dataset, import_dataset, remove_dataset
from .create import create_symbol_dataset
from .errors import FinanceCliError
from .matrix import (
    build_matrix_jobs,
    build_matrix_output_dir,
    build_matrix_output_path,
    run_matrix_jobs,
    slugify_rule,
    write_matrix_manifest,
)
from .models import AnalysisConfig, DatasetConfig, ResolvedSource
from .presentation import (
    print_dataset_list,
    print_dataset_refresh_summary,
)
from .run_workflow import execute_analysis, refresh_generated_datasets
from .sources import resolve_custom_source, resolve_dataset_source
from .wizard import build_wizard_menu_items, run_wizard


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
    parser = build_parser()

    try:
        if not args_list:
            run_wizard()
            return 0

        try:
            args = parser.parse_args(args_list)
        except SystemExit as exc:
            return exc.code if isinstance(exc.code, int) else 1
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

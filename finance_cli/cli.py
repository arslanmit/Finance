"""CLI entrypoint and wizard flow."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from .analysis import (
    analyze_dataframe,
    build_default_output_path,
    prepare_dataframe,
    render_filtered_rows,
    save_dataframe,
)
from .errors import FinanceCliError
from .models import DatasetConfig, ResolvedSource
from .refresh import refresh_selected_source
from .registry import add_dataset, get_dataset, load_registry, remove_dataset, save_registry
from .sources import load_dataframe, resolve_custom_source, resolve_dataset_source


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Finance dataset analysis CLI with guided and command-based workflows."
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run moving-average analysis.")
    run_group = run_parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument("--dataset", help="Dataset id from datasets.json.")
    run_group.add_argument("--file", help="Path to a CSV or Excel file.")
    run_parser.add_argument("--sheet", help="Excel sheet name for --file inputs.")
    run_parser.add_argument("--months", type=int, required=True, help="Moving average window size.")
    run_parser.add_argument("--output", help="Optional output file path.")
    run_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh a registered live dataset before analysis.",
    )

    datasets_parser = subparsers.add_parser("datasets", help="Manage registered datasets.")
    datasets_subparsers = datasets_parser.add_subparsers(dest="datasets_command", required=True)

    datasets_subparsers.add_parser("list", help="List all registered datasets.")

    add_parser = datasets_subparsers.add_parser("add", help="Add a dataset to datasets.json.")
    add_parser.add_argument("--id", required=True, help="Unique dataset id.")
    add_parser.add_argument("--label", required=True, help="Friendly dataset label.")
    add_parser.add_argument("--path", required=True, help="Path to the dataset file.")
    add_parser.add_argument("--sheet", help="Sheet name for Excel datasets.")
    add_parser.add_argument(
        "--refresh-symbol",
        help="Yahoo Finance symbol for live refresh support. Only valid for .xlsx datasets.",
    )

    remove_parser = datasets_subparsers.add_parser("remove", help="Remove a registered dataset.")
    remove_parser.add_argument("--id", required=True, help="Dataset id to remove.")

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
    if args.command == "datasets":
        handle_datasets_command(args)
        return 0
    raise FinanceCliError("Unknown command.")


def handle_run_command(args: argparse.Namespace) -> None:
    datasets = load_registry() if args.dataset else []
    source = resolve_run_source(args, datasets)
    output_path = Path(args.output).expanduser() if args.output else build_default_output_path(source.input_path)
    execute_analysis(source, months=args.months, output_path=output_path, refresh_requested=args.refresh)


def resolve_run_source(args: argparse.Namespace, datasets: list[DatasetConfig]) -> ResolvedSource:
    if args.dataset:
        if args.sheet:
            raise FinanceCliError("--sheet can only be used together with --file.")
        dataset = get_dataset(datasets, args.dataset)
        return resolve_dataset_source(dataset)

    return resolve_custom_source(args.file, sheet_name=args.sheet, interactive=False)


def handle_datasets_command(args: argparse.Namespace) -> None:
    if args.datasets_command == "list":
        print_dataset_list(load_registry())
        return

    datasets = load_registry()
    if args.datasets_command == "add":
        dataset = add_dataset(
            datasets,
            dataset_id=args.id,
            label=args.label,
            path=args.path,
            sheet=args.sheet,
            refresh_symbol=args.refresh_symbol,
        )
        save_registry(datasets)
        print(f"Added dataset '{dataset.id}' -> {dataset.path}")
        return

    if args.datasets_command == "remove":
        removed = remove_dataset(datasets, args.id)
        save_registry(datasets)
        print(f"Removed dataset '{removed.id}'")
        return

    raise FinanceCliError("Unknown datasets command.")


def run_wizard() -> None:
    datasets = load_registry()
    source = prompt_for_source(datasets)
    months = prompt_for_months()
    refresh_requested = source.dataset is not None and source.dataset.supports_refresh and prompt_yes_no(
        "Refresh live data first?",
        default=False,
    )

    default_output = build_default_output_path(source.input_path)
    output_path = prompt_for_output_path(default_output)
    execute_analysis(source, months=months, output_path=output_path, refresh_requested=refresh_requested)


def prompt_for_source(datasets: list[DatasetConfig]) -> ResolvedSource:
    print("Choose a dataset:\n")
    for index, dataset in enumerate(datasets, start=1):
        refresh_tag = " [refresh available]" if dataset.supports_refresh else ""
        print(f"{index}. {dataset.id} - {dataset.label} ({dataset.file_name}){refresh_tag}")
    custom_index = len(datasets) + 1
    print(f"{custom_index}. custom - Use your own file path")

    while True:
        response = input("\nEnter a dataset number or alias: ").strip()
        if not response:
            continue

        if response.isdigit():
            selection = int(response)
            if 1 <= selection <= len(datasets):
                return resolve_dataset_source(datasets[selection - 1])
            if selection == custom_index:
                return prompt_for_custom_source()

        for dataset in datasets:
            if response == dataset.id:
                return resolve_dataset_source(dataset)

        if response.lower() == "custom":
            return prompt_for_custom_source()

        print("Invalid selection. Choose a listed number, dataset alias, or 'custom'.")


def prompt_for_custom_source() -> ResolvedSource:
    while True:
        custom_path = input("Enter the file path: ").strip()
        if not custom_path:
            print("Please enter a file path.")
            continue

        try:
            return resolve_custom_source(
                custom_path,
                interactive=True,
                choose_sheet=prompt_for_sheet_choice,
            )
        except FinanceCliError as exc:
            print(f"Error: {exc}")


def prompt_for_sheet_choice(sheet_names: list[str], input_path: Path) -> str:
    print(f"\nMultiple sheets were found in {input_path.name}:")
    for index, sheet_name in enumerate(sheet_names, start=1):
        print(f"{index}. {sheet_name}")

    while True:
        response = input("Choose a sheet number: ").strip()
        if response.isdigit():
            selection = int(response)
            if 1 <= selection <= len(sheet_names):
                return sheet_names[selection - 1]
        print("Invalid sheet selection.")


def prompt_for_months() -> int:
    while True:
        response = input("Enter the moving average window (examples: 3, 6, 12): ").strip()
        try:
            return int(response)
        except ValueError:
            print("Please enter a whole number, for example 3, 6, or 12.")


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
    months: int,
    output_path: Path,
    refresh_requested: bool,
) -> None:
    if refresh_requested:
        summary = refresh_selected_source(source)
        print_refresh_summary(summary)

    raw_dataframe = load_dataframe(source.input_path, source.sheet_name)
    prepared_dataframe = prepare_dataframe(raw_dataframe, months)
    analyzed_dataframe = analyze_dataframe(prepared_dataframe, months)

    print(render_filtered_rows(analyzed_dataframe))
    save_dataframe(analyzed_dataframe, output_path)
    print(f"\nProcessed data saved to: {output_path}")


def print_refresh_summary(summary) -> None:
    print(
        "Refresh summary: "
        f"symbol={summary.symbol}, "
        f"range={summary.min_date}..{summary.max_date}, "
        f"rows={summary.row_count}, "
        f"backup={summary.backup_path}"
    )


def print_dataset_list(datasets: list[DatasetConfig]) -> None:
    print("Available datasets:\n")
    for dataset in datasets:
        refresh_text = "yes" if dataset.supports_refresh else "no"
        print(
            f"- {dataset.id}: {dataset.label} | file: {dataset.file_name} | "
            f"path: {dataset.path} | refresh: {refresh_text}"
        )

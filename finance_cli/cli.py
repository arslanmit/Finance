"""CLI entrypoint and wizard flow."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .analysis import (
    analyze_dataframe,
    build_default_output_path,
    prepare_dataframe,
    render_filtered_rows,
    save_dataframe,
)
from .catalog import discover_datasets, get_dataset, import_dataset, remove_dataset
from .create import create_symbol_dataset
from .errors import FinanceCliError
from .models import DatasetConfig, RefreshSummary, ResolvedSource
from .refresh import refresh_selected_source
from .sources import ensure_symbol_column, load_dataframe, resolve_custom_source, resolve_dataset_source


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Finance dataset analysis CLI with guided and command-based workflows."
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run moving-average analysis.")
    run_group = run_parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument("--dataset", help="Dataset id discovered from the managed CSV folders.")
    run_group.add_argument("--file", help="Path to a CSV file.")
    run_parser.add_argument("--months", type=int, required=True, help="Moving average window size.")
    run_parser.add_argument("--output", help="Optional output CSV path.")
    run_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh a discovered live dataset before analysis.",
    )

    datasets_parser = subparsers.add_parser("datasets", help="Manage discovered datasets.")
    datasets_subparsers = datasets_parser.add_subparsers(dest="datasets_command", required=True)

    datasets_subparsers.add_parser("list", help="List all discovered datasets.")

    add_parser = datasets_subparsers.add_parser(
        "add",
        help="Import a CSV into the managed dataset folders.",
    )
    add_parser.add_argument("--id", required=True, help="Unique dataset id.")
    add_parser.add_argument("--label", help="Ignored compatibility flag.")
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

    remove_parser = datasets_subparsers.add_parser("remove", help="Remove a discovered dataset.")
    remove_parser.add_argument("--id", required=True, help="Dataset id to remove.")

    refresh_parser = datasets_subparsers.add_parser(
        "refresh",
        help="Refresh discovered live datasets from their configured provider.",
    )
    refresh_group = refresh_parser.add_mutually_exclusive_group(required=True)
    refresh_group.add_argument("--id", help="Dataset id to refresh.")
    refresh_group.add_argument(
        "--all",
        action="store_true",
        help="Refresh all discovered datasets that support live refresh.",
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
    if args.command == "datasets":
        handle_datasets_command(args)
        return 0
    raise FinanceCliError("Unknown command.")


def handle_run_command(args: argparse.Namespace) -> None:
    datasets = discover_datasets() if args.dataset else []
    source = resolve_run_source(args, datasets)
    output_path = Path(args.output).expanduser() if args.output else build_default_output_path(source.input_path)
    execute_analysis(source, months=args.months, output_path=output_path, refresh_requested=args.refresh)


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
            dataset_id=args.id,
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
        refreshed = refresh_discovered_datasets(
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
    refresh_requested = (
        not selection.created_now
        and source.dataset is not None
        and source.dataset.supports_refresh
        and prompt_yes_no("Refresh live data first?", default=False)
    )

    default_output = build_default_output_path(source.input_path)
    output_path = prompt_for_output_path(default_output)
    execute_analysis(source, months=months, output_path=output_path, refresh_requested=refresh_requested)


def prompt_for_source(datasets: list[DatasetConfig]) -> WizardSourceChoice:
    print("Choose a dataset:\n")
    menu_items = build_wizard_menu_items(datasets)

    primary_items = [item for item in menu_items if item.action in {"dataset-default", "create", "custom"}]
    other_items = [item for item in menu_items if item.action == "dataset-other"]

    for index, item in enumerate(primary_items, start=1):
        print(f"{index}. {item.label}")

    if other_items:
        print("others:")
        offset = len(primary_items)
        for index, item in enumerate(other_items, start=offset + 1):
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

        print("Invalid selection. Choose a listed number, dataset alias, 'custom', or 'create'.")


def build_wizard_menu_items(datasets: list[DatasetConfig]) -> list[WizardMenuItem]:
    default_dataset = next((dataset for dataset in datasets if dataset.id == "default"), None)
    other_datasets = [dataset for dataset in datasets if dataset.id != "default"]

    menu_items: list[WizardMenuItem] = []
    if default_dataset is not None:
        menu_items.append(
            WizardMenuItem(
                alias=default_dataset.id,
                label=dataset_menu_label(default_dataset),
                action="dataset-default",
                dataset=default_dataset,
            )
        )

    menu_items.append(
        WizardMenuItem(
            alias="create",
            label="create - Create a new dataset from a Yahoo symbol",
            action="create",
        )
    )
    menu_items.append(
        WizardMenuItem(
            alias="custom",
            label="custom - Use your own CSV file path",
            action="custom",
        )
    )

    for dataset in other_datasets:
        menu_items.append(
            WizardMenuItem(
                alias=dataset.id,
                label=dataset_menu_label(dataset),
                action="dataset-other",
                dataset=dataset,
            )
        )

    return menu_items


def dataset_menu_label(dataset: DatasetConfig) -> str:
    refresh_tag = " [refresh available]" if dataset.supports_refresh else ""
    return f"{dataset.id} ({dataset.file_name}){refresh_tag}"


def select_wizard_menu_item(
    item: WizardMenuItem,
) -> WizardSourceChoice:
    if item.action in {"dataset-default", "dataset-other"}:
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

    raw_dataframe = load_dataframe(source.input_path)
    raw_dataframe = ensure_symbol_column(
        raw_dataframe,
        None if source.dataset is None else source.dataset.symbol,
    )
    prepared_dataframe = prepare_dataframe(raw_dataframe, months)
    analyzed_dataframe = analyze_dataframe(prepared_dataframe, months)

    print(render_filtered_rows(analyzed_dataframe))
    save_dataframe(analyzed_dataframe, output_path)
    print(f"\nProcessed data saved to: {output_path}")


def refresh_discovered_datasets(
    datasets: list[DatasetConfig],
    *,
    dataset_id: str | None,
    refresh_all: bool,
) -> list[tuple[DatasetConfig, RefreshSummary]]:
    if refresh_all:
        targets = [dataset for dataset in datasets if dataset.supports_refresh]
        if not targets:
            raise FinanceCliError("No discovered datasets support live refresh.")
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
    for dataset in datasets:
        refresh_text = "yes" if dataset.supports_refresh else "no"
        print(
            f"- {dataset.id}: {dataset.id} | file: {dataset.file_name} | "
            f"path: {dataset.path} | refresh: {refresh_text}"
        )

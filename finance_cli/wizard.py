"""Interactive wizard helpers for selecting and running analyses."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .analysis import build_default_output_path, format_rule, get_indicator_registry, parse_rule
from .catalog import discover_datasets
from .create import create_symbol_dataset
from .errors import FinanceCliError
from .models import AnalysisConfig, DatasetConfig, ResolvedSource
from .presentation import sort_datasets_for_display
from .run_workflow import execute_analysis
from .sources import resolve_custom_source, resolve_dataset_source


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
        ),
        WizardMenuItem(
            alias="custom",
            label="custom - Use your own CSV file path",
            action="custom",
        ),
    ]
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


def select_wizard_menu_item(item: WizardMenuItem) -> WizardSourceChoice:
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

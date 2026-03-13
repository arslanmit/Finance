"""Input loops for the interactive wizard."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable

from .analysis import format_rule, get_indicator_registry, parse_rule
from .errors import FinanceCliError

if TYPE_CHECKING:
    from .models import DatasetConfig
    from .wizard import WizardMenuItem, WizardSourceChoice


def prompt_for_source(
    datasets: list["DatasetConfig"],
    *,
    build_menu_items: Callable[[list["DatasetConfig"]], list["WizardMenuItem"]],
    select_menu_item: Callable[["WizardMenuItem"], "WizardSourceChoice"],
) -> "WizardSourceChoice":
    print("Choose a dataset:\n")
    menu_items = build_menu_items(datasets)

    for index, item in enumerate(menu_items, start=1):
        print(f"{index}. {item.label}")

    while True:
        response = input("\nEnter a dataset number or alias: ").strip()
        if not response:
            continue

        if response.isdigit():
            selection = int(response)
            if 1 <= selection <= len(menu_items):
                return select_menu_item(menu_items[selection - 1])

        for item in menu_items:
            if response.lower() == item.alias:
                return select_menu_item(item)

        print("Invalid selection. Choose a listed number, dataset alias, 'create', or 'custom'.")


def prompt_for_custom_source(
    *,
    resolve_custom_source: Callable[[str], "WizardSourceChoice"],
) -> "WizardSourceChoice":
    while True:
        custom_path = input("Enter the CSV file path: ").strip()
        if not custom_path:
            print("Please enter a file path.")
            continue

        try:
            return resolve_custom_source(custom_path)
        except FinanceCliError as exc:
            print(f"Error: {exc}")


def prompt_for_symbol_dataset(
    *,
    create_symbol_dataset: Callable[[str], object],
    resolve_dataset_source: Callable[[object], "WizardSourceChoice"],
) -> "WizardSourceChoice":
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
            return resolve_dataset_source(dataset)
        except FinanceCliError as exc:
            print(f"Error: {exc}")


def prompt_for_months() -> int:
    while True:
        response = input("Enter the indicator window (examples: 3, 6, 12): ").strip()
        try:
            return int(response)
        except ValueError:
            print("Please enter a whole number, for example 3, 6, or 12.")


def prompt_for_indicator(available: list[str] | None = None) -> str:
    indicator_options = available or get_indicator_registry().list_indicators()
    default_indicator = "sma"
    while True:
        response = input(
            f"Enter the indicator [{default_indicator}] (available: {', '.join(indicator_options)}): "
        ).strip()
        indicator = default_indicator if not response else response.lower()
        if indicator in indicator_options:
            return indicator
        print(f"Please choose one of: {', '.join(indicator_options)}.")


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


__all__ = [
    "prompt_for_custom_source",
    "prompt_for_indicator",
    "prompt_for_months",
    "prompt_for_output_path",
    "prompt_for_rule",
    "prompt_for_source",
    "prompt_for_symbol_dataset",
    "prompt_yes_no",
]

"""Interactive wizard helpers for selecting and running analyses."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .analysis import build_default_output_path
from .catalog import discover_datasets
from .create import create_symbol_dataset
from .errors import FinanceCliError
from .models import AnalysisConfig, DatasetConfig, ResolvedSource
from .presentation import sort_datasets_for_display
from .run_workflow import execute_analysis
from .sources import resolve_custom_source, resolve_dataset_source
from .wizard_prompts import (
    prompt_for_custom_source as prompt_for_custom_source_impl,
    prompt_for_indicator as prompt_for_indicator_impl,
    prompt_for_months as prompt_for_months_impl,
    prompt_for_output_path as prompt_for_output_path_impl,
    prompt_for_rule as prompt_for_rule_impl,
    prompt_for_source as prompt_for_source_impl,
    prompt_for_symbol_dataset as prompt_for_symbol_dataset_impl,
    prompt_yes_no as prompt_yes_no_impl,
)


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
    return prompt_for_source_impl(
        datasets,
        build_menu_items=build_wizard_menu_items,
        select_menu_item=select_wizard_menu_item,
    )


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
    return prompt_for_custom_source_impl(
        resolve_custom_source=lambda custom_path: WizardSourceChoice(resolve_custom_source(custom_path))
    )


def prompt_for_symbol_dataset() -> WizardSourceChoice:
    return prompt_for_symbol_dataset_impl(
        create_symbol_dataset=create_symbol_dataset,
        resolve_dataset_source=lambda dataset: WizardSourceChoice(
            resolve_dataset_source(dataset), created_now=True
        ),
    )


def prompt_for_months() -> int:
    return prompt_for_months_impl()


def prompt_for_indicator() -> str:
    return prompt_for_indicator_impl()


def prompt_for_rule() -> str:
    return prompt_for_rule_impl()


def prompt_for_output_path(default_output: Path) -> Path:
    return prompt_for_output_path_impl(default_output)


def prompt_yes_no(prompt: str, default: bool = False) -> bool:
    return prompt_yes_no_impl(prompt, default=default)


__all__ = [
    "WizardMenuItem",
    "WizardSourceChoice",
    "build_wizard_menu_items",
    "dataset_menu_label",
    "prompt_for_custom_source",
    "prompt_for_indicator",
    "prompt_for_months",
    "prompt_for_output_path",
    "prompt_for_rule",
    "prompt_for_source",
    "prompt_for_symbol_dataset",
    "prompt_yes_no",
    "run_wizard",
    "select_wizard_menu_item",
]

from pathlib import Path

import pytest

from finance_cli.models import DatasetConfig, RefreshMetadata
from finance_cli.wizard import WizardMenuItem, WizardSourceChoice, build_wizard_menu_items
from finance_cli.wizard_prompts import (
    prompt_for_indicator,
    prompt_for_months,
    prompt_for_output_path,
    prompt_for_rule,
    prompt_for_source,
    prompt_yes_no,
)


def build_dataset(dataset_id: str, symbol: str | None = None) -> DatasetConfig:
    refresh = None if symbol is None else RefreshMetadata(provider="yahoo", symbol=symbol)
    return DatasetConfig(
        id=dataset_id,
        label=dataset_id,
        path=f"data/generated/{dataset_id}.csv",
        refresh=refresh,
        base_dir=Path("/tmp"),
    )


def test_build_wizard_menu_items_shows_actions_before_datasets() -> None:
    items = build_wizard_menu_items(
        [
            build_dataset("nvda", "NVDA"),
            build_dataset("500_pa", "500.PA"),
        ]
    )

    assert [item.alias for item in items] == ["create", "custom", "500_pa", "nvda"]
    assert items[0].label == "create - Create a new dataset from a Yahoo symbol"
    assert items[2].label == "500_pa [refresh available]"


def test_prompt_for_source_accepts_dataset_alias(monkeypatch) -> None:
    items = [
        WizardMenuItem(alias="create", label="create", action="create"),
        WizardMenuItem(alias="custom", label="custom", action="custom"),
        WizardMenuItem(alias="nvda", label="nvda", action="dataset", dataset=build_dataset("nvda", "NVDA")),
    ]
    expected = WizardSourceChoice(source=object())  # type: ignore[arg-type]
    monkeypatch.setattr("builtins.input", lambda _: "nvda")

    selection = prompt_for_source(
        [build_dataset("nvda", "NVDA")],
        build_menu_items=lambda datasets: items,
        select_menu_item=lambda item: expected,
    )

    assert selection is expected


def test_prompt_for_indicator_retries_until_supported(monkeypatch) -> None:
    responses = iter(["bad", "EMA"])
    monkeypatch.setattr("builtins.input", lambda _: next(responses))

    assert prompt_for_indicator(["ema", "sma", "wma"]) == "ema"


def test_prompt_for_months_retries_until_integer(monkeypatch) -> None:
    responses = iter(["nope", "6"])
    monkeypatch.setattr("builtins.input", lambda _: next(responses))

    assert prompt_for_months() == 6


def test_prompt_for_rule_uses_default_when_blank(monkeypatch) -> None:
    monkeypatch.setattr("builtins.input", lambda _: "")

    assert prompt_for_rule() == "indicator > open"


def test_prompt_for_output_path_accepts_default(monkeypatch) -> None:
    monkeypatch.setattr("builtins.input", lambda _: "")

    assert prompt_for_output_path(Path("output/nvda_processed.csv")) == Path("output/nvda_processed.csv")


@pytest.mark.parametrize(
    ("response", "default", "expected"),
    [
        ("", True, True),
        ("", False, False),
        ("y", False, True),
        ("n", True, False),
    ],
)
def test_prompt_yes_no_handles_default_and_explicit_answers(
    monkeypatch,
    response: str,
    default: bool,
    expected: bool,
) -> None:
    monkeypatch.setattr("builtins.input", lambda _: response)

    assert prompt_yes_no("Refresh?", default=default) is expected

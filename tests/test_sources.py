from pathlib import Path

import pandas as pd
import pytest

from finance_cli.errors import SourceError
from finance_cli.models import DatasetConfig
from finance_cli.sources import (
    load_dataframe,
    resolve_custom_source,
    resolve_dataset_source,
)


def write_csv(path: Path) -> None:
    pd.DataFrame(
        {
            "Date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "Open": [10, 11, 12],
        }
    ).to_csv(path, index=False)


def write_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path) as writer:
        for sheet_name, dataframe in sheets.items():
            dataframe.to_excel(writer, sheet_name=sheet_name, index=False)


def test_resolve_dataset_source_for_registered_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    dataset = DatasetConfig(
        id="sample",
        label="Sample CSV",
        path="sample.csv",
        sheet=None,
        refresh=None,
        base_dir=tmp_path,
    )

    source = resolve_dataset_source(dataset)

    assert source.input_path == csv_path.resolve(strict=False)
    assert source.sheet_name is None
    assert source.dataset == dataset


def test_resolve_custom_source_auto_selects_single_sheet(tmp_path: Path) -> None:
    workbook_path = tmp_path / "single.xlsx"
    write_workbook(
        workbook_path,
        {"Sheet1": pd.DataFrame({"date": ["2024-01-01"], "open": [10]})},
    )

    source = resolve_custom_source(workbook_path)

    assert source.sheet_name == "Sheet1"


def test_resolve_custom_source_errors_for_multi_sheet_without_prompt(tmp_path: Path) -> None:
    workbook_path = tmp_path / "multi.xlsx"
    write_workbook(
        workbook_path,
        {
            "First": pd.DataFrame({"date": ["2024-01-01"], "open": [10]}),
            "Second": pd.DataFrame({"date": ["2024-02-01"], "open": [11]}),
        },
    )

    with pytest.raises(SourceError, match="multiple sheets"):
        resolve_custom_source(workbook_path)


def test_resolve_custom_source_uses_sheet_prompt_callback(tmp_path: Path) -> None:
    workbook_path = tmp_path / "multi.xlsx"
    write_workbook(
        workbook_path,
        {
            "First": pd.DataFrame({"date": ["2024-01-01"], "open": [10]}),
            "Second": pd.DataFrame({"date": ["2024-02-01"], "open": [11]}),
        },
    )

    source = resolve_custom_source(
        workbook_path,
        interactive=True,
        choose_sheet=lambda names, _path: names[1],
    )

    assert source.sheet_name == "Second"


def test_load_dataframe_normalizes_column_names(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)

    dataframe = load_dataframe(csv_path, sheet_name=None)

    assert list(dataframe.columns) == ["date", "open"]

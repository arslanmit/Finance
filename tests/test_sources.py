from pathlib import Path

import pandas as pd
import pytest

from finance_cli.errors import SourceError
from finance_cli.models import DatasetConfig
from finance_cli.sources import (
    ensure_symbol_column,
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


def test_resolve_dataset_source_for_discovered_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    dataset = DatasetConfig(
        id="sample",
        label="Sample CSV",
        path="sample.csv",
        refresh=None,
        base_dir=tmp_path,
    )

    source = resolve_dataset_source(dataset)

    assert source.input_path == csv_path.resolve(strict=False)
    assert source.dataset == dataset


def test_resolve_custom_source_for_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)

    source = resolve_custom_source(csv_path)

    assert source.input_path == csv_path
    assert source.dataset is None


def test_resolve_custom_source_rejects_excel_extensions(tmp_path: Path) -> None:
    workbook_path = tmp_path / "legacy.xlsx"
    workbook_path.write_text("placeholder", encoding="utf-8")

    with pytest.raises(SourceError, match=r"Unsupported input format '.xlsx'.*\.csv"):
        resolve_custom_source(workbook_path)


def test_load_dataframe_normalizes_column_names(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)

    dataframe = load_dataframe(csv_path)

    assert list(dataframe.columns) == ["date", "open"]


def test_ensure_symbol_column_inserts_symbol_first() -> None:
    dataframe = pd.DataFrame({"date": ["2024-01-01"], "open": [10]})

    normalized = ensure_symbol_column(dataframe, symbol="NVDA")

    assert list(normalized.columns) == ["symbol", "date", "open"]
    assert normalized.iloc[0]["symbol"] == "NVDA"


def test_ensure_symbol_column_reorders_existing_symbol_column() -> None:
    dataframe = pd.DataFrame({"date": ["2024-01-01"], "symbol": [""], "open": [10]})

    normalized = ensure_symbol_column(dataframe, symbol="SPY")

    assert list(normalized.columns) == ["symbol", "date", "open"]
    assert normalized.iloc[0]["symbol"] == "SPY"

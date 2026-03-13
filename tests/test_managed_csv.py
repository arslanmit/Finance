from pathlib import Path

import pandas as pd
import pytest

from finance_cli.managed_csv import create_backup, load_existing_csv_data, write_managed_dataset_csv
from finance_cli.models import DatasetConfig, RefreshMetadata, ResolvedSource
from finance_cli.refresh_validation import (
    validate_overlap,
    validate_refreshable_source,
    validate_source_contiguity,
)
from finance_cli.refresh_yahoo import build_chart_url


def test_build_chart_url_embeds_symbol_and_periods() -> None:
    assert (
        build_chart_url("SPY", 123, 456)
        == "https://query1.finance.yahoo.com/v8/finance/chart/SPY"
        "?interval=1mo&period1=123&period2=456"
        "&includePrePost=false&events=div%2Csplits"
    )


def test_write_managed_dataset_csv_sorts_descending_and_keeps_symbol_first(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "data" / "generated" / "spy.csv"
    source = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-01", "2024-03-01"]),
            "open": [12.0, 11.0],
            "high": [13.0, 12.0],
            "low": [11.0, 10.0],
            "close": [12.5, 11.5],
            "volume": [110, 120],
        }
    )

    write_managed_dataset_csv(output_path, source, "SPY")

    written = pd.read_csv(output_path)
    assert list(written.columns) == ["symbol", "date", "open", "high", "low", "close", "volume"]
    assert written.iloc[0]["symbol"] == "SPY"
    assert written.iloc[0]["date"] == "2024-03-01"


def test_load_existing_csv_data_normalizes_dates_and_drops_symbol(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        "symbol,Date,Open,High,Low,Close,Volume\nSPY,2024-03-01,11,12,10,11.5,100\n",
        encoding="utf-8",
    )

    loaded = load_existing_csv_data(csv_path)

    assert list(loaded.columns) == ["date", "open", "high", "low", "close", "volume"]
    assert str(loaded.loc[0, "date"].date()) == "2024-03-01"


def test_create_backup_uses_refresh_backup_directory(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("date,open\n2024-03-01,11\n", encoding="utf-8")

    backup_path = create_backup(csv_path)
    resolved_backup_path = (tmp_path / backup_path).resolve()

    assert resolved_backup_path.parent == tmp_path / "tmp" / "refresh_backups"
    assert resolved_backup_path.read_text(encoding="utf-8") == csv_path.read_text(encoding="utf-8")


def test_validate_refreshable_source_rejects_custom_files(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("date,open\n2024-03-01,11\n", encoding="utf-8")

    with pytest.raises(Exception, match="generated datasets"):
        validate_refreshable_source(ResolvedSource(input_path=csv_path, dataset=None))


def test_validate_source_contiguity_rejects_missing_months() -> None:
    source = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-03-01"]),
            "open": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.0, 10.0],
            "close": [10.5, 11.5],
            "volume": [100, 110],
        }
    )

    with pytest.raises(Exception, match="missing dates"):
        validate_source_contiguity(source)


def test_validate_overlap_accepts_expected_reference_months() -> None:
    reference_dates = pd.to_datetime(
        ["2024-08-01", "2024-09-01", "2024-10-01", "2024-11-01"]
    )
    existing = pd.DataFrame(
        {
            "date": reference_dates,
            "open": [10.0, 11.0, 12.0, 13.0],
            "high": [11.0, 12.0, 13.0, 14.0],
            "low": [9.0, 10.0, 11.0, 12.0],
            "close": [10.5, 11.5, 12.5, 13.5],
            "volume": [100, 110, 120, 130],
        }
    )
    source = existing.copy()

    validate_overlap(existing, source)


def test_validate_refreshable_source_accepts_yahoo_csv_dataset(tmp_path: Path) -> None:
    dataset_path = tmp_path / "sample.csv"
    dataset_path.write_text("", encoding="utf-8")
    dataset = DatasetConfig(
        id="sample",
        label="sample",
        path="sample.csv",
        refresh=RefreshMetadata(provider="yahoo", symbol="SPY"),
        base_dir=tmp_path,
    )

    validate_refreshable_source(ResolvedSource(input_path=dataset_path, dataset=dataset))

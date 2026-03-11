from pathlib import Path

import pandas as pd
import pytest

from finance_cli.errors import RefreshError
from finance_cli.models import DatasetConfig, RefreshMetadata, ResolvedSource
from finance_cli.refresh import (
    create_backup,
    get_primary_live_dataset_path,
    refresh_default_dataset,
    refresh_yahoo_monthly_csv,
    validate_refreshable_source,
    write_source_csv,
)


def write_csv(path: Path, include_symbol: bool = False) -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01"],
            "open": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.0, 10.0],
            "close": [10.5, 11.5],
            "volume": [100, 110],
        }
    )
    if include_symbol:
        dataframe.insert(0, "symbol", "SPY")
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path, index=False)


def test_validate_refresh_rejects_custom_file(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("date,open\n2024-01-01,10\n", encoding="utf-8")
    source = ResolvedSource(input_path=csv_path, dataset=None)

    with pytest.raises(RefreshError, match="discovered datasets"):
        validate_refreshable_source(source)


def test_validate_refresh_rejects_non_csv_dataset(tmp_path: Path) -> None:
    dataset_path = tmp_path / "sample.xlsx"
    dataset_path.write_text("placeholder", encoding="utf-8")
    dataset = DatasetConfig(
        id="sample",
        label="sample",
        path="sample.xlsx",
        refresh=RefreshMetadata(provider="yahoo", symbol="500.PA"),
        base_dir=tmp_path,
    )
    source = ResolvedSource(input_path=dataset_path, dataset=dataset)

    with pytest.raises(RefreshError, match=r"only \.csv datasets"):
        validate_refreshable_source(source)


def test_validate_refresh_rejects_unsupported_provider(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("", encoding="utf-8")
    dataset = DatasetConfig(
        id="sample",
        label="sample",
        path="sample.csv",
        refresh=RefreshMetadata(provider="custom", symbol="500.PA"),
        base_dir=tmp_path,
    )
    source = ResolvedSource(input_path=csv_path, dataset=dataset)

    with pytest.raises(RefreshError, match="unsupported refresh provider"):
        validate_refreshable_source(source)


def test_write_source_csv_preserves_symbol_first_column(tmp_path: Path) -> None:
    csv_path = tmp_path / "spy.csv"
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

    write_source_csv(csv_path, source, "SPY")

    written = pd.read_csv(csv_path)
    assert list(written.columns) == ["symbol", "date", "open", "high", "low", "close", "volume"]
    assert written.iloc[0]["symbol"] == "SPY"
    assert written.iloc[0]["date"] == "2024-03-01"


def test_create_backup_uses_csv_refresh_backup_directory(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)

    backup_path = create_backup(csv_path)
    resolved_backup_path = (tmp_path / backup_path).resolve()

    assert resolved_backup_path.parent == tmp_path / "tmp" / "refresh_backups"
    assert backup_path.suffix == ".csv"
    assert resolved_backup_path.read_text(encoding="utf-8") == csv_path.read_text(encoding="utf-8")


def test_refresh_yahoo_monthly_csv_rewrites_csv_and_returns_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    csv_path = tmp_path / "data" / "live" / "live.csv"
    write_csv(csv_path, include_symbol=True)
    fetched = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-04-01", "2024-05-01"]),
            "open": [13.0, 14.0],
            "high": [14.0, 15.0],
            "low": [12.0, 13.0],
            "close": [13.5, 14.5],
            "volume": [130, 140],
        }
    )

    monkeypatch.setattr(
        "finance_cli.refresh.fetch_full_history_monthly_source",
        lambda symbol: fetched,
    )
    monkeypatch.setattr("finance_cli.refresh.validate_source_contiguity", lambda source: None)

    summary = refresh_yahoo_monthly_csv(csv_path, symbol="NVDA", strict_validation=False)

    rewritten = pd.read_csv(csv_path)
    assert summary.symbol == "NVDA"
    assert summary.row_count == 2
    assert summary.backup_path.endswith(".csv")
    assert list(rewritten.columns) == ["symbol", "date", "open", "high", "low", "close", "volume"]
    assert rewritten.iloc[0]["symbol"] == "NVDA"
    assert rewritten.iloc[0]["date"] == "2024-05-01"


def test_get_primary_live_dataset_path_is_derived_from_live_folder(tmp_path: Path) -> None:
    write_csv(tmp_path / "data" / "live" / "sp500_live.csv", include_symbol=True)
    write_csv(tmp_path / "data" / "live" / "zzz.csv", include_symbol=True)

    primary = get_primary_live_dataset_path(tmp_path)

    assert primary == (tmp_path / "data" / "live" / "sp500_live.csv").resolve(strict=False)


def test_refresh_default_dataset_uses_primary_live_file_when_not_provided(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "live" / "sp500_live.csv", include_symbol=True)

    monkeypatch.setattr(
        "finance_cli.refresh.refresh_yahoo_monthly_csv",
        lambda path, symbol, strict_validation: (path, symbol, strict_validation),
    )

    result = refresh_default_dataset()

    assert result == (
        (tmp_path / "data" / "live" / "sp500_live.csv").resolve(strict=False),
        "500.PA",
        True,
    )

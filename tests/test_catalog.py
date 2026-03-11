from pathlib import Path

import pandas as pd
import pytest

from finance_cli.catalog import discover_datasets, get_dataset, import_dataset, remove_dataset
from finance_cli.errors import CatalogError


def write_csv(path: Path, *, symbol: str | None = None) -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10, 11, 12],
            "high": [11, 12, 13],
            "low": [9, 10, 11],
            "close": [10.5, 11.5, 12.5],
            "volume": [100, 110, 120],
        }
    )
    if symbol is not None:
        dataframe.insert(0, "symbol", symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path, index=False)


def test_discover_datasets_scans_only_managed_directories(tmp_path: Path) -> None:
    write_csv(tmp_path / "data" / "live" / "default.csv", symbol="500.PA")
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA")
    write_csv(tmp_path / "data" / "imported" / "sample_imported.csv")
    write_csv(tmp_path / "data" / "ignored.csv", symbol="SPY")

    datasets = discover_datasets(tmp_path)

    assert [dataset.id for dataset in datasets] == ["default", "nvda", "sample_imported"]
    assert get_dataset("default", datasets).symbol == "500.PA"
    assert get_dataset("sample_imported", datasets).supports_refresh is False


def test_discover_datasets_rejects_duplicate_ids(tmp_path: Path) -> None:
    write_csv(tmp_path / "data" / "live" / "spy.csv", symbol="SPY")
    write_csv(tmp_path / "data" / "generated" / "spy.csv", symbol="SPY")

    with pytest.raises(CatalogError, match="Duplicate dataset id 'spy'"):
        discover_datasets(tmp_path)


def test_import_dataset_copies_csv_into_imported_folder(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    write_csv(source)

    dataset = import_dataset(
        dataset_id="sample",
        source_path=source,
        base_dir=tmp_path,
    )

    assert dataset.path == "data/imported/sample.csv"
    assert (tmp_path / dataset.path).exists()
    assert pd.read_csv(tmp_path / dataset.path).equals(pd.read_csv(source))


def test_import_dataset_writes_refreshable_live_csv_with_symbol_first(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    write_csv(source)

    dataset = import_dataset(
        dataset_id="nvda",
        source_path=source,
        refresh_symbol="nvda",
        base_dir=tmp_path,
    )

    imported = pd.read_csv(tmp_path / dataset.path)
    assert dataset.path == "data/live/nvda.csv"
    assert list(imported.columns)[:3] == ["symbol", "date", "open"]
    assert imported["symbol"].tolist() == ["NVDA", "NVDA", "NVDA"]


def test_remove_dataset_deletes_backing_file(tmp_path: Path) -> None:
    write_csv(tmp_path / "data" / "imported" / "sample.csv")

    removed = remove_dataset("sample", base_dir=tmp_path)

    assert removed.id == "sample"
    assert not (tmp_path / "data" / "imported" / "sample.csv").exists()

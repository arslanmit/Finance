from pathlib import Path

import pandas as pd
import pytest

from finance_cli.errors import RegistryError
from finance_cli.models import DatasetConfig
from finance_cli.registry import add_dataset, load_registry, remove_dataset, save_registry


def write_csv(path: Path) -> None:
    pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01"],
            "open": [10, 11],
        }
    ).to_csv(path, index=False)


def write_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path) as writer:
        for sheet_name, dataframe in sheets.items():
            dataframe.to_excel(writer, sheet_name=sheet_name, index=False)


def test_save_and_load_registry_round_trip(tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.json"
    dataset = DatasetConfig(
        id="sample",
        label="Sample",
        path="sample.csv",
        sheet=None,
        refresh=None,
        base_dir=tmp_path,
    )

    save_registry([dataset], config_path=config_path)
    loaded = load_registry(config_path=config_path)

    assert len(loaded) == 1
    assert loaded[0].id == "sample"
    assert loaded[0].path == "sample.csv"


def test_add_dataset_auto_fills_single_sheet_excel(tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.json"
    workbook_path = tmp_path / "sample.xlsx"
    write_workbook(
        workbook_path,
        {"OnlySheet": pd.DataFrame({"date": ["2024-01-01"], "open": [10]})},
    )

    datasets: list[DatasetConfig] = []
    dataset = add_dataset(
        datasets,
        dataset_id="sample",
        label="Sample",
        path="sample.xlsx",
        config_path=config_path,
    )

    assert dataset.sheet == "OnlySheet"
    assert datasets[0].id == "sample"


def test_add_dataset_requires_unique_id(tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    datasets = [
        DatasetConfig(
            id="sample",
            label="Existing",
            path="sample.csv",
            sheet=None,
            refresh=None,
            base_dir=tmp_path,
        )
    ]

    with pytest.raises(RegistryError, match="already exists"):
        add_dataset(
            datasets,
            dataset_id="sample",
            label="Duplicate",
            path="sample.csv",
            config_path=config_path,
        )


def test_add_dataset_requires_sheet_for_multi_sheet_excel(tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.json"
    workbook_path = tmp_path / "sample.xlsx"
    write_workbook(
        workbook_path,
        {
            "First": pd.DataFrame({"date": ["2024-01-01"], "open": [10]}),
            "Second": pd.DataFrame({"date": ["2024-02-01"], "open": [11]}),
        },
    )

    with pytest.raises(RegistryError, match="Use --sheet"):
        add_dataset(
            [],
            dataset_id="sample",
            label="Sample",
            path="sample.xlsx",
            config_path=config_path,
        )


def test_add_dataset_rejects_refresh_for_non_xlsx(tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)

    with pytest.raises(RegistryError, match="only with .xlsx"):
        add_dataset(
            [],
            dataset_id="sample",
            label="Sample",
            path="sample.csv",
            refresh_symbol="500.PA",
            config_path=config_path,
        )


def test_remove_dataset_removes_entry(tmp_path: Path) -> None:
    dataset = DatasetConfig(
        id="sample",
        label="Sample",
        path="sample.csv",
        sheet=None,
        refresh=None,
        base_dir=tmp_path,
    )
    datasets = [dataset]

    removed = remove_dataset(datasets, "sample")

    assert removed.id == "sample"
    assert datasets == []

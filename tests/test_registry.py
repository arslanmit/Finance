import json
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


def test_save_and_load_registry_round_trip_without_sheet(tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.json"
    dataset = DatasetConfig(
        id="sample",
        label="Sample",
        path="sample.csv",
        refresh=None,
        base_dir=tmp_path,
    )

    save_registry([dataset], config_path=config_path)
    loaded = load_registry(config_path=config_path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))

    assert len(loaded) == 1
    assert loaded[0].id == "sample"
    assert loaded[0].path == "sample.csv"
    assert "sheet" not in payload["datasets"][0]


def test_load_registry_tolerates_legacy_sheet_field(tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.json"
    config_path.write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "id": "sample",
                        "label": "Legacy",
                        "path": "sample.csv",
                        "sheet": "OldSheet",
                        "refresh": None,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    loaded = load_registry(config_path=config_path)

    assert loaded[0].id == "sample"
    assert loaded[0].path == "sample.csv"


def test_add_dataset_accepts_csv(tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)

    datasets: list[DatasetConfig] = []
    dataset = add_dataset(
        datasets,
        dataset_id="sample",
        label="Sample",
        path="sample.csv",
        config_path=config_path,
    )

    assert dataset.path == "sample.csv"
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


def test_add_dataset_rejects_excel_paths(tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.json"
    workbook_path = tmp_path / "sample.xlsx"
    workbook_path.write_text("placeholder", encoding="utf-8")

    with pytest.raises(RegistryError, match=r"Unsupported dataset format '.xlsx'.*\.csv"):
        add_dataset(
            [],
            dataset_id="sample",
            label="Sample",
            path="sample.xlsx",
            config_path=config_path,
        )


def test_add_dataset_allows_refresh_for_csv(tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)

    dataset = add_dataset(
        [],
        dataset_id="sample",
        label="Sample",
        path="sample.csv",
        refresh_symbol="500.PA",
        config_path=config_path,
    )

    assert dataset.refresh is not None
    assert dataset.refresh.symbol == "500.PA"


def test_remove_dataset_removes_entry(tmp_path: Path) -> None:
    dataset = DatasetConfig(
        id="sample",
        label="Sample",
        path="sample.csv",
        refresh=None,
        base_dir=tmp_path,
    )
    datasets = [dataset]

    removed = remove_dataset(datasets, "sample")

    assert removed.id == "sample"
    assert datasets == []

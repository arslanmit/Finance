from pathlib import Path

import pandas as pd
import pytest

from finance_cli.create import build_generated_dataset_path, create_symbol_dataset, normalize_symbol_slug
from finance_cli.errors import CreationError


def monthly_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
            "open": [10.0, 12.0, 11.0],
            "high": [11.0, 13.0, 12.0],
            "low": [9.0, 11.0, 10.0],
            "close": [10.5, 12.5, 11.5],
            "volume": [100, 110, 120],
        }
    )


def test_normalize_symbol_slug() -> None:
    assert normalize_symbol_slug("500.PA") == "500_pa"
    assert normalize_symbol_slug(" SPY ") == "spy"
    assert normalize_symbol_slug("BRK.B") == "brk_b"


def test_build_generated_dataset_path() -> None:
    assert build_generated_dataset_path("spy") == Path("data/generated/spy.csv")


def test_create_dataset_writes_csv_into_generated_folder(tmp_path: Path) -> None:
    dataset = create_symbol_dataset(
        "SPY",
        base_dir=tmp_path,
        fetcher=lambda symbol: monthly_frame(),
    )

    assert dataset.id == "spy"
    assert dataset.path == "data/generated/spy.csv"
    assert dataset.refresh is not None
    assert dataset.refresh.symbol == "SPY"

    created = pd.read_csv(tmp_path / dataset.path)
    assert list(created.columns) == ["symbol", "date", "open", "high", "low", "close", "volume"]
    assert created.iloc[0]["symbol"] == "SPY"
    assert created.iloc[0]["date"] == "2024-03-01"


def test_create_dataset_rejects_duplicate_id(tmp_path: Path) -> None:
    create_symbol_dataset(
        "SPY",
        base_dir=tmp_path,
        fetcher=lambda symbol: monthly_frame(),
    )

    with pytest.raises(CreationError, match="already exists"):
        create_symbol_dataset(
            "SPY",
            base_dir=tmp_path,
            fetcher=lambda symbol: monthly_frame(),
        )


def test_create_dataset_rejects_existing_target_file(tmp_path: Path) -> None:
    target = tmp_path / "data" / "generated" / "spy.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("existing", encoding="utf-8")

    with pytest.raises(CreationError, match="already exists"):
        create_symbol_dataset(
            "SPY",
            base_dir=tmp_path,
            fetcher=lambda symbol: monthly_frame(),
        )


def test_create_dataset_rejects_empty_data(tmp_path: Path) -> None:
    with pytest.raises(CreationError, match="no monthly data"):
        create_symbol_dataset(
            "SPY",
            base_dir=tmp_path,
            fetcher=lambda symbol: monthly_frame().iloc[0:0],
        )


def test_create_dataset_rejects_invalid_symbol_slug(tmp_path: Path) -> None:
    with pytest.raises(CreationError, match="letter or number"):
        create_symbol_dataset(
            "!!!",
            base_dir=tmp_path,
            fetcher=lambda symbol: monthly_frame(),
        )


def test_create_dataset_keeps_original_symbol_in_first_column(tmp_path: Path) -> None:
    dataset = create_symbol_dataset(
        "500.PA",
        base_dir=tmp_path,
        fetcher=lambda symbol: monthly_frame(),
    )

    created = pd.read_csv(tmp_path / dataset.path)
    assert created.iloc[0]["symbol"] == "500.PA"


def test_create_dataset_wraps_fetch_errors(tmp_path: Path) -> None:
    with pytest.raises(CreationError, match="network down"):
        create_symbol_dataset(
            "SPY",
            base_dir=tmp_path,
            fetcher=lambda symbol: (_ for _ in ()).throw(RuntimeError("network down")),
        )

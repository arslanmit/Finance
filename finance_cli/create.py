"""Create registered workbook datasets from Yahoo Finance symbols."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .errors import CreationError, RegistryError
from .models import DatasetConfig, RefreshMetadata
from .refresh import fetch_full_history_monthly_source
from .registry import get_config_path, save_registry

GENERATED_DATA_DIR = Path("data/generated")
GENERATED_SHEET_NAME = "Sheet1"
SYMBOL_COLUMN = "symbol"
EXPECTED_COLUMNS = ["date", "open", "high", "low", "close", "volume"]


def normalize_symbol_slug(symbol: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", symbol.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        raise CreationError("Symbol must contain at least one letter or number.")
    return slug


def build_generated_dataset_path(symbol_slug: str) -> Path:
    return GENERATED_DATA_DIR / f"{symbol_slug}.xlsx"


def create_and_register_symbol_dataset(
    datasets: list[DatasetConfig],
    symbol: str,
    *,
    config_path: Path | None = None,
    fetcher=None,
) -> DatasetConfig:
    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise CreationError("Symbol is required.")

    symbol_slug = normalize_symbol_slug(normalized_symbol)
    if any(dataset.id == symbol_slug for dataset in datasets):
        raise CreationError(f"Dataset '{symbol_slug}' already exists.")

    resolved_config = get_config_path(config_path)
    base_dir = resolved_config.parent
    relative_path = build_generated_dataset_path(symbol_slug)
    resolved_output_path = (base_dir / relative_path).resolve(strict=False)
    if resolved_output_path.exists():
        raise CreationError(f"Target dataset file already exists: {resolved_output_path}")

    effective_fetcher = fetch_full_history_monthly_source if fetcher is None else fetcher

    try:
        dataframe = effective_fetcher(normalized_symbol)
    except (CreationError, RegistryError):
        raise
    except Exception as exc:
        raise CreationError(str(exc)) from exc

    validate_created_dataframe(dataframe, normalized_symbol)

    try:
        write_created_workbook(dataframe, resolved_output_path, normalized_symbol)
    except Exception as exc:
        raise CreationError(f"Failed to write dataset workbook: {exc}") from exc

    dataset = DatasetConfig(
        id=symbol_slug,
        label=f"Yahoo symbol {normalized_symbol}",
        path=relative_path.as_posix(),
        sheet=GENERATED_SHEET_NAME,
        refresh=RefreshMetadata(provider="yahoo", symbol=normalized_symbol),
        base_dir=base_dir,
    )
    datasets.append(dataset)

    try:
        save_registry(datasets, config_path=resolved_config)
    except Exception as exc:
        datasets.pop()
        try:
            if resolved_output_path.exists():
                resolved_output_path.unlink()
        except OSError:
            pass
        raise CreationError(f"Failed to save dataset registry: {exc}") from exc

    return dataset


def validate_created_dataframe(dataframe: pd.DataFrame, symbol: str) -> None:
    if dataframe.empty:
        raise CreationError(f"Yahoo Finance returned no monthly data for {symbol}.")

    columns = list(dataframe.columns)
    if columns != EXPECTED_COLUMNS:
        raise CreationError(
            f"Unexpected monthly dataset columns for {symbol}: {', '.join(columns)}"
        )

    if dataframe["date"].duplicated().any():
        raise CreationError(f"Yahoo Finance returned duplicate monthly dates for {symbol}.")


def write_created_workbook(dataframe: pd.DataFrame, output_path: Path, symbol: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    descending = dataframe.sort_values("date", ascending=False).reset_index(drop=True)
    descending.insert(0, SYMBOL_COLUMN, symbol)
    with pd.ExcelWriter(output_path) as writer:
        descending.to_excel(writer, sheet_name=GENERATED_SHEET_NAME, index=False)

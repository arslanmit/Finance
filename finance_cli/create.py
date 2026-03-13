"""Create managed CSV datasets from Yahoo Finance symbols."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .catalog import GENERATED_DATA_DIR, discover_datasets, get_base_dir
from .errors import CatalogError, CreationError
from .managed_csv import write_managed_dataset_csv
from .models import DatasetConfig, RefreshMetadata
from .refresh_yahoo import fetch_full_history_monthly_source

SYMBOL_COLUMN = "symbol"
EXPECTED_COLUMNS = ["date", "open", "high", "low", "close", "volume"]


def normalize_symbol_slug(symbol: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", symbol.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        raise CreationError("Symbol must contain at least one letter or number.")
    return slug


def build_generated_dataset_path(symbol_slug: str) -> Path:
    return GENERATED_DATA_DIR / f"{symbol_slug}.csv"


def create_symbol_dataset(
    symbol: str,
    *,
    base_dir: Path | None = None,
    fetcher=None,
) -> DatasetConfig:
    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise CreationError("Symbol is required.")

    symbol_slug = normalize_symbol_slug(normalized_symbol)
    root = get_base_dir(base_dir)
    if any(dataset.id == symbol_slug for dataset in discover_datasets(root)):
        raise CreationError(f"Dataset '{symbol_slug}' already exists.")

    relative_path = build_generated_dataset_path(symbol_slug)
    resolved_output_path = (root / relative_path).resolve(strict=False)
    if resolved_output_path.exists():
        raise CreationError(f"Target dataset file already exists: {resolved_output_path}")

    effective_fetcher = fetch_full_history_monthly_source if fetcher is None else fetcher

    try:
        dataframe = effective_fetcher(normalized_symbol)
    except (CatalogError, CreationError):
        raise
    except Exception as exc:
        raise CreationError(str(exc)) from exc

    validate_created_dataframe(dataframe, normalized_symbol)

    try:
        write_created_csv(dataframe, resolved_output_path, normalized_symbol)
    except Exception as exc:
        raise CreationError(f"Failed to write dataset CSV: {exc}") from exc

    dataset = DatasetConfig(
        id=symbol_slug,
        label=symbol_slug,
        path=relative_path.as_posix(),
        refresh=RefreshMetadata(provider="yahoo", symbol=normalized_symbol),
        base_dir=root,
    )

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


def write_created_csv(dataframe: pd.DataFrame, output_path: Path, symbol: str) -> None:
    write_managed_dataset_csv(output_path, dataframe, symbol)

"""Thin refresh orchestrator for generated Yahoo monthly data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .errors import RefreshError
from .managed_csv import create_backup, load_existing_csv_data, write_managed_dataset_csv
from .refresh_validation import (
    validate_overlap,
    validate_refreshable_source,
    validate_source_contiguity,
)
from .refresh_yahoo import (
    DEFAULT_SYMBOL,
    build_chart_url,
    fetch_full_history_monthly_source,
    fetch_monthly_source,
)
from .models import RefreshSummary, ResolvedSource


def refresh_selected_source(
    source: ResolvedSource,
    backup_dir: Path | None = None,
) -> RefreshSummary:
    validate_refreshable_source(source)
    dataset = source.dataset
    if dataset is None or dataset.refresh is None:
        raise RefreshError("Live refresh is not configured for the selected source.")

    if backup_dir is None:
        return refresh_yahoo_monthly_csv(
            source.input_path,
            symbol=dataset.refresh.symbol,
            strict_validation=False,
        )

    return refresh_yahoo_monthly_csv(
        source.input_path,
        symbol=dataset.refresh.symbol,
        strict_validation=False,
        backup_dir=backup_dir,
    )


def write_source_csv(data_path: Path, source: pd.DataFrame, symbol: str) -> None:
    write_managed_dataset_csv(data_path, source, symbol)


def refresh_yahoo_monthly_csv(
    data_path: Path,
    symbol: str = DEFAULT_SYMBOL,
    strict_validation: bool = True,
    backup_dir: Path | None = None,
) -> RefreshSummary:
    if not data_path.exists():
        raise RefreshError(f"Refresh target does not exist: {data_path}")

    existing = load_existing_csv_data(data_path)
    source = fetch_monthly_source(symbol) if strict_validation else fetch_full_history_monthly_source(symbol)
    validate_source_contiguity(source)
    if strict_validation:
        validate_overlap(existing, source)
    backup_path = create_backup(data_path, backup_dir=backup_dir)
    write_managed_dataset_csv(data_path, source, symbol)

    return RefreshSummary(
        symbol=symbol,
        row_count=len(source),
        min_date=source["date"].min().date().isoformat(),
        max_date=source["date"].max().date().isoformat(),
        backup_path=str(backup_path),
    )


def refresh_generated_dataset(
    data_path: Path,
    symbol: str = DEFAULT_SYMBOL,
    backup_dir: Path | None = None,
) -> RefreshSummary:
    if backup_dir is None:
        return refresh_yahoo_monthly_csv(
            data_path,
            symbol=symbol,
            strict_validation=False,
        )

    return refresh_yahoo_monthly_csv(
        data_path,
        symbol=symbol,
        strict_validation=False,
        backup_dir=backup_dir,
    )


__all__ = [
    "DEFAULT_SYMBOL",
    "build_chart_url",
    "create_backup",
    "fetch_full_history_monthly_source",
    "fetch_monthly_source",
    "load_existing_csv_data",
    "refresh_generated_dataset",
    "refresh_selected_source",
    "refresh_yahoo_monthly_csv",
    "validate_overlap",
    "validate_refreshable_source",
    "validate_source_contiguity",
    "write_source_csv",
]

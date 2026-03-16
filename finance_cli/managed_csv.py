"""Managed dataset CSV persistence helpers."""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import pandas as pd

from .sources import ensure_symbol_column, normalize_columns


def load_existing_csv_data(data_path: Path) -> pd.DataFrame:
    dataframe = pd.read_csv(data_path)
    dataframe.columns = normalize_columns(list(dataframe.columns))
    if "symbol" in dataframe.columns:
        dataframe = dataframe.drop(columns=["symbol"])
    dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.normalize()
    return dataframe


def create_backup(data_path: Path, backup_dir: Path | None = None) -> Path:
    effective_backup_dir = Path("tmp/refresh_backups") if backup_dir is None else Path(backup_dir)
    effective_backup_dir.mkdir(parents=True, exist_ok=True)
    backup_name = f"{data_path.stem}.backup.{time.strftime('%Y%m%d-%H%M%S')}.csv"
    backup_path = effective_backup_dir / backup_name
    shutil.copy2(data_path, backup_path)
    return backup_path


def write_managed_dataset_csv(output_path: Path, dataframe: pd.DataFrame, symbol: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    descending = dataframe.sort_values("date", ascending=False).reset_index(drop=True)
    output = ensure_symbol_column(descending, symbol)
    output.to_csv(output_path, index=False, date_format="%Y-%m-%d")


__all__ = [
    "create_backup",
    "load_existing_csv_data",
    "write_managed_dataset_csv",
]

"""Generated-dataset discovery and filesystem-backed dataset operations."""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Iterable

import pandas as pd

from .errors import CatalogError
from .models import DatasetConfig, RefreshMetadata
from .sources import (
    SYMBOL_COLUMN,
    ensure_supported_file_suffix,
    ensure_symbol_column,
    load_dataframe,
    normalize_columns,
)

GENERATED_DATA_DIR = Path("data/generated")
SUPPORTED_REFRESH_PROVIDER = "yahoo"
DATASET_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def get_base_dir(base_dir: Path | None = None) -> Path:
    return Path("." if base_dir is None else base_dir).resolve(strict=False)


def validate_dataset_id(dataset_id: str) -> str:
    normalized = dataset_id.strip()
    if not normalized:
        raise CatalogError("Dataset id is required.")
    if not DATASET_ID_PATTERN.fullmatch(normalized):
        raise CatalogError(
            "Dataset ids must use only letters, numbers, dots, underscores, or hyphens."
        )
    return normalized


def discover_datasets(base_dir: Path | None = None) -> list[DatasetConfig]:
    root = get_base_dir(base_dir)
    dataset_dir = root / GENERATED_DATA_DIR
    if not dataset_dir.exists():
        return []

    datasets = [build_dataset_config(dataset_path, root) for dataset_path in sorted(dataset_dir.glob("*.csv"))]
    return sorted(datasets, key=lambda dataset: dataset.id.lower())


def build_dataset_config(dataset_path: Path, base_dir: Path | None = None) -> DatasetConfig:
    root = get_base_dir(base_dir)
    resolved_path = dataset_path.resolve(strict=False)
    ensure_supported_file_suffix(resolved_path.suffix.lower(), kind="dataset")

    try:
        relative_path = resolved_path.relative_to(root).as_posix()
    except ValueError as exc:
        raise CatalogError(f"Dataset path must live under the project root: {resolved_path}") from exc

    dataset_id = validate_dataset_id(resolved_path.stem)
    return DatasetConfig(
        id=dataset_id,
        label=dataset_id,
        path=relative_path,
        refresh=infer_refresh_metadata(resolved_path),
        base_dir=root,
    )


def infer_refresh_metadata(dataset_path: Path) -> RefreshMetadata | None:
    try:
        preview = pd.read_csv(dataset_path, nrows=1000)
    except Exception as exc:
        raise CatalogError(f"Failed to inspect dataset '{dataset_path.stem}': {exc}") from exc

    preview.columns = normalize_columns(list(preview.columns))
    if SYMBOL_COLUMN not in preview.columns:
        return None

    symbols = preview[SYMBOL_COLUMN].astype("string").str.strip()
    symbols = symbols.mask(symbols == "", pd.NA).dropna()
    if symbols.empty:
        return None

    return RefreshMetadata(provider=SUPPORTED_REFRESH_PROVIDER, symbol=str(symbols.iloc[0]))


def get_dataset(dataset_id: str, datasets: Iterable[DatasetConfig] | None = None) -> DatasetConfig:
    normalized_id = validate_dataset_id(dataset_id)
    available_datasets = list(discover_datasets() if datasets is None else datasets)
    for dataset in available_datasets:
        if dataset.id == normalized_id:
            return dataset

    available = ", ".join(dataset.id for dataset in available_datasets)
    raise CatalogError(f"Unknown dataset '{normalized_id}'. Available datasets: {available}")


def import_dataset(
    *,
    source_path: str | Path,
    refresh_symbol: str | None = None,
    base_dir: Path | None = None,
) -> DatasetConfig:
    root = get_base_dir(base_dir)
    normalized_symbol = normalize_refresh_symbol(refresh_symbol)

    source = Path(source_path).expanduser()
    if not source.exists():
        raise CatalogError(f"Dataset path was not found: {source}")
    ensure_supported_file_suffix(source.suffix.lower(), kind="dataset")

    dataset_id = derive_dataset_id_from_source(source)
    target_path = root / GENERATED_DATA_DIR / f"{dataset_id}.csv"
    if target_path.exists():
        raise CatalogError(
            f"Generated dataset '{dataset_id}' already exists. Rename '{source.name}' or remove the existing dataset first."
        )

    target_path.parent.mkdir(parents=True, exist_ok=True)
    if normalized_symbol is None:
        shutil.copy2(source, target_path)
    else:
        dataframe = load_dataframe(source)
        ensure_symbol_column(dataframe, normalized_symbol).to_csv(target_path, index=False)

    return build_dataset_config(target_path, root)


def derive_dataset_id_from_source(source_path: Path) -> str:
    source_stem = source_path.stem.strip()
    try:
        return validate_dataset_id(source_stem)
    except CatalogError as exc:
        raise CatalogError(
            f"Source filename '{source_path.name}' cannot become a dataset id. Rename the file to use only letters, numbers, dots, underscores, or hyphens."
        ) from exc


def remove_dataset(dataset_id: str, base_dir: Path | None = None) -> DatasetConfig:
    dataset = get_dataset(dataset_id, discover_datasets(base_dir))
    try:
        dataset.resolved_path.unlink()
    except FileNotFoundError as exc:
        raise CatalogError(f"Dataset file was not found: {dataset.resolved_path}") from exc
    except OSError as exc:
        raise CatalogError(f"Failed to remove dataset '{dataset.id}': {exc}") from exc
    return dataset


def normalize_refresh_symbol(symbol: str | None) -> str | None:
    if symbol is None:
        return None
    normalized = symbol.strip().upper()
    if not normalized:
        raise CatalogError("Refresh symbol must not be empty.")
    return normalized

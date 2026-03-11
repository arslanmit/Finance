"""Input source resolution and dataframe loading."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .errors import SourceError
from .models import DatasetConfig, ResolvedSource

SUPPORTED_FILE_SUFFIXES = {".csv"}
SYMBOL_COLUMN = "symbol"


def ensure_supported_file_suffix(suffix: str, kind: str) -> None:
    if suffix in SUPPORTED_FILE_SUFFIXES:
        return
    supported = ", ".join(sorted(SUPPORTED_FILE_SUFFIXES))
    raise SourceError(
        f"Unsupported {kind} format '{suffix}'. Supported formats: {supported}."
    )


def resolve_dataset_source(dataset: DatasetConfig) -> ResolvedSource:
    input_path = dataset.resolved_path
    if not input_path.exists():
        raise SourceError(f"Input file was not found: {input_path}")

    ensure_supported_file_suffix(input_path.suffix.lower(), kind="input")
    return ResolvedSource(input_path=input_path, dataset=dataset)


def resolve_custom_source(file_path: str | Path) -> ResolvedSource:
    input_path = Path(file_path).expanduser()
    if not input_path.exists():
        raise SourceError(f"Input file was not found: {input_path}")

    ensure_supported_file_suffix(input_path.suffix.lower(), kind="input")
    return ResolvedSource(input_path=input_path, dataset=None)


def normalize_columns(columns: list[object]) -> list[str]:
    normalized_columns: list[str] = []
    seen: dict[str, int] = {}

    for column in columns:
        normalized = str(column).strip().lower() or "unnamed"
        seen[normalized] = seen.get(normalized, 0) + 1
        if seen[normalized] > 1:
            normalized = f"{normalized}_{seen[normalized]}"
        normalized_columns.append(normalized)

    return normalized_columns


def ensure_symbol_column(dataframe: pd.DataFrame, symbol: str | None = None) -> pd.DataFrame:
    if SYMBOL_COLUMN not in dataframe.columns and symbol is None:
        return dataframe

    normalized = dataframe.copy()
    if SYMBOL_COLUMN not in normalized.columns:
        normalized.insert(0, SYMBOL_COLUMN, symbol)
        return normalized

    if symbol is not None:
        symbol_values = normalized[SYMBOL_COLUMN].astype("string").str.strip()
        normalized[SYMBOL_COLUMN] = symbol_values.mask(symbol_values == "", pd.NA).fillna(symbol)

    ordered_columns = [
        SYMBOL_COLUMN,
        *[column for column in normalized.columns if column != SYMBOL_COLUMN],
    ]
    return normalized[ordered_columns]


def load_dataframe(input_path: Path) -> pd.DataFrame:
    ensure_supported_file_suffix(input_path.suffix.lower(), kind="input")

    try:
        dataframe = pd.read_csv(input_path)
    except Exception as exc:
        raise SourceError(str(exc)) from exc

    dataframe.columns = normalize_columns(list(dataframe.columns))
    return dataframe

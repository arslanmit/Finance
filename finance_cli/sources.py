"""Input source resolution and dataframe loading."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd

from .errors import SourceError
from .models import DatasetConfig, ResolvedSource

SUPPORTED_FILE_SUFFIXES = {".csv", ".xlsx", ".xls"}
SYMBOL_COLUMN = "symbol"
SheetChooser = Callable[[list[str], Path], str]


def ensure_supported_file_suffix(suffix: str, kind: str) -> None:
    if suffix in SUPPORTED_FILE_SUFFIXES:
        return
    supported = ", ".join(sorted(SUPPORTED_FILE_SUFFIXES))
    raise SourceError(
        f"Unsupported {kind} format '{suffix}'. Supported formats: {supported}."
    )


def get_excel_sheet_names(input_path: Path) -> list[str]:
    try:
        workbook = pd.ExcelFile(input_path)
        return workbook.sheet_names
    except ValueError as exc:
        raise SourceError(str(exc)) from exc
    except ImportError as exc:
        raise SourceError(
            "Reading Excel files requires the appropriate engine dependencies to be installed."
        ) from exc


def resolve_sheet_name(
    input_path: Path,
    requested_sheet: str | None = None,
    interactive: bool = False,
    choose_sheet: SheetChooser | None = None,
) -> str | None:
    suffix = input_path.suffix.lower()
    ensure_supported_file_suffix(suffix, kind="input")

    if suffix == ".csv":
        if requested_sheet:
            raise SourceError("CSV files do not support sheet selection.")
        return None

    sheet_names = get_excel_sheet_names(input_path)
    if requested_sheet is not None:
        if requested_sheet not in sheet_names:
            raise SourceError(f"Sheet '{requested_sheet}' was not found in {input_path}.")
        return requested_sheet

    if len(sheet_names) == 1:
        return sheet_names[0]

    if interactive and choose_sheet is not None:
        return choose_sheet(sheet_names, input_path)

    raise SourceError(
        f"{input_path} contains multiple sheets ({', '.join(sheet_names)}). "
        "Use --sheet or run the guided wizard."
    )


def resolve_dataset_source(dataset: DatasetConfig) -> ResolvedSource:
    input_path = dataset.resolved_path
    if not input_path.exists():
        raise SourceError(f"Input file was not found: {input_path}")

    sheet_name = resolve_sheet_name(input_path, requested_sheet=dataset.sheet)
    return ResolvedSource(input_path=input_path, sheet_name=sheet_name, dataset=dataset)


def resolve_custom_source(
    file_path: str | Path,
    sheet_name: str | None = None,
    interactive: bool = False,
    choose_sheet: SheetChooser | None = None,
) -> ResolvedSource:
    input_path = Path(file_path).expanduser()
    if not input_path.exists():
        raise SourceError(f"Input file was not found: {input_path}")

    resolved_sheet = resolve_sheet_name(
        input_path,
        requested_sheet=sheet_name,
        interactive=interactive,
        choose_sheet=choose_sheet,
    )
    return ResolvedSource(input_path=input_path, sheet_name=resolved_sheet, dataset=None)


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

    ordered_columns = [SYMBOL_COLUMN, *[column for column in normalized.columns if column != SYMBOL_COLUMN]]
    return normalized[ordered_columns]


def load_dataframe(input_path: Path, sheet_name: str | None) -> pd.DataFrame:
    suffix = input_path.suffix.lower()
    ensure_supported_file_suffix(suffix, kind="input")

    try:
        if suffix == ".csv":
            dataframe = pd.read_csv(input_path)
        else:
            dataframe = pd.read_excel(input_path, sheet_name=sheet_name)
    except ValueError as exc:
        if "Worksheet named" in str(exc):
            raise SourceError(f"Sheet '{sheet_name}' was not found in {input_path}.") from exc
        raise SourceError(str(exc)) from exc
    except ImportError as exc:
        raise SourceError(
            "Reading Excel files requires the appropriate engine dependencies to be installed."
        ) from exc

    dataframe.columns = normalize_columns(list(dataframe.columns))
    return dataframe

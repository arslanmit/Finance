"""Data preparation, analysis, formatting, and output writing."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .errors import AnalysisError
from .sources import ensure_supported_file_suffix, ensure_symbol_column

PRIMARY_GAP_COLUMN = "moving_average_minus_open_over_open"
SECONDARY_GAP_COLUMN = "open_minus_moving_average_over_moving_average"
LEADING_OUTPUT_COLUMNS = [
    "symbol",
    PRIMARY_GAP_COLUMN,
    SECONDARY_GAP_COLUMN,
    "date",
    "open",
]
TRAILING_DERIVED_COLUMNS = [
    "moving_average_window_months",
    "Moving_Average",
    "condition",
]


def prepare_dataframe(dataframe: pd.DataFrame, months: int) -> pd.DataFrame:
    required_columns = {"date", "open"}
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise AnalysisError(f"Input data must contain the following columns: {missing}.")

    prepared = dataframe.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce")
    prepared["open"] = pd.to_numeric(
        prepared["open"].astype("string").str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )

    invalid_dates = int(prepared["date"].isna().sum())
    if invalid_dates:
        raise AnalysisError(f"Found {invalid_dates} invalid date value(s) in the input data.")

    invalid_open_values = int(prepared["open"].isna().sum())
    if invalid_open_values:
        raise AnalysisError(
            f"Found {invalid_open_values} invalid open value(s) in the input data."
        )

    prepared.sort_values(by="date", inplace=True)
    prepared.reset_index(drop=True, inplace=True)

    row_count = len(prepared)
    if row_count == 0:
        raise AnalysisError("The input data does not contain any rows.")
    if months < 1 or months > row_count:
        raise AnalysisError(
            f"Months must be between 1 and {row_count} for the selected input data."
        )

    return prepared


def analyze_dataframe(dataframe: pd.DataFrame, months: int) -> pd.DataFrame:
    analyzed = dataframe.copy()
    analyzed["moving_average_window_months"] = months
    analyzed["Moving_Average"] = analyzed["open"].rolling(window=months).mean()
    analyzed[PRIMARY_GAP_COLUMN] = (analyzed["Moving_Average"] - analyzed["open"]) / analyzed[
        "open"
    ]
    analyzed[SECONDARY_GAP_COLUMN] = (
        analyzed["open"] - analyzed["Moving_Average"]
    ) / analyzed["Moving_Average"]
    analyzed["condition"] = (
        (analyzed["Moving_Average"] > analyzed["open"]).fillna(False).astype(int)
    )
    return analyzed


def build_default_output_path(input_path: Path) -> Path:
    return Path("output") / f"{input_path.stem}_processed.csv"


def render_filtered_rows(dataframe: pd.DataFrame) -> str:
    displayed = dataframe[ordered_output_columns(dataframe)]
    if displayed.empty:
        return "No rows are available for display."
    return displayed.to_string(index=False)


def save_dataframe(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_supported_file_suffix(output_path.suffix.lower(), kind="output")
    output_dataframe = ensure_symbol_column(dataframe)
    output_dataframe = output_dataframe[ordered_output_columns(output_dataframe)]
    output_dataframe.to_csv(output_path, index=False, date_format="%Y-%m-%d")


def ordered_output_columns(dataframe: pd.DataFrame) -> list[str]:
    leading_columns = [column for column in LEADING_OUTPUT_COLUMNS if column in dataframe.columns]
    trailing_columns = [
        column
        for column in TRAILING_DERIVED_COLUMNS
        if column in dataframe.columns and column not in leading_columns
    ]
    middle_columns = [
        column
        for column in dataframe.columns
        if column not in leading_columns and column not in trailing_columns
    ]
    return [*leading_columns, *middle_columns, *trailing_columns]

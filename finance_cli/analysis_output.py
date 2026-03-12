"""Output formatting and persistence helpers for analysis results."""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from .sources import ensure_supported_file_suffix, ensure_symbol_column

PRIMARY_GAP_COLUMN = "moving_average_minus_open_over_open"
SECONDARY_GAP_COLUMN = "open_minus_moving_average_over_moving_average"
SCREENING_RULE_COLUMN = "screening_rule"
LEADING_OUTPUT_COLUMNS = [
    "symbol",
    PRIMARY_GAP_COLUMN,
    SECONDARY_GAP_COLUMN,
    "date",
    "open",
]
TRAILING_DERIVED_COLUMNS = ["moving_average_window_months", "condition", SCREENING_RULE_COLUMN]
INDICATOR_COLUMN_PATTERN = re.compile(r"^[A-Z]+_\d+_months$")


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


def get_trailing_derived_columns(dataframe: pd.DataFrame) -> list[str]:
    trailing_columns: list[str] = []
    indicator_columns = [
        column
        for column in dataframe.columns
        if INDICATOR_COLUMN_PATTERN.fullmatch(column)
    ]
    if "Moving_Average" in dataframe.columns:
        indicator_columns.append("Moving_Average")

    for column in [*indicator_columns, *TRAILING_DERIVED_COLUMNS]:
        if column in dataframe.columns and column not in trailing_columns:
            trailing_columns.append(column)

    return trailing_columns


def ordered_output_columns(dataframe: pd.DataFrame) -> list[str]:
    leading_columns = [column for column in LEADING_OUTPUT_COLUMNS if column in dataframe.columns]
    trailing_columns = [
        column
        for column in get_trailing_derived_columns(dataframe)
        if column not in leading_columns
    ]
    middle_columns = [
        column
        for column in dataframe.columns
        if column not in leading_columns and column not in trailing_columns
    ]
    return [*leading_columns, *middle_columns, *trailing_columns]

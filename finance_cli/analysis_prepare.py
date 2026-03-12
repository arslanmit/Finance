"""Dataframe preparation helpers for analysis workflows."""

from __future__ import annotations

import pandas as pd

from .errors import AnalysisError


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

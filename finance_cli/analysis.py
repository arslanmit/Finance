"""Data preparation, analysis, formatting, and output writing."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .errors import AnalysisError
from .sources import ensure_supported_file_suffix, ensure_symbol_column

DISPLAY_COLUMNS = ["date", "open", "Moving_Average", "condition"]


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
    analyzed["Moving_Average"] = analyzed["open"].rolling(window=months).mean()
    analyzed["condition"] = (
        (analyzed["Moving_Average"] > analyzed["open"]).fillna(False).astype(int)
    )
    return analyzed


def build_default_output_path(input_path: Path) -> Path:
    return Path("output") / f"{input_path.stem}_processed{input_path.suffix.lower()}"


def render_filtered_rows(dataframe: pd.DataFrame) -> str:
    filtered = dataframe[dataframe["condition"] == 1][DISPLAY_COLUMNS]
    if filtered.empty:
        return "No rows matched the Moving_Average > open condition."
    return filtered.to_string(index=False)


def save_dataframe(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()
    ensure_supported_file_suffix(suffix, kind="output")
    output_dataframe = ensure_symbol_column(dataframe)

    if suffix == ".csv":
        output_dataframe.to_csv(output_path, index=False)
        return
    if suffix == ".xlsx":
        output_dataframe.to_excel(output_path, index=False)
        return

    write_xls(output_dataframe, output_path)


def write_xls(dataframe: pd.DataFrame, output_path: Path) -> None:
    try:
        import xlwt
    except ImportError as exc:
        raise AnalysisError(
            "Writing .xls output requires the 'xlwt' package to be installed."
        ) from exc

    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet("Sheet1")
    date_style = xlwt.easyxf(num_format_str="YYYY-MM-DD")
    datetime_style = xlwt.easyxf(num_format_str="YYYY-MM-DD HH:MM:SS")

    for column_index, column_name in enumerate(dataframe.columns):
        worksheet.write(0, column_index, str(column_name))

    for row_index, row in enumerate(dataframe.itertuples(index=False), start=1):
        for column_index, value in enumerate(row):
            if pd.isna(value):
                worksheet.write(row_index, column_index, "")
                continue
            if isinstance(value, pd.Timestamp):
                style = date_style if value.time() == pd.Timestamp(0).time() else datetime_style
                worksheet.write(row_index, column_index, value.to_pydatetime(), style)
                continue
            worksheet.write(row_index, column_index, value)

    workbook.save(str(output_path))

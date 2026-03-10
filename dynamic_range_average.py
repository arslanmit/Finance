#!/usr/bin/env python3
"""CLI for calculating moving averages over finance datasets."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

DEFAULT_INPUT = Path("data/sp500_raw_data.xlsx")
DEFAULT_SHEET = "Sheet1"
DISPLAY_COLUMNS = ["date", "open", "Moving_Average", "condition"]
SUPPORTED_INPUT_SUFFIXES = {".csv", ".xlsx", ".xls"}


class DataProcessingError(Exception):
    """Raised when the input data or runtime options are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Calculate a moving average for the input dataset, print rows where "
            "the moving average is above the open price, and save the processed data."
        )
    )
    parser.add_argument(
        "--months",
        type=int,
        help="Number of rows to use for the moving average window.",
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help="Path to the input file. Supported formats: .csv, .xlsx, .xls.",
    )
    parser.add_argument(
        "--sheet",
        default=DEFAULT_SHEET,
        help="Excel sheet name to load when the input file is .xlsx or .xls.",
    )
    parser.add_argument(
        "--output",
        help=(
            "Optional output path. If omitted, writes to "
            "output/<input_stem>_processed.<input_extension>."
        ),
    )
    return parser


def resolve_months(months: int | None) -> int:
    if months is not None:
        return months

    if not sys.stdin.isatty():
        raise DataProcessingError(
            "Missing required --months value. Provide --months when running "
            "the script non-interactively."
        )

    response = input("Enter the number of months for the moving average: ").strip()

    try:
        return int(response)
    except ValueError as exc:
        raise DataProcessingError("Months must be an integer.") from exc


def normalize_columns(columns: list[object]) -> list[str]:
    normalized_columns: list[str] = []
    seen: dict[str, int] = {}

    for column in columns:
        normalized = str(column).strip().lower()
        if not normalized:
            normalized = "unnamed"

        seen[normalized] = seen.get(normalized, 0) + 1
        if seen[normalized] > 1:
            normalized = f"{normalized}_{seen[normalized]}"

        normalized_columns.append(normalized)

    return normalized_columns


def load_dataframe(input_path: Path, sheet_name: str) -> pd.DataFrame:
    if not input_path.exists():
        raise DataProcessingError(f"Input file was not found: {input_path}")

    suffix = input_path.suffix.lower()
    if suffix not in SUPPORTED_INPUT_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_INPUT_SUFFIXES))
        raise DataProcessingError(
            f"Unsupported input format '{input_path.suffix}'. Supported formats: {supported}."
        )

    try:
        if suffix == ".csv":
            dataframe = pd.read_csv(input_path)
        else:
            dataframe = pd.read_excel(input_path, sheet_name=sheet_name)
    except ValueError as exc:
        if "Worksheet named" in str(exc):
            raise DataProcessingError(
                f"Sheet '{sheet_name}' was not found in {input_path}."
            ) from exc
        raise DataProcessingError(str(exc)) from exc
    except ImportError as exc:
        raise DataProcessingError(
            "Reading Excel files requires the appropriate engine dependencies to be installed."
        ) from exc

    dataframe.columns = normalize_columns(list(dataframe.columns))
    return dataframe


def prepare_dataframe(dataframe: pd.DataFrame, months: int) -> pd.DataFrame:
    required_columns = {"date", "open"}
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise DataProcessingError(
            f"Input data must contain the following columns: {missing}."
        )

    prepared = dataframe.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce")
    prepared["open"] = pd.to_numeric(
        prepared["open"].astype("string").str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )

    invalid_dates = int(prepared["date"].isna().sum())
    if invalid_dates:
        raise DataProcessingError(
            f"Found {invalid_dates} invalid date value(s) in the input data."
        )

    invalid_open_values = int(prepared["open"].isna().sum())
    if invalid_open_values:
        raise DataProcessingError(
            f"Found {invalid_open_values} invalid open value(s) in the input data."
        )

    prepared.sort_values(by="date", inplace=True)
    prepared.reset_index(drop=True, inplace=True)

    row_count = len(prepared)
    if row_count == 0:
        raise DataProcessingError("The input data does not contain any rows.")

    if months < 1 or months > row_count:
        raise DataProcessingError(
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


def write_xls(dataframe: pd.DataFrame, output_path: Path) -> None:
    try:
        import xlwt
    except ImportError as exc:
        raise DataProcessingError(
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


def save_dataframe(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    suffix = output_path.suffix.lower()
    if suffix == ".csv":
        dataframe.to_csv(output_path, index=False)
        return

    if suffix == ".xlsx":
        dataframe.to_excel(output_path, index=False)
        return

    if suffix == ".xls":
        write_xls(dataframe, output_path)
        return

    supported = ", ".join(sorted(SUPPORTED_INPUT_SUFFIXES))
    raise DataProcessingError(
        f"Unsupported output format '{output_path.suffix}'. Supported formats: {supported}."
    )


def print_filtered_rows(dataframe: pd.DataFrame) -> None:
    filtered = dataframe[dataframe["condition"] == 1][DISPLAY_COLUMNS]
    if filtered.empty:
        print("No rows matched the Moving_Average > open condition.")
        return

    print(filtered.to_string(index=False))


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        months = resolve_months(args.months)
        input_path = Path(args.input)
        output_path = Path(args.output) if args.output else build_default_output_path(input_path)

        raw_dataframe = load_dataframe(input_path, args.sheet)
        prepared_dataframe = prepare_dataframe(raw_dataframe, months)
        analyzed_dataframe = analyze_dataframe(prepared_dataframe, months)

        print_filtered_rows(analyzed_dataframe)
        save_dataframe(analyzed_dataframe, output_path)
        print(f"\nProcessed data saved to: {output_path}")
        return 0
    except DataProcessingError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

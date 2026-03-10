#!/usr/bin/env python3
"""User-friendly CLI for finance dataset analysis."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from refresh_sp500_data import RefreshError, refresh_yahoo_monthly_workbook

DATASETS_CONFIG_PATH = Path("datasets.json")
DISPLAY_COLUMNS = ["date", "open", "Moving_Average", "condition"]
SUPPORTED_INPUT_SUFFIXES = {".csv", ".xlsx", ".xls"}


class DataProcessingError(Exception):
    """Raised when the input data or runtime options are invalid."""


@dataclass(frozen=True)
class RefreshMetadata:
    """Refresh metadata for a configured dataset."""

    provider: str
    symbol: str


@dataclass(frozen=True)
class DatasetConfig:
    """Configured dataset entry from datasets.json."""

    id: str
    label: str
    path: str
    sheet: str | None
    refresh: RefreshMetadata | None
    base_dir: Path

    @property
    def resolved_path(self) -> Path:
        return (self.base_dir / self.path).resolve(strict=False)

    @property
    def file_name(self) -> str:
        return Path(self.path).name

    @property
    def supports_refresh(self) -> bool:
        return self.refresh is not None


@dataclass(frozen=True)
class SelectedSource:
    """Resolved input source for a run."""

    input_path: Path
    sheet_name: str | None
    dataset: DatasetConfig | None
    prompted: bool


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run moving average analysis on a configured dataset alias or on your own "
            "CSV/Excel file."
        )
    )
    parser.add_argument(
        "--dataset",
        help="Dataset alias from datasets.json. Use --list-datasets to see available options.",
    )
    parser.add_argument(
        "--file",
        help="Path to your own CSV or Excel file.",
    )
    parser.add_argument(
        "--months",
        type=int,
        help=(
            "Moving average window size in rows/months. Examples: 3, 6, 12, 24. "
            "If omitted in an interactive terminal, the script prompts for it."
        ),
    )
    parser.add_argument(
        "--output",
        help=(
            "Optional output path. If omitted, writes to "
            "output/<input_stem>_processed.<input_extension>."
        ),
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh the selected configured dataset from live data before analysis.",
    )
    parser.add_argument(
        "--list-datasets",
        action="store_true",
        help="List configured dataset aliases and exit.",
    )
    return parser


def load_dataset_configs(config_path: Path = DATASETS_CONFIG_PATH) -> list[DatasetConfig]:
    if not config_path.exists():
        raise DataProcessingError(f"Dataset config file was not found: {config_path}")

    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DataProcessingError(f"Dataset config file is not valid JSON: {exc}") from exc

    entries = payload.get("datasets") if isinstance(payload, dict) else payload
    if not isinstance(entries, list):
        raise DataProcessingError("datasets.json must contain a top-level 'datasets' list.")

    datasets: list[DatasetConfig] = []
    seen_ids: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise DataProcessingError("Each dataset entry in datasets.json must be an object.")

        dataset_id = str(entry.get("id", "")).strip()
        label = str(entry.get("label", "")).strip()
        path = str(entry.get("path", "")).strip()
        if not dataset_id or not label or not path:
            raise DataProcessingError(
                "Each dataset entry must define non-empty 'id', 'label', and 'path' values."
            )
        if dataset_id in seen_ids:
            raise DataProcessingError(f"Duplicate dataset id found in datasets.json: {dataset_id}")
        seen_ids.add(dataset_id)

        sheet_value = entry.get("sheet")
        sheet = None if sheet_value in (None, "") else str(sheet_value).strip()

        refresh_entry = entry.get("refresh")
        refresh = None
        if refresh_entry is not None:
            if not isinstance(refresh_entry, dict):
                raise DataProcessingError(f"Dataset '{dataset_id}' has an invalid refresh object.")
            provider = str(refresh_entry.get("provider", "")).strip()
            symbol = str(refresh_entry.get("symbol", "")).strip()
            if not provider or not symbol:
                raise DataProcessingError(
                    f"Dataset '{dataset_id}' refresh metadata must define provider and symbol."
                )
            refresh = RefreshMetadata(provider=provider, symbol=symbol)

        datasets.append(
            DatasetConfig(
                id=dataset_id,
                label=label,
                path=path,
                sheet=sheet,
                refresh=refresh,
                base_dir=config_path.parent,
            )
        )

    if not datasets:
        raise DataProcessingError("datasets.json does not define any datasets.")

    return datasets


def print_dataset_list(datasets: list[DatasetConfig]) -> None:
    print("Available datasets:\n")
    for dataset in datasets:
        refresh_text = "yes" if dataset.supports_refresh else "no"
        print(
            f"- {dataset.id}: {dataset.label} | file: {dataset.file_name} | "
            f"path: {dataset.path} | refresh: {refresh_text}"
        )


def resolve_dataset(datasets: list[DatasetConfig], dataset_id: str) -> DatasetConfig:
    for dataset in datasets:
        if dataset.id == dataset_id:
            return dataset
    available = ", ".join(dataset.id for dataset in datasets)
    raise DataProcessingError(f"Unknown dataset '{dataset_id}'. Available datasets: {available}")


def prompt_user_to_choose_source(datasets: list[DatasetConfig]) -> tuple[DatasetConfig | None, Path | None]:
    print("Choose a dataset:\n")
    for index, dataset in enumerate(datasets, start=1):
        refresh_tag = " [refresh available]" if dataset.supports_refresh else ""
        print(f"{index}. {dataset.id} - {dataset.label} ({dataset.file_name}){refresh_tag}")
    custom_index = len(datasets) + 1
    print(f"{custom_index}. custom - Use your own file path")

    while True:
        response = input("\nEnter a dataset number or alias: ").strip()
        if not response:
            continue

        if response.isdigit():
            selection = int(response)
            if 1 <= selection <= len(datasets):
                return datasets[selection - 1], None
            if selection == custom_index:
                custom_path = input("Enter the file path: ").strip()
                if custom_path:
                    return None, Path(custom_path).expanduser()
                print("Please enter a file path.")
                continue

        for dataset in datasets:
            if response == dataset.id:
                return dataset, None
        if response.lower() == "custom":
            custom_path = input("Enter the file path: ").strip()
            if custom_path:
                return None, Path(custom_path).expanduser()
            print("Please enter a file path.")
            continue

        print("Invalid selection. Choose a listed number, dataset alias, or 'custom'.")


def prompt_yes_no(prompt: str, default: bool = False) -> bool:
    suffix = " [Y/n]: " if default else " [y/N]: "
    while True:
        response = input(f"{prompt}{suffix}").strip().lower()
        if not response:
            return default
        if response in {"y", "yes"}:
            return True
        if response in {"n", "no"}:
            return False
        print("Please answer with 'y' or 'n'.")


def resolve_months(months: int | None) -> tuple[int, bool]:
    if months is not None:
        return months, False

    if not sys.stdin.isatty():
        raise DataProcessingError(
            "Missing required --months value. Provide --months when running "
            "the script non-interactively."
        )

    while True:
        response = input("Enter the moving average window (examples: 3, 6, 12): ").strip()
        try:
            return int(response), True
        except ValueError:
            print("Please enter a whole number, for example 3, 6, or 12.")


def get_excel_sheet_names(input_path: Path) -> list[str]:
    try:
        workbook = pd.ExcelFile(input_path)
        return workbook.sheet_names
    except ValueError as exc:
        raise DataProcessingError(str(exc)) from exc
    except ImportError as exc:
        raise DataProcessingError(
            "Reading Excel files requires the appropriate engine dependencies to be installed."
        ) from exc


def prompt_for_sheet(sheet_names: list[str], input_path: Path) -> str:
    print(f"\nMultiple sheets were found in {input_path.name}:")
    for index, sheet_name in enumerate(sheet_names, start=1):
        print(f"{index}. {sheet_name}")

    while True:
        response = input("Choose a sheet number: ").strip()
        if response.isdigit():
            selection = int(response)
            if 1 <= selection <= len(sheet_names):
                return sheet_names[selection - 1]
        print("Invalid sheet selection.")


def resolve_sheet_name(input_path: Path, configured_sheet: str | None, interactive: bool) -> str | None:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return None

    if suffix not in SUPPORTED_INPUT_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_INPUT_SUFFIXES))
        raise DataProcessingError(
            f"Unsupported input format '{input_path.suffix}'. Supported formats: {supported}."
        )

    sheet_names = get_excel_sheet_names(input_path)
    if configured_sheet is not None:
        if configured_sheet not in sheet_names:
            raise DataProcessingError(
                f"Sheet '{configured_sheet}' was not found in {input_path}."
            )
        return configured_sheet

    if len(sheet_names) == 1:
        return sheet_names[0]

    if not interactive:
        raise DataProcessingError(
            f"{input_path} contains multiple sheets ({', '.join(sheet_names)}). "
            "Run interactively to choose one."
        )

    return prompt_for_sheet(sheet_names, input_path)


def resolve_source(
    args: argparse.Namespace,
    datasets: list[DatasetConfig],
    interactive: bool,
) -> SelectedSource:
    if args.dataset and args.file:
        raise DataProcessingError("Use either --dataset or --file, not both.")

    prompted = False
    dataset: DatasetConfig | None = None
    input_path: Path | None = None

    if args.dataset:
        dataset = resolve_dataset(datasets, args.dataset)
        input_path = dataset.resolved_path
    elif args.file:
        input_path = Path(args.file).expanduser()
    elif interactive:
        dataset, custom_path = prompt_user_to_choose_source(datasets)
        prompted = True
        input_path = dataset.resolved_path if dataset else custom_path
    else:
        raise DataProcessingError("Provide --dataset <alias> or --file <path>.")

    if input_path is None:
        raise DataProcessingError("Could not resolve an input file.")
    if not input_path.exists():
        raise DataProcessingError(f"Input file was not found: {input_path}")

    configured_sheet = dataset.sheet if dataset else None
    sheet_name = resolve_sheet_name(input_path, configured_sheet, interactive)
    return SelectedSource(
        input_path=input_path,
        sheet_name=sheet_name,
        dataset=dataset,
        prompted=prompted,
    )


def resolve_refresh_requested(
    requested_via_flag: bool,
    selected_source: SelectedSource,
    months_prompted: bool,
    interactive: bool,
) -> bool:
    if requested_via_flag:
        return True

    dataset = selected_source.dataset
    if not interactive or dataset is None or not dataset.supports_refresh:
        return False

    if not (selected_source.prompted or months_prompted):
        return False

    return prompt_yes_no("Refresh live data first?", default=False)


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


def load_dataframe(input_path: Path, sheet_name: str | None) -> pd.DataFrame:
    suffix = input_path.suffix.lower()

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


def run_refresh(selected_source: SelectedSource) -> None:
    dataset = selected_source.dataset
    if dataset is None:
        raise DataProcessingError("Live refresh is only available for configured datasets, not custom files.")
    if dataset.refresh is None:
        raise DataProcessingError(f"Dataset '{dataset.id}' does not support live refresh.")
    if dataset.refresh.provider != "yahoo":
        raise DataProcessingError(
            f"Dataset '{dataset.id}' uses an unsupported refresh provider: {dataset.refresh.provider}"
        )
    if selected_source.input_path.suffix.lower() != ".xlsx":
        raise DataProcessingError("Live refresh currently supports only .xlsx workbook datasets.")
    if selected_source.sheet_name is None:
        raise DataProcessingError("Live refresh requires a configured Excel sheet.")

    try:
        summary = refresh_yahoo_monthly_workbook(
            selected_source.input_path,
            symbol=dataset.refresh.symbol,
            sheet_name=selected_source.sheet_name,
        )
    except RefreshError as exc:
        raise DataProcessingError(str(exc)) from exc

    print(
        "Refresh summary: "
        f"symbol={summary.symbol}, "
        f"range={summary.min_date}..{summary.max_date}, "
        f"rows={summary.row_count}, "
        f"backup={summary.backup_path}"
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    interactive = sys.stdin.isatty()

    try:
        datasets = load_dataset_configs()

        if args.list_datasets:
            print_dataset_list(datasets)
            return 0

        selected_source = resolve_source(args, datasets, interactive)
        months, months_prompted = resolve_months(args.months)
        refresh_requested = resolve_refresh_requested(
            requested_via_flag=args.refresh,
            selected_source=selected_source,
            months_prompted=months_prompted,
            interactive=interactive,
        )

        if args.refresh and selected_source.dataset is None:
            raise DataProcessingError("Live refresh is only available for configured datasets, not custom files.")
        if args.refresh and selected_source.dataset and not selected_source.dataset.supports_refresh:
            raise DataProcessingError(
                f"Dataset '{selected_source.dataset.id}' does not support live refresh."
            )
        if refresh_requested:
            run_refresh(selected_source)

        output_path = (
            Path(args.output).expanduser()
            if args.output
            else build_default_output_path(selected_source.input_path)
        )

        raw_dataframe = load_dataframe(selected_source.input_path, selected_source.sheet_name)
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

"""Console presentation helpers for CLI output."""

from __future__ import annotations

from .models import DatasetConfig, RefreshSummary


def sort_datasets_for_display(datasets: list[DatasetConfig]) -> list[DatasetConfig]:
    return sorted(
        datasets,
        key=lambda dataset: (
            dataset.file_name.lower(),
            dataset.id.lower(),
        ),
    )


def print_refresh_summary(summary: RefreshSummary) -> None:
    print(
        "Refresh summary: "
        f"symbol={summary.symbol}, "
        f"range={summary.min_date}..{summary.max_date}, "
        f"rows={summary.row_count}, "
        f"backup={summary.backup_path}"
    )


def print_dataset_refresh_summary(dataset: DatasetConfig, summary: RefreshSummary) -> None:
    print(
        f"Refreshed dataset '{dataset.id}': "
        f"symbol={summary.symbol}, "
        f"range={summary.min_date}..{summary.max_date}, "
        f"rows={summary.row_count}, "
        f"backup={summary.backup_path}"
    )


def print_dataset_list(datasets: list[DatasetConfig]) -> None:
    print("Available datasets:\n")
    for dataset in sort_datasets_for_display(datasets):
        refresh_text = "yes" if dataset.supports_refresh else "no"
        print(f"- {dataset.id} | file: {dataset.file_name} | refresh: {refresh_text}")

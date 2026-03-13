"""Validation helpers for refreshable managed datasets."""

from __future__ import annotations

import pandas as pd

from .errors import RefreshError
from .models import ResolvedSource

SUPPORTED_REFRESH_PROVIDER = "yahoo"
OVERLAP_MONTHS = ("2024-08-01", "2024-10-01")
NOVEMBER_OPEN_MONTH = pd.Timestamp("2024-11-01")


def validate_refreshable_source(source: ResolvedSource) -> None:
    dataset = source.dataset
    if dataset is None:
        raise RefreshError("Live refresh is only available for generated datasets, not custom files.")
    if dataset.refresh is None:
        raise RefreshError(f"Dataset '{dataset.id}' does not support live refresh.")
    if dataset.refresh.provider != SUPPORTED_REFRESH_PROVIDER:
        raise RefreshError(
            f"Dataset '{dataset.id}' uses an unsupported refresh provider: {dataset.refresh.provider}"
        )
    if source.input_path.suffix.lower() != ".csv":
        raise RefreshError("Live refresh currently supports only .csv datasets.")


def validate_source_contiguity(source: pd.DataFrame) -> None:
    if source["date"].duplicated().any():
        raise RefreshError("Live monthly source contains duplicate dates.")

    expected = pd.date_range(source["date"].min(), source["date"].max(), freq="MS")
    missing = expected.difference(source["date"])
    if not missing.empty:
        preview = ", ".join(day.date().isoformat() for day in missing[:5])
        raise RefreshError(f"Live monthly source is missing dates: {preview}.")


def validate_overlap(existing: pd.DataFrame, source: pd.DataFrame) -> None:
    merged = existing.merge(source, on="date", suffixes=("_existing", "_source"))
    if merged.empty:
        raise RefreshError("Could not compare the current dataset to the live source.")

    completed = merged[
        merged["date"].between(pd.Timestamp(OVERLAP_MONTHS[0]), pd.Timestamp(OVERLAP_MONTHS[1]))
    ].sort_values("date")
    if len(completed) != 3:
        raise RefreshError(
            "Expected three completed overlap months (2024-08 through 2024-10) for validation."
        )

    for column in ("open", "high", "low", "close"):
        delta = (completed[f"{column}_existing"] - completed[f"{column}_source"]).abs().max()
        if float(delta) > 0.02:
            raise RefreshError(
                f"Live source validation failed for {column}; max delta was {float(delta):.4f}."
            )

    november = merged[merged["date"] == NOVEMBER_OPEN_MONTH]
    if november.empty:
        raise RefreshError("Expected an overlap row for 2024-11-01.")

    open_delta = abs(
        float(november.iloc[0]["open_existing"]) - float(november.iloc[0]["open_source"])
    )
    if open_delta > 0.02:
        raise RefreshError(
            f"Live source validation failed for 2024-11 open; delta was {open_delta:.4f}."
        )


__all__ = [
    "SUPPORTED_REFRESH_PROVIDER",
    "validate_overlap",
    "validate_refreshable_source",
    "validate_source_contiguity",
]

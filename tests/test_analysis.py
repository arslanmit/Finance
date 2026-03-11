from pathlib import Path

import pandas as pd
import pytest

from finance_cli.analysis import (
    analyze_dataframe,
    build_default_output_path,
    prepare_dataframe,
    render_filtered_rows,
    save_dataframe,
)
from finance_cli.errors import AnalysisError, SourceError


def test_prepare_dataframe_requires_date_and_open() -> None:
    dataframe = pd.DataFrame({"close": [1, 2, 3]})

    with pytest.raises(AnalysisError, match="date, open"):
        prepare_dataframe(dataframe, months=2)


def test_prepare_dataframe_rejects_invalid_values() -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "bad-date"],
            "open": ["10", "oops"],
        }
    )

    with pytest.raises(AnalysisError, match="invalid date"):
        prepare_dataframe(dataframe, months=1)


def test_prepare_dataframe_rejects_invalid_months() -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01"],
            "open": [10, 11],
        }
    )

    with pytest.raises(AnalysisError, match="between 1 and 2"):
        prepare_dataframe(dataframe, months=3)


def test_analyze_dataframe_and_render_output() -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10, 12, 11],
        }
    )

    prepared = prepare_dataframe(dataframe, months=2)
    analyzed = analyze_dataframe(prepared, months=2)
    rendered = render_filtered_rows(analyzed)

    assert "Moving_Average" in analyzed.columns
    assert analyzed["moving_average_window_months"].tolist() == [2, 2, 2]
    assert analyzed["condition"].tolist() == [0, 0, 1]
    assert "2024-03-01" in rendered


def test_render_filtered_rows_shows_symbol_when_present() -> None:
    dataframe = pd.DataFrame(
        {
            "symbol": ["NVDA", "NVDA", "NVDA"],
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10, 12, 11],
        }
    )

    prepared = prepare_dataframe(dataframe, months=2)
    analyzed = analyze_dataframe(prepared, months=2)
    rendered = render_filtered_rows(analyzed)

    assert "symbol" in rendered
    assert "NVDA" in rendered


def test_build_default_output_path_always_returns_csv() -> None:
    path = build_default_output_path(Path("data/example.xlsx"))

    assert path == Path("output/example_processed.csv")


def test_save_dataframe_rejects_non_csv_output(tmp_path: Path) -> None:
    dataframe = pd.DataFrame({"date": ["2024-01-01"], "open": [10]})

    with pytest.raises(SourceError, match=r"Supported formats: \.csv"):
        save_dataframe(dataframe, tmp_path / "output.xlsx")


def test_save_dataframe_persists_window_column(tmp_path: Path) -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10, 12, 11],
        }
    )

    prepared = prepare_dataframe(dataframe, months=2)
    analyzed = analyze_dataframe(prepared, months=2)
    output_path = tmp_path / "output.csv"

    save_dataframe(analyzed, output_path)

    written = pd.read_csv(output_path)
    assert "moving_average_window_months" in written.columns
    assert written["moving_average_window_months"].tolist() == [2, 2, 2]

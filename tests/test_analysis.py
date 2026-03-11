from pathlib import Path

import pandas as pd
import pytest

from finance_cli.analysis import (
    PRIMARY_GAP_COLUMN,
    SECONDARY_GAP_COLUMN,
    analyze_dataframe,
    build_default_output_path,
    ordered_output_columns,
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
    assert PRIMARY_GAP_COLUMN in analyzed.columns
    assert SECONDARY_GAP_COLUMN in analyzed.columns
    assert analyzed["moving_average_window_months"].tolist() == [2, 2, 2]
    assert analyzed["condition"].tolist() == [0, 0, 1]
    assert pd.isna(analyzed.loc[0, PRIMARY_GAP_COLUMN])
    assert pd.isna(analyzed.loc[0, SECONDARY_GAP_COLUMN])
    assert analyzed.loc[1, PRIMARY_GAP_COLUMN] == pytest.approx((11 - 12) / 12)
    assert analyzed.loc[1, SECONDARY_GAP_COLUMN] == pytest.approx((12 - 11) / 11)
    assert analyzed.loc[2, PRIMARY_GAP_COLUMN] == pytest.approx((11.5 - 11) / 11)
    assert analyzed.loc[2, SECONDARY_GAP_COLUMN] == pytest.approx((11 - 11.5) / 11.5)
    assert analyzed.loc[2, PRIMARY_GAP_COLUMN] > 0
    assert "2024-01-01" in rendered
    assert "2024-03-01" in rendered
    assert PRIMARY_GAP_COLUMN in rendered
    assert SECONDARY_GAP_COLUMN in rendered
    assert "moving_average_window_months" in rendered
    assert "0" in rendered
    assert "1" in rendered


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
    header_tokens = rendered.splitlines()[0].split()

    assert "symbol" in rendered
    assert "NVDA" in rendered
    assert header_tokens[:3] == ["symbol", PRIMARY_GAP_COLUMN, SECONDARY_GAP_COLUMN]
    assert "moving_average_window_months" in rendered


def test_render_filtered_rows_includes_all_input_columns_in_order() -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10, 12, 11],
            "high": [11, 13, 12],
            "low": [9, 11, 10],
            "close": [10.5, 12.5, 11.5],
            "volume": [100, 101, 102],
        }
    )

    prepared = prepare_dataframe(dataframe, months=2)
    analyzed = analyze_dataframe(prepared, months=2)
    rendered = render_filtered_rows(analyzed)
    header_tokens = rendered.splitlines()[0].split()

    assert header_tokens == ordered_output_columns(analyzed)


def test_render_filtered_rows_shows_all_rows_when_condition_is_zero() -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01"],
            "open": [10, 11],
        }
    )

    prepared = prepare_dataframe(dataframe, months=2)
    analyzed = analyze_dataframe(prepared, months=2)
    rendered = render_filtered_rows(analyzed)

    assert "2024-01-01" in rendered
    assert "2024-02-01" in rendered
    assert "0" in rendered


def test_build_default_output_path_always_returns_csv() -> None:
    path = build_default_output_path(Path("data/example.xlsx"))

    assert path == Path("output/example_processed.csv")


def test_save_dataframe_rejects_non_csv_output(tmp_path: Path) -> None:
    dataframe = pd.DataFrame({"date": ["2024-01-01"], "open": [10]})

    with pytest.raises(SourceError, match=r"Supported formats: \.csv"):
        save_dataframe(dataframe, tmp_path / "output.xlsx")


def test_save_dataframe_places_gap_columns_first_without_symbol(tmp_path: Path) -> None:
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
    assert list(written.columns[:2]) == [PRIMARY_GAP_COLUMN, SECONDARY_GAP_COLUMN]
    assert "moving_average_window_months" in written.columns
    assert written["moving_average_window_months"].tolist() == [2, 2, 2]
    assert written[PRIMARY_GAP_COLUMN].dtype.kind == "f"
    assert written[SECONDARY_GAP_COLUMN].dtype.kind == "f"


def test_save_dataframe_places_symbol_before_gap_columns(tmp_path: Path) -> None:
    dataframe = pd.DataFrame(
        {
            "symbol": ["NVDA", "NVDA", "NVDA"],
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10, 12, 11],
        }
    )

    prepared = prepare_dataframe(dataframe, months=2)
    analyzed = analyze_dataframe(prepared, months=2)
    output_path = tmp_path / "output.csv"

    save_dataframe(analyzed, output_path)

    written = pd.read_csv(output_path)
    assert list(written.columns[:3]) == ["symbol", PRIMARY_GAP_COLUMN, SECONDARY_GAP_COLUMN]


def test_save_dataframe_preserves_extra_input_columns_and_matches_terminal_order(
    tmp_path: Path,
) -> None:
    dataframe = pd.DataFrame(
        {
            "symbol": ["NVDA", "NVDA", "NVDA"],
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10, 12, 11],
            "high": [11, 13, 12],
            "low": [9, 11, 10],
            "close": [10.5, 12.5, 11.5],
            "volume": [100, 101, 102],
        }
    )

    prepared = prepare_dataframe(dataframe, months=2)
    analyzed = analyze_dataframe(prepared, months=2)
    rendered = render_filtered_rows(analyzed)
    header_tokens = rendered.splitlines()[0].split()
    output_path = tmp_path / "output.csv"

    save_dataframe(analyzed, output_path)

    written = pd.read_csv(output_path)
    assert list(written.columns) == ordered_output_columns(analyzed)
    assert header_tokens == list(written.columns)

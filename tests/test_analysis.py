from pathlib import Path

import pandas as pd
import pytest

from finance_cli.analysis import (
    PRIMARY_GAP_COLUMN,
    SCREENING_RULE_COLUMN,
    SECONDARY_GAP_COLUMN,
    analyze_dataframe,
    analyze_dataframe_with_config,
    build_default_output_path,
    format_indicator_column_name,
    get_trailing_derived_columns,
    ordered_output_columns,
    prepare_dataframe,
    render_filtered_rows,
    save_dataframe,
)
from finance_cli.errors import AnalysisError, SourceError
from finance_cli.models import AnalysisConfig


RULE_TOKEN_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"


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


def test_rule_helpers_are_available_from_rule_module() -> None:
    from finance_cli.analysis_rules import evaluate_rule as evaluate_rule_module
    from finance_cli.analysis_rules import format_rule as format_rule_module
    from finance_cli.analysis_rules import parse_rule as parse_rule_module

    dataframe = pd.DataFrame(
        {
            "EMA_2_months": [12.0, 9.0],
            "open": [10.0, 10.0],
        }
    )
    parsed_rule = parse_rule_module("indicator > open")

    assert format_rule_module(parsed_rule) == "indicator > open"
    assert list(evaluate_rule_module(dataframe, parsed_rule, "EMA_2_months")) == [1, 0]


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
    assert SCREENING_RULE_COLUMN not in analyzed.columns
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
    assert "Moving_Average" in rendered
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
    assert "Moving_Average" in rendered


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
    assert "Moving_Average" in written.columns
    assert SCREENING_RULE_COLUMN not in written.columns
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


def test_analyze_dataframe_with_custom_indicator_and_rule() -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10.0, 12.0, 11.0],
            "close": [10.5, 11.0, 12.0],
        }
    )

    prepared = prepare_dataframe(dataframe, months=2)
    config = AnalysisConfig(months=2, indicator_type="ema", rule="indicator <= close")
    analyzed = analyze_dataframe_with_config(prepared, config)
    indicator_column = format_indicator_column_name("ema", 2)

    assert indicator_column in analyzed.columns
    assert analyzed.loc[0, indicator_column] == pytest.approx(10.0)
    assert analyzed.loc[1, indicator_column] == pytest.approx(11.333333, abs=1e-5)
    assert analyzed.loc[2, indicator_column] == pytest.approx(11.111111, abs=1e-5)
    assert analyzed["condition"].tolist() == [1, 0, 1]
    assert analyzed[SCREENING_RULE_COLUMN].tolist() == ["indicator <= close"] * 3


def test_analyze_dataframe_preserves_legacy_default_output_shape() -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10.0, 12.0, 11.0],
        }
    )

    prepared = prepare_dataframe(dataframe, months=2)
    analyzed = analyze_dataframe(prepared, months=2)

    assert "Moving_Average" in analyzed.columns
    assert format_indicator_column_name("sma", 2) not in analyzed.columns
    assert SCREENING_RULE_COLUMN not in analyzed.columns
    assert analyzed.loc[2, "Moving_Average"] == pytest.approx(11.5)


def test_analyze_dataframe_with_unknown_indicator_raises_error() -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01"],
            "open": [10.0, 12.0],
        }
    )

    prepared = prepare_dataframe(dataframe, months=1)

    with pytest.raises(AnalysisError, match="Unknown indicator type 'unknown'"):
        analyze_dataframe_with_config(
            prepared,
            AnalysisConfig(months=1, indicator_type="unknown"),
        )


def test_get_trailing_derived_columns_supports_dynamic_and_legacy_columns() -> None:
    dataframe = pd.DataFrame(
        {
            "symbol": ["NVDA"],
            "EMA_6_months": [123.0],
            "Moving_Average": [120.0],
            "moving_average_window_months": [6],
            "condition": [1],
            "screening_rule": ["indicator > open"],
        }
    )

    trailing = get_trailing_derived_columns(dataframe)

    assert trailing == [
        "EMA_6_months",
        "Moving_Average",
        "moving_average_window_months",
        "condition",
        "screening_rule",
    ]


def test_ordered_output_columns_places_dynamic_indicator_before_base_trailing_columns() -> None:
    dataframe = pd.DataFrame(
        {
            "symbol": ["NVDA"],
            PRIMARY_GAP_COLUMN: [0.1],
            SECONDARY_GAP_COLUMN: [-0.09],
            "date": ["2024-01-01"],
            "open": [100.0],
            "close": [101.0],
            "EMA_6_months": [102.0],
            "moving_average_window_months": [6],
            "condition": [1],
            "screening_rule": ["indicator > close"],
        }
    )

    assert ordered_output_columns(dataframe) == [
        "symbol",
        PRIMARY_GAP_COLUMN,
        SECONDARY_GAP_COLUMN,
        "date",
        "open",
        "close",
        "EMA_6_months",
        "moving_average_window_months",
        "condition",
        "screening_rule",
    ]


def test_parse_rule_with_valid_format() -> None:
    from finance_cli.analysis import parse_rule

    rule = parse_rule("indicator > open")

    assert rule.left_operand == "indicator"
    assert rule.operator == ">"
    assert rule.right_operand == "open"


def test_parse_rule_with_all_valid_operators() -> None:
    from finance_cli.analysis import parse_rule

    operators = [">", "<", ">=", "<="]
    for op in operators:
        rule = parse_rule(f"indicator {op} close")
        assert rule.operator == op
        assert rule.left_operand == "indicator"
        assert rule.right_operand == "close"


def test_parse_rule_strips_whitespace() -> None:
    from finance_cli.analysis import parse_rule

    rule = parse_rule("  indicator   >   open  ")

    assert rule.left_operand == "indicator"
    assert rule.operator == ">"
    assert rule.right_operand == "open"


def test_parse_rule_rejects_invalid_format_too_few_parts() -> None:
    from finance_cli.analysis import parse_rule

    with pytest.raises(AnalysisError, match="Invalid rule format"):
        parse_rule("indicator>open")


def test_parse_rule_rejects_invalid_format_too_many_parts() -> None:
    from finance_cli.analysis import parse_rule

    with pytest.raises(AnalysisError, match="Invalid rule format"):
        parse_rule("indicator > open > close")


def test_parse_rule_error_includes_example() -> None:
    from finance_cli.analysis import parse_rule

    with pytest.raises(AnalysisError, match="indicator > open"):
        parse_rule("invalid")


def test_parse_rule_rejects_invalid_operator() -> None:
    from finance_cli.analysis import parse_rule

    with pytest.raises(AnalysisError, match="Invalid operator"):
        parse_rule("indicator == open")


def test_parse_rule_invalid_operator_lists_valid_operators() -> None:
    from finance_cli.analysis import parse_rule

    with pytest.raises(AnalysisError, match="Valid operators"):
        parse_rule("indicator != open")


# Property-Based Tests

from hypothesis import given, strategies as st, settings


@given(
    indicator_type=st.sampled_from(["sma", "ema", "wma"]),
    months=st.integers(min_value=1, max_value=120),
)
@settings(max_examples=50)
def test_property_indicator_column_naming_pattern(
    indicator_type: str, months: int
) -> None:
    """Property 5: Indicator Column Naming Pattern."""
    column_name = format_indicator_column_name(indicator_type, months)

    assert column_name == f"{indicator_type.upper()}_{months}_months"
    assert column_name.endswith("_months")
    assert str(months) in column_name


@given(
    invalid_indicator=st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=12).filter(
        lambda value: value.lower() not in {"sma", "ema", "wma"}
    ),
    months=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=40)
def test_property_invalid_indicator_error_messages(
    invalid_indicator: str, months: int
) -> None:
    """Property 1: Invalid Indicator Error Messages."""
    dataframe = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=months, freq="MS"),
            "open": [100.0] * months,
        }
    )

    with pytest.raises(AnalysisError) as exc_info:
        analyze_dataframe_with_config(
            prepare_dataframe(dataframe, months=months),
            AnalysisConfig(months=months, indicator_type=invalid_indicator),
        )

    error_message = str(exc_info.value)
    assert "Unknown indicator type" in error_message
    assert invalid_indicator.strip().lower() in error_message
    for valid_indicator in ["sma", "ema", "wma"]:
        assert valid_indicator in error_message


@given(
    indicator_type=st.sampled_from(["sma", "ema", "wma"]),
    values=st.lists(
        st.floats(min_value=10.0, max_value=500.0, allow_nan=False, allow_infinity=False),
        min_size=3,
        max_size=12,
    ),
    months=st.integers(min_value=1, max_value=6),
)
@settings(max_examples=40)
def test_property_gap_ratio_calculation_correctness(
    indicator_type: str, values: list[float], months: int
) -> None:
    """Property 3: Gap Ratio Calculation Correctness."""
    months = min(months, len(values))
    dataframe = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=len(values), freq="MS"),
            "open": values,
        }
    )

    analyzed = analyze_dataframe_with_config(
        prepare_dataframe(dataframe, months=months),
        AnalysisConfig(months=months, indicator_type=indicator_type),
    )
    indicator_column = format_indicator_column_name(indicator_type, months)
    expected_primary = (analyzed[indicator_column] - analyzed["open"]) / analyzed["open"]
    expected_secondary = (analyzed["open"] - analyzed[indicator_column]) / analyzed[indicator_column]

    pd.testing.assert_series_equal(
        analyzed[PRIMARY_GAP_COLUMN],
        expected_primary,
        check_names=False,
    )
    pd.testing.assert_series_equal(
        analyzed[SECONDARY_GAP_COLUMN],
        expected_secondary,
        check_names=False,
    )


@given(
    indicator_type=st.sampled_from(["sma", "ema", "wma"]),
    operator=st.sampled_from([">", "<", ">=", "<="]),
    right_operand=st.sampled_from(["open", "high", "low", "close"]),
)
@settings(max_examples=40)
def test_property_screening_rule_metadata_preservation(
    indicator_type: str, operator: str, right_operand: str
) -> None:
    """Property 11: Screening Rule Metadata Preservation."""
    rule = f"indicator {operator} {right_operand}"
    dataframe = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=6, freq="MS"),
            "open": [100.0, 102.0, 101.0, 104.0, 103.0, 105.0],
            "high": [101.0, 103.0, 102.0, 105.0, 104.0, 106.0],
            "low": [99.0, 100.0, 100.0, 103.0, 102.0, 104.0],
            "close": [100.5, 101.5, 101.0, 104.5, 103.5, 105.5],
        }
    )

    analyzed = analyze_dataframe_with_config(
        prepare_dataframe(dataframe, months=3),
        AnalysisConfig(months=3, indicator_type=indicator_type, rule=rule),
    )

    assert analyzed[SCREENING_RULE_COLUMN].tolist() == [rule] * len(analyzed)


@given(
    indicator_type=st.sampled_from(["SMA", "EMA", "WMA"]),
    include_symbol=st.booleans(),
)
@settings(max_examples=30)
def test_property_dynamic_column_ordering(
    indicator_type: str, include_symbol: bool
) -> None:
    """Property 12: Dynamic Column Ordering."""
    indicator_column = f"{indicator_type}_6_months"
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "open": [100.0],
            "close": [101.0],
            "volume": [10],
            indicator_column: [102.0],
            "moving_average_window_months": [6],
            "condition": [1],
            "screening_rule": ["indicator > close"],
        }
    )
    if include_symbol:
        dataframe.insert(0, "symbol", "NVDA")
    dataframe.insert(1 if include_symbol else 0, PRIMARY_GAP_COLUMN, 0.02)
    dataframe.insert(2 if include_symbol else 1, SECONDARY_GAP_COLUMN, -0.0196)

    ordered = ordered_output_columns(dataframe)
    indicator_index = ordered.index(indicator_column)

    assert ordered[indicator_index + 1 :] == [
        "moving_average_window_months",
        "condition",
        "screening_rule",
    ]
    assert ordered[: 3 if include_symbol else 2] == (
        ["symbol", PRIMARY_GAP_COLUMN, SECONDARY_GAP_COLUMN]
        if include_symbol
        else [PRIMARY_GAP_COLUMN, SECONDARY_GAP_COLUMN]
    )


@given(
    left_operand=st.text(
        min_size=1, 
        max_size=20,
        alphabet=st.characters(blacklist_categories=("Zs", "Zl", "Zp", "Cc"))
    ).filter(lambda x: len(x.split()) == 1),
    operator=st.sampled_from([">", "<", ">=", "<="]),
    right_operand=st.text(
        min_size=1, 
        max_size=20,
        alphabet=st.characters(blacklist_categories=("Zs", "Zl", "Zp", "Cc"))
    ).filter(lambda x: len(x.split()) == 1)
)
@settings(max_examples=100)
def test_property_rule_parsing_round_trip(
    left_operand: str, operator: str, right_operand: str
) -> None:
    """Property 8: Rule Parsing Round-Trip.
    
    **Validates: Requirements 4.3**
    
    For any valid rule string in the format "left_operand operator right_operand",
    parsing the rule should extract the three components correctly, and the parsed
    components should represent the original rule semantics.
    
    This property ensures that:
    1. Valid rule strings are parsed without errors
    2. Parsed components match the original input exactly
    3. The parsing is reversible (round-trip property)
    4. Whitespace handling is consistent
    """
    from finance_cli.analysis import parse_rule
    
    # Construct a valid rule string
    rule_string = f"{left_operand} {operator} {right_operand}"
    
    # Parse the rule
    parsed = parse_rule(rule_string)
    
    # Property 1: Parsed components match original input
    assert parsed.left_operand == left_operand, \
        f"Left operand mismatch: expected '{left_operand}', got '{parsed.left_operand}'"
    assert parsed.operator == operator, \
        f"Operator mismatch: expected '{operator}', got '{parsed.operator}'"
    assert parsed.right_operand == right_operand, \
        f"Right operand mismatch: expected '{right_operand}', got '{parsed.right_operand}'"
    
    # Property 2: Round-trip reconstruction produces semantically equivalent rule
    reconstructed = f"{parsed.left_operand} {parsed.operator} {parsed.right_operand}"
    assert reconstructed == rule_string, \
        f"Round-trip failed: original '{rule_string}', reconstructed '{reconstructed}'"
    
    # Property 3: Operator is valid (validated by ParsedRule.__post_init__)
    assert parsed.operator in parsed.VALID_OPERATORS, \
        f"Operator '{parsed.operator}' not in valid operators"
    
    # Property 4: Whitespace variations produce same result
    rule_with_extra_spaces = f"  {left_operand}   {operator}   {right_operand}  "
    parsed_with_spaces = parse_rule(rule_with_extra_spaces)
    assert parsed_with_spaces.left_operand == left_operand
    assert parsed_with_spaces.operator == operator
    assert parsed_with_spaces.right_operand == right_operand


# Tests for evaluate_rule function

def test_evaluate_rule_with_greater_than_operator() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule

    dataframe = pd.DataFrame({
        "indicator_col": [10, 20, 15],
        "open": [8, 25, 15]
    })
    rule = parse_rule("indicator > open")
    
    result = evaluate_rule(dataframe, rule, "indicator_col")
    
    assert result.tolist() == [1, 0, 0]


def test_evaluate_rule_with_less_than_operator() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule

    dataframe = pd.DataFrame({
        "indicator_col": [10, 20, 15],
        "open": [8, 25, 15]
    })
    rule = parse_rule("indicator < open")
    
    result = evaluate_rule(dataframe, rule, "indicator_col")
    
    assert result.tolist() == [0, 1, 0]


def test_evaluate_rule_with_greater_than_or_equal_operator() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule

    dataframe = pd.DataFrame({
        "indicator_col": [10, 20, 15],
        "open": [8, 25, 15]
    })
    rule = parse_rule("indicator >= open")
    
    result = evaluate_rule(dataframe, rule, "indicator_col")
    
    assert result.tolist() == [1, 0, 1]


def test_evaluate_rule_with_less_than_or_equal_operator() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule

    dataframe = pd.DataFrame({
        "indicator_col": [10, 20, 15],
        "open": [8, 25, 15]
    })
    rule = parse_rule("indicator <= open")
    
    result = evaluate_rule(dataframe, rule, "indicator_col")
    
    assert result.tolist() == [0, 1, 1]


def test_evaluate_rule_handles_nan_in_left_operand() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule
    import numpy as np

    dataframe = pd.DataFrame({
        "indicator_col": [10, np.nan, 15],
        "open": [8, 25, 15]
    })
    rule = parse_rule("indicator > open")
    
    result = evaluate_rule(dataframe, rule, "indicator_col")
    
    assert result.tolist() == [1, 0, 0]


def test_evaluate_rule_handles_nan_in_right_operand() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule
    import numpy as np

    dataframe = pd.DataFrame({
        "indicator_col": [10, 20, 15],
        "open": [8, np.nan, 15]
    })
    rule = parse_rule("indicator > open")
    
    result = evaluate_rule(dataframe, rule, "indicator_col")
    
    assert result.tolist() == [1, 0, 0]


def test_evaluate_rule_handles_nan_in_both_operands() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule
    import numpy as np

    dataframe = pd.DataFrame({
        "indicator_col": [10, np.nan, np.nan],
        "open": [8, 25, np.nan]
    })
    rule = parse_rule("indicator > open")
    
    result = evaluate_rule(dataframe, rule, "indicator_col")
    
    assert result.tolist() == [1, 0, 0]


def test_evaluate_rule_with_ohlcv_columns() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule

    dataframe = pd.DataFrame({
        "indicator_col": [10, 20, 15],
        "open": [8, 25, 15],
        "high": [12, 30, 18],
        "low": [7, 20, 14],
        "close": [9, 22, 16]
    })
    
    # Test with close column
    rule = parse_rule("indicator > close")
    result = evaluate_rule(dataframe, rule, "indicator_col")
    assert result.tolist() == [1, 0, 0]
    
    # Test with high column
    rule = parse_rule("indicator < high")
    result = evaluate_rule(dataframe, rule, "indicator_col")
    assert result.tolist() == [1, 1, 1]


def test_evaluate_rule_resolves_indicator_keyword() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule

    dataframe = pd.DataFrame({
        "SMA_6_months": [10, 20, 15],
        "open": [8, 25, 15]
    })
    rule = parse_rule("indicator > open")
    
    result = evaluate_rule(dataframe, rule, "SMA_6_months")
    
    assert result.tolist() == [1, 0, 0]


def test_evaluate_rule_rejects_nonexistent_left_operand() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule

    dataframe = pd.DataFrame({
        "indicator_col": [10, 20, 15],
        "open": [8, 25, 15]
    })
    rule = parse_rule("volume > open")
    
    with pytest.raises(AnalysisError, match="non-existent column 'volume'"):
        evaluate_rule(dataframe, rule, "indicator_col")


def test_evaluate_rule_rejects_nonexistent_right_operand() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule

    dataframe = pd.DataFrame({
        "indicator_col": [10, 20, 15],
        "open": [8, 25, 15]
    })
    rule = parse_rule("indicator > close")
    
    with pytest.raises(AnalysisError, match="non-existent column 'close'"):
        evaluate_rule(dataframe, rule, "indicator_col")


def test_evaluate_rule_error_lists_available_columns() -> None:
    from finance_cli.analysis import evaluate_rule, parse_rule

    dataframe = pd.DataFrame({
        "indicator_col": [10, 20, 15],
        "open": [8, 25, 15]
    })
    rule = parse_rule("indicator > close")
    
    with pytest.raises(AnalysisError, match="Available columns"):
        evaluate_rule(dataframe, rule, "indicator_col")


@given(
    operator=st.sampled_from([">", "<", ">=", "<="]),
    ohlcv_column=st.sampled_from(["open", "high", "low", "close"]),
    data_length=st.integers(min_value=3, max_value=20),
    seed=st.integers(min_value=0, max_value=10000)
)
@settings(max_examples=100)
def test_property_rule_evaluation_across_ohlcv_columns(
    operator: str, ohlcv_column: str, data_length: int, seed: int
) -> None:
    """Property 6: Rule Evaluation Across OHLCV Columns.
    
    **Validates: Requirements 3.3**
    
    For any OHLCV column (open, high, low, close) that exists in the dataframe,
    the Rule Engine should successfully evaluate screening rules that reference
    that column.
    
    This property ensures that:
    1. Rules can reference any OHLCV column as an operand
    2. Rule evaluation produces binary output (0 or 1)
    3. The evaluation logic is consistent across all OHLCV columns
    4. No errors occur when evaluating valid column references
    """
    from finance_cli.analysis import evaluate_rule, parse_rule
    import numpy as np
    
    # Set seed for reproducibility within this test
    np.random.seed(seed)
    
    # Generate realistic OHLCV data
    # For realistic data: low <= open <= high, low <= close <= high
    base_price = np.random.uniform(50, 150, data_length)
    volatility = np.random.uniform(0.01, 0.1, data_length)
    
    low = base_price * (1 - volatility)
    high = base_price * (1 + volatility)
    open_price = np.random.uniform(low, high)
    close_price = np.random.uniform(low, high)
    
    # Create indicator column with values that will produce varied comparison results
    indicator_values = base_price * np.random.uniform(0.95, 1.05, data_length)
    
    # Build dataframe with all OHLCV columns
    dataframe = pd.DataFrame({
        "open": open_price,
        "high": high,
        "low": low,
        "close": close_price,
        "indicator_col": indicator_values
    })
    
    # Construct rule comparing indicator to the selected OHLCV column
    rule_string = f"indicator {operator} {ohlcv_column}"
    parsed_rule = parse_rule(rule_string)
    
    # Property 1: Rule evaluation should succeed without errors
    result = evaluate_rule(dataframe, parsed_rule, "indicator_col")
    
    # Property 2: Result should be a pandas Series
    assert isinstance(result, pd.Series), \
        f"Expected pandas Series, got {type(result)}"
    
    # Property 3: Result should have same length as input dataframe
    assert len(result) == len(dataframe), \
        f"Result length {len(result)} != dataframe length {len(dataframe)}"
    
    # Property 4: Result should contain only binary values (0 or 1)
    unique_values = set(result.unique())
    assert unique_values.issubset({0, 1}), \
        f"Result contains non-binary values: {unique_values}"
    
    # Property 5: Result values should match manual comparison
    left_values = dataframe["indicator_col"]
    right_values = dataframe[ohlcv_column]
    
    if operator == ">":
        expected = (left_values > right_values).fillna(False).astype(int)
    elif operator == "<":
        expected = (left_values < right_values).fillna(False).astype(int)
    elif operator == ">=":
        expected = (left_values >= right_values).fillna(False).astype(int)
    elif operator == "<=":
        expected = (left_values <= right_values).fillna(False).astype(int)
    
    assert result.equals(expected), \
        f"Rule evaluation mismatch for '{rule_string}'"
    
    # Property 6: Test with "indicator" keyword on both sides
    # Test rule like "indicator > close" where left operand uses "indicator" keyword
    rule_with_keyword = parse_rule(f"indicator {operator} {ohlcv_column}")
    result_with_keyword = evaluate_rule(dataframe, rule_with_keyword, "indicator_col")
    assert result_with_keyword.equals(expected), \
        f"Rule evaluation with 'indicator' keyword failed"
    
    # Property 7: Test reverse comparison (OHLCV column on left side)
    reverse_operator_map = {">": "<", "<": ">", ">=": "<=", "<=": ">="}
    reverse_operator = reverse_operator_map[operator]
    reverse_rule = parse_rule(f"{ohlcv_column} {reverse_operator} indicator")
    reverse_result = evaluate_rule(dataframe, reverse_rule, "indicator_col")
    
    # Reverse comparison should produce same result
    assert reverse_result.equals(expected), \
        f"Reverse rule evaluation mismatch"


@given(
    operator=st.sampled_from([">", "<", ">=", "<="]),
    data_length=st.integers(min_value=5, max_value=20),
    nan_positions=st.lists(
        st.sampled_from(["left", "right", "both"]),
        min_size=1,
        max_size=5
    ),
    seed=st.integers(min_value=0, max_value=10000)
)
@settings(max_examples=100, deadline=None)
def test_property_nan_handling_in_rule_evaluation(
    operator: str, data_length: int, nan_positions: list, seed: int
) -> None:
    """Property 7: NaN Handling in Rule Evaluation.
    
    **Validates: Requirements 3.6**
    
    For any screening rule, when either operand contains NaN values, the condition
    flag for those rows should be 0.
    
    This property ensures that:
    1. NaN values in the left operand result in condition flag = 0
    2. NaN values in the right operand result in condition flag = 0
    3. NaN values in both operands result in condition flag = 0
    4. The behavior is consistent across all comparison operators
    5. Non-NaN rows are evaluated correctly regardless of NaN presence elsewhere
    """
    from finance_cli.analysis import evaluate_rule, parse_rule
    import numpy as np
    
    # Set seed for reproducibility
    np.random.seed(seed)
    
    # Generate base data without NaN values
    left_values = np.random.uniform(50, 150, data_length)
    right_values = np.random.uniform(50, 150, data_length)
    
    # Determine which rows should have NaN values
    # We'll inject NaN values at specific positions based on nan_positions
    nan_indices = set()
    for i, position_type in enumerate(nan_positions):
        if i < data_length:
            nan_indices.add(i)
    
    # Create copies for manipulation
    left_with_nan = left_values.copy()
    right_with_nan = right_values.copy()
    
    # Track which rows should have condition = 0 due to NaN
    expected_nan_rows = set()
    
    # Inject NaN values based on nan_positions
    for idx in nan_indices:
        position_type = nan_positions[idx % len(nan_positions)]
        if position_type == "left":
            left_with_nan[idx] = np.nan
            expected_nan_rows.add(idx)
        elif position_type == "right":
            right_with_nan[idx] = np.nan
            expected_nan_rows.add(idx)
        elif position_type == "both":
            left_with_nan[idx] = np.nan
            right_with_nan[idx] = np.nan
            expected_nan_rows.add(idx)
    
    # Create dataframe
    dataframe = pd.DataFrame({
        "indicator_col": left_with_nan,
        "open": right_with_nan
    })
    
    # Parse and evaluate rule
    rule_string = f"indicator {operator} open"
    parsed_rule = parse_rule(rule_string)
    result = evaluate_rule(dataframe, parsed_rule, "indicator_col")
    
    # Property 1: Result should be a pandas Series with correct length
    assert isinstance(result, pd.Series), \
        f"Expected pandas Series, got {type(result)}"
    assert len(result) == data_length, \
        f"Result length {len(result)} != expected {data_length}"
    
    # Property 2: Result should contain only binary values (0 or 1)
    unique_values = set(result.unique())
    assert unique_values.issubset({0, 1}), \
        f"Result contains non-binary values: {unique_values}"
    
    # Property 3: All rows with NaN in either operand should have condition = 0
    for idx in expected_nan_rows:
        assert result.iloc[idx] == 0, \
            f"Row {idx} has NaN but condition != 0 (got {result.iloc[idx]})"
    
    # Property 4: Non-NaN rows should be evaluated correctly
    for idx in range(data_length):
        if idx not in expected_nan_rows:
            left_val = left_with_nan[idx]
            right_val = right_with_nan[idx]
            
            # Calculate expected result for this row
            if operator == ">":
                expected_val = 1 if left_val > right_val else 0
            elif operator == "<":
                expected_val = 1 if left_val < right_val else 0
            elif operator == ">=":
                expected_val = 1 if left_val >= right_val else 0
            elif operator == "<=":
                expected_val = 1 if left_val <= right_val else 0
            
            assert result.iloc[idx] == expected_val, \
                f"Row {idx} (non-NaN) has incorrect condition: " \
                f"expected {expected_val}, got {result.iloc[idx]} " \
                f"(left={left_val}, right={right_val}, operator={operator})"
    
    # Property 5: Verify consistency with pandas comparison behavior
    # Pandas comparisons with NaN return False, which should become 0
    left_series = pd.Series(left_with_nan)
    right_series = pd.Series(right_with_nan)
    
    if operator == ">":
        expected_series = (left_series > right_series).fillna(False).astype(int)
    elif operator == "<":
        expected_series = (left_series < right_series).fillna(False).astype(int)
    elif operator == ">=":
        expected_series = (left_series >= right_series).fillna(False).astype(int)
    elif operator == "<=":
        expected_series = (left_series <= right_series).fillna(False).astype(int)
    
    assert result.equals(expected_series), \
        f"Result does not match expected pandas comparison behavior"


@given(
    # Generate invalid rule formats
    invalid_format=st.one_of(
        # Too few parts (0, 1, or 2 parts)
        st.just(""),
        st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=20),
        st.tuples(
            st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=10),
            st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=10)
        ).map(lambda t: f"{t[0]} {t[1]}"),
        # Too many parts (4+ parts)
        st.tuples(
            st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=10),
            st.sampled_from([">", "<", ">=", "<="]),
            st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=10),
            st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=10)
        ).map(lambda t: f"{t[0]} {t[1]} {t[2]} {t[3]}")
    )
)
@settings(max_examples=100)
def test_property_rule_validation_invalid_format_errors(
    invalid_format: str
) -> None:
    """Property 9a: Rule Validation Error Messages - Invalid Format.
    
    **Validates: Requirements 4.4, 4.5**
    
    For any invalid rule specification with wrong format (not exactly 3 parts),
    the system should raise a descriptive AnalysisError explaining the validation
    failure with format examples.
    
    This property ensures that:
    1. Invalid formats always raise AnalysisError
    2. Error messages contain "Invalid rule format"
    3. Error messages include the expected format description
    4. Error messages include an example of valid format
    """
    from finance_cli.analysis import parse_rule
    
    # Property 1: Invalid format should always raise AnalysisError
    with pytest.raises(AnalysisError) as exc_info:
        parse_rule(invalid_format)
    
    error_msg = str(exc_info.value)
    
    # Property 2: Error message should mention "Invalid rule format"
    assert "Invalid rule format" in error_msg, \
        f"Error message missing 'Invalid rule format': {error_msg}"
    
    # Property 3: Error message should include expected format description
    assert "left_operand operator right_operand" in error_msg, \
        f"Error message missing format description: {error_msg}"
    
    # Property 4: Error message should include an example
    assert "indicator > open" in error_msg, \
        f"Error message missing example: {error_msg}"
    
    # Property 5: Error message should be non-empty and descriptive
    assert len(error_msg) > 20, \
        f"Error message too short to be descriptive: {error_msg}"


@given(
    # Generate invalid operators
    invalid_operator=st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=5).filter(
        lambda x: x not in {">", "<", ">=", "<="}
    ),
    left_operand=st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=10),
    right_operand=st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=10)
)
@settings(max_examples=100)
def test_property_rule_validation_invalid_operator_errors(
    invalid_operator: str, left_operand: str, right_operand: str
) -> None:
    """Property 9b: Rule Validation Error Messages - Invalid Operator.
    
    **Validates: Requirements 4.4, 4.5**
    
    For any invalid rule specification with unsupported operators, the system
    should raise a descriptive AnalysisError explaining the validation failure
    and listing valid operators.
    
    This property ensures that:
    1. Invalid operators always raise AnalysisError
    2. Error messages contain "Invalid operator"
    3. Error messages list all valid operators
    4. The error is caught during parsing (ParsedRule validation)
    """
    from finance_cli.analysis import parse_rule
    
    # Construct rule string with invalid operator
    rule_string = f"{left_operand} {invalid_operator} {right_operand}"
    
    # Property 1: Invalid operator should always raise AnalysisError
    with pytest.raises(AnalysisError) as exc_info:
        parse_rule(rule_string)
    
    error_msg = str(exc_info.value)
    
    # Property 2: Error message should mention "Invalid operator"
    assert "Invalid operator" in error_msg, \
        f"Error message missing 'Invalid operator': {error_msg}"
    
    # Property 3: Error message should list valid operators
    assert "Valid operators" in error_msg or "valid operators" in error_msg, \
        f"Error message missing valid operators list: {error_msg}"
    
    # Property 4: Error message should include all valid operators
    for valid_op in [">", "<", ">=", "<="]:
        assert valid_op in error_msg, \
            f"Error message missing valid operator '{valid_op}': {error_msg}"
    
    # Property 5: Error message should be non-empty and descriptive
    assert len(error_msg) > 20, \
        f"Error message too short to be descriptive: {error_msg}"


@given(
    # Generate invalid operands (non-existent columns)
    invalid_operand=st.text(alphabet=RULE_TOKEN_ALPHABET, min_size=1, max_size=15).filter(
        lambda x: x not in {"open", "high", "low", "close", "indicator"}
    ),
    operator=st.sampled_from([">", "<", ">=", "<="]),
    operand_position=st.sampled_from(["left", "right"]),
    data_length=st.integers(min_value=3, max_value=10),
    seed=st.integers(min_value=0, max_value=10000)
)
@settings(max_examples=100)
def test_property_rule_validation_invalid_operand_errors(
    invalid_operand: str, operator: str, operand_position: str,
    data_length: int, seed: int
) -> None:
    """Property 9c: Rule Validation Error Messages - Invalid Operands.
    
    **Validates: Requirements 4.4, 4.5**
    
    For any invalid rule specification with operands referencing non-existent
    columns, the system should raise a descriptive AnalysisError explaining
    the validation failure and listing available columns.
    
    This property ensures that:
    1. Invalid operands always raise AnalysisError during evaluation
    2. Error messages identify the invalid operand
    3. Error messages list available columns
    4. Error messages are descriptive and actionable
    """
    from finance_cli.analysis import evaluate_rule, parse_rule
    import numpy as np
    
    # Set seed for reproducibility
    np.random.seed(seed)
    
    # Create a dataframe with standard OHLC columns
    dataframe = pd.DataFrame({
        "indicator_col": np.random.uniform(50, 150, data_length),
        "open": np.random.uniform(50, 150, data_length),
        "high": np.random.uniform(60, 160, data_length),
        "low": np.random.uniform(40, 140, data_length),
        "close": np.random.uniform(50, 150, data_length)
    })
    
    # Construct rule string with invalid operand
    if operand_position == "left":
        rule_string = f"{invalid_operand} {operator} open"
    else:
        rule_string = f"indicator {operator} {invalid_operand}"
    
    # Parse the rule (should succeed - format is valid)
    parsed_rule = parse_rule(rule_string)
    
    # Property 1: Invalid operand should raise AnalysisError during evaluation
    with pytest.raises(AnalysisError) as exc_info:
        evaluate_rule(dataframe, parsed_rule, "indicator_col")
    
    error_msg = str(exc_info.value)
    
    # Property 2: Error message should mention "non-existent column"
    assert "non-existent column" in error_msg, \
        f"Error message missing 'non-existent column': {error_msg}"
    
    # Property 3: Error message should identify the invalid operand
    assert invalid_operand in error_msg, \
        f"Error message missing invalid operand '{invalid_operand}': {error_msg}"
    
    # Property 4: Error message should list available columns
    assert "Available columns" in error_msg, \
        f"Error message missing 'Available columns': {error_msg}"
    
    # Property 5: Error message should include actual available columns
    for col in ["indicator_col", "open", "high", "low", "close"]:
        assert col in error_msg, \
            f"Error message missing available column '{col}': {error_msg}"
    
    # Property 6: Error message should be non-empty and descriptive
    assert len(error_msg) > 30, \
        f"Error message too short to be descriptive: {error_msg}"
    
    # Property 7: Error message should help user understand what went wrong
    # It should contain both the problem (invalid operand) and the solution (available columns)
    assert error_msg.count(invalid_operand) >= 1, \
        f"Error message should reference the invalid operand: {error_msg}"

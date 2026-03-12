"""Data preparation, analysis, formatting, and output writing."""

from __future__ import annotations

import pandas as pd

from .analysis_indicators import (
    IndicatorCalculator,
    IndicatorRegistry,
    calculate_ema,
    calculate_sma,
    calculate_wma,
    format_indicator_column_name,
    get_indicator_registry,
    validate_indicator_result,
)
from .analysis_output import (
    PRIMARY_GAP_COLUMN,
    SCREENING_RULE_COLUMN,
    SECONDARY_GAP_COLUMN,
    build_default_output_path,
    get_trailing_derived_columns,
    ordered_output_columns,
    render_filtered_rows,
    save_dataframe,
)
from .analysis_prepare import prepare_dataframe
from .analysis_rules import evaluate_rule, format_rule, parse_rule
from .errors import AnalysisError
from .models import AnalysisConfig


def analyze_dataframe_with_config(
    dataframe: pd.DataFrame,
    config: AnalysisConfig,
) -> pd.DataFrame:
    analyzed = dataframe.copy()
    indicator_column = format_indicator_column_name(config.indicator_type, config.months)
    calculator = get_indicator_registry().get(config.indicator_type)
    try:
        indicator_result = calculator(analyzed["open"], config.months)
    except Exception as exc:
        raise AnalysisError(
            f"Indicator calculation failed for '{config.indicator_type}': {exc}"
        ) from exc

    validate_indicator_result(
        indicator_result,
        indicator_type=config.indicator_type,
        row_count=len(analyzed),
    )

    parsed_rule = parse_rule(config.rule)
    normalized_rule = format_rule(parsed_rule)

    analyzed["moving_average_window_months"] = config.months
    analyzed[indicator_column] = indicator_result
    analyzed[PRIMARY_GAP_COLUMN] = (analyzed[indicator_column] - analyzed["open"]) / analyzed[
        "open"
    ]
    analyzed[SECONDARY_GAP_COLUMN] = (
        analyzed["open"] - analyzed[indicator_column]
    ) / analyzed[indicator_column]
    analyzed["condition"] = evaluate_rule(analyzed, parsed_rule, indicator_column)
    analyzed[SCREENING_RULE_COLUMN] = normalized_rule
    return analyzed


def analyze_dataframe(dataframe: pd.DataFrame, months: int) -> pd.DataFrame:
    result = analyze_dataframe_with_config(dataframe, AnalysisConfig(months=months))
    sma_column = format_indicator_column_name("sma", months)

    if sma_column in result.columns:
        result = result.rename(columns={sma_column: "Moving_Average"})
    if SCREENING_RULE_COLUMN in result.columns:
        result = result.drop(columns=[SCREENING_RULE_COLUMN])

    return result

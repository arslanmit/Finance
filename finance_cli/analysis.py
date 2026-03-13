"""Data preparation, analysis, formatting, and output writing."""

from __future__ import annotations

import pandas as pd

from . import analysis_indicators as _analysis_indicators
from . import analysis_output as _analysis_output
from . import analysis_prepare as _analysis_prepare
from . import analysis_rules as _analysis_rules
from .errors import AnalysisError
from .models import AnalysisConfig

IndicatorCalculator = _analysis_indicators.IndicatorCalculator
IndicatorRegistry = _analysis_indicators.IndicatorRegistry
calculate_ema = _analysis_indicators.calculate_ema
calculate_sma = _analysis_indicators.calculate_sma
calculate_wma = _analysis_indicators.calculate_wma
format_indicator_column_name = _analysis_indicators.format_indicator_column_name
get_indicator_registry = _analysis_indicators.get_indicator_registry
validate_indicator_result = _analysis_indicators.validate_indicator_result

PRIMARY_GAP_COLUMN = _analysis_output.PRIMARY_GAP_COLUMN
SCREENING_RULE_COLUMN = _analysis_output.SCREENING_RULE_COLUMN
SECONDARY_GAP_COLUMN = _analysis_output.SECONDARY_GAP_COLUMN
build_default_output_path = _analysis_output.build_default_output_path
get_trailing_derived_columns = _analysis_output.get_trailing_derived_columns
ordered_output_columns = _analysis_output.ordered_output_columns
render_filtered_rows = _analysis_output.render_filtered_rows
save_dataframe = _analysis_output.save_dataframe

prepare_dataframe = _analysis_prepare.prepare_dataframe

evaluate_rule = _analysis_rules.evaluate_rule
format_rule = _analysis_rules.format_rule
parse_rule = _analysis_rules.parse_rule


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


__all__ = [
    "AnalysisConfig",
    "IndicatorCalculator",
    "IndicatorRegistry",
    "PRIMARY_GAP_COLUMN",
    "SCREENING_RULE_COLUMN",
    "SECONDARY_GAP_COLUMN",
    "analyze_dataframe",
    "analyze_dataframe_with_config",
    "build_default_output_path",
    "calculate_ema",
    "calculate_sma",
    "calculate_wma",
    "evaluate_rule",
    "format_indicator_column_name",
    "format_rule",
    "get_indicator_registry",
    "get_trailing_derived_columns",
    "ordered_output_columns",
    "parse_rule",
    "prepare_dataframe",
    "render_filtered_rows",
    "save_dataframe",
    "validate_indicator_result",
]

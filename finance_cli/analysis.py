"""Data preparation, analysis, formatting, and output writing."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Protocol

import pandas as pd

from .errors import AnalysisError
from .models import AnalysisConfig, ParsedRule
from .sources import ensure_supported_file_suffix, ensure_symbol_column

PRIMARY_GAP_COLUMN = "moving_average_minus_open_over_open"
SECONDARY_GAP_COLUMN = "open_minus_moving_average_over_moving_average"
SCREENING_RULE_COLUMN = "screening_rule"
LEADING_OUTPUT_COLUMNS = [
    "symbol",
    PRIMARY_GAP_COLUMN,
    SECONDARY_GAP_COLUMN,
    "date",
    "open",
]
TRAILING_DERIVED_COLUMNS = ["moving_average_window_months", "condition", SCREENING_RULE_COLUMN]
INDICATOR_COLUMN_PATTERN = re.compile(r"^[A-Z]+_\d+_months$")


class IndicatorCalculator(Protocol):
    """Protocol for indicator calculation functions."""

    def __call__(self, series: pd.Series, window: int) -> pd.Series:
        """Calculate indicator values for a price series.
        
        Args:
            series: Price series to calculate indicator on
            window: Window size for the indicator calculation
            
        Returns:
            Series with indicator values (same length as input)
        """
        ...


class IndicatorRegistry:
    """Registry of available technical indicators."""

    def __init__(self) -> None:
        self._calculators: dict[str, IndicatorCalculator] = {}

    def register(self, name: str, calculator: IndicatorCalculator) -> None:
        """Register a new indicator calculator.
        
        Args:
            name: Indicator name (e.g., "sma", "ema", "wma")
            calculator: Function that calculates the indicator
            
        Raises:
            AnalysisError: If indicator name is already registered
        """
        normalized_name = name.strip().lower()
        if not normalized_name:
            raise AnalysisError("Indicator name cannot be empty")
        if not callable(calculator):
            raise AnalysisError(
                f"Indicator '{normalized_name}' must be registered with a callable calculator"
            )
        if normalized_name in self._calculators:
            raise AnalysisError(f"Indicator '{normalized_name}' is already registered")
        self._calculators[normalized_name] = calculator

    def get(self, name: str) -> IndicatorCalculator:
        """Retrieve an indicator calculator by name.
        
        Args:
            name: Indicator name to retrieve
            
        Returns:
            The indicator calculator function
            
        Raises:
            AnalysisError: If indicator name is not registered
        """
        normalized_name = name.strip().lower()
        if normalized_name not in self._calculators:
            available = ", ".join(sorted(self._calculators.keys()))
            raise AnalysisError(
                f"Unknown indicator type '{normalized_name}'. Available: {available}"
            )
        return self._calculators[normalized_name]

    def list_indicators(self) -> list[str]:
        """Return all registered indicator names.
        
        Returns:
            Sorted list of indicator names
        """
        return sorted(self._calculators.keys())


# Global registry instance
_INDICATOR_REGISTRY = IndicatorRegistry()


def get_indicator_registry() -> IndicatorRegistry:
    """Access the global indicator registry.
    
    Returns:
        The global IndicatorRegistry instance
    """
    return _INDICATOR_REGISTRY


def calculate_sma(series: pd.Series, window: int) -> pd.Series:
    """Calculate Simple Moving Average.

    Args:
        series: Price series to calculate SMA on
        window: Window size for the moving average

    Returns:
        Series with SMA values (same length as input)
    """
    return series.rolling(window=window).mean()


def calculate_ema(series: pd.Series, window: int) -> pd.Series:
    """Calculate Exponential Moving Average.

    Args:
        series: Price series to calculate EMA on
        window: Window size (span) for the exponential moving average

    Returns:
        Series with EMA values (same length as input)
    """
    return series.ewm(span=window, adjust=False).mean()


def calculate_wma(series: pd.Series, window: int) -> pd.Series:
    """Calculate Weighted Moving Average.

    Args:
        series: Price series to calculate WMA on
        window: Window size for the weighted moving average

    Returns:
        Series with WMA values (same length as input)
    """
    def weighted_mean(values: pd.Series) -> float:
        if len(values) < window:
            return float('nan')
        weights = pd.Series(range(1, window + 1))
        return (values.values * weights.values).sum() / weights.sum()

    return series.rolling(window=window).apply(weighted_mean, raw=False)


def parse_rule(rule_str: str) -> ParsedRule:
    """Parse a rule string into components.

    Expected format: "left_operand operator right_operand"
    Examples: "indicator > open", "indicator <= close"

    Args:
        rule_str: Rule string to parse

    Returns:
        ParsedRule instance with parsed components

    Raises:
        AnalysisError: If rule format is invalid
    """
    parts = rule_str.strip().split()
    if len(parts) != 3:
        raise AnalysisError(
            f"Invalid rule format: '{rule_str}'. "
            "Expected format: 'left_operand operator right_operand' "
            "(e.g., 'indicator > open')"
        )

    return ParsedRule(
        left_operand=parts[0],
        operator=parts[1],
        right_operand=parts[2]
    )


def format_rule(rule: ParsedRule) -> str:
    """Return a normalized string representation of a parsed rule."""
    return f"{rule.left_operand} {rule.operator} {rule.right_operand}"


def _resolve_rule_operand(operand: str, indicator_column: str) -> str:
    if operand.lower() == "indicator":
        return indicator_column
    return operand


def evaluate_rule(
    dataframe: pd.DataFrame,
    rule: ParsedRule,
    indicator_column: str
) -> pd.Series:
    """Evaluate a screening rule and return binary flags.

    Returns a Series of 1 (condition met) or 0 (condition not met).
    NaN values result in 0.

    Args:
        dataframe: DataFrame containing the data to evaluate
        rule: Parsed rule with left_operand, operator, right_operand
        indicator_column: Name of the indicator column in the dataframe

    Returns:
        Series of binary flags (1 for true, 0 for false)

    Raises:
        AnalysisError: If rule operands reference non-existent columns
    """
    # Resolve operands to column names
    left_col = _resolve_rule_operand(rule.left_operand, indicator_column)
    right_col = _resolve_rule_operand(rule.right_operand, indicator_column)

    # Validate columns exist
    for col_name, operand in [(left_col, rule.left_operand), (right_col, rule.right_operand)]:
        if col_name not in dataframe.columns:
            available = ", ".join(sorted(dataframe.columns))
            raise AnalysisError(
                f"Rule operand '{operand}' references non-existent column '{col_name}'. "
                f"Available columns: {available}"
            )

    left = dataframe[left_col]
    right = dataframe[right_col]

    # Apply comparison operator
    if rule.operator == ">":
        result = left > right
    elif rule.operator == "<":
        result = left < right
    elif rule.operator == ">=":
        result = left >= right
    elif rule.operator == "<=":
        result = left <= right
    else:
        raise AnalysisError(f"Unsupported operator: {rule.operator}")

    # Convert to binary flags, treating NaN as False (0)
    return result.fillna(False).astype(int)


# Register indicators at module initialization
_INDICATOR_REGISTRY.register("sma", calculate_sma)
_INDICATOR_REGISTRY.register("ema", calculate_ema)
_INDICATOR_REGISTRY.register("wma", calculate_wma)


def format_indicator_column_name(indicator_type: str, months: int) -> str:
    """Format the derived indicator column name."""
    return f"{indicator_type.upper()}_{months}_months"


def validate_indicator_result(
    indicator_result: pd.Series,
    *,
    indicator_type: str,
    row_count: int,
) -> None:
    """Validate an indicator result before it is added to the output."""
    if not isinstance(indicator_result, pd.Series):
        raise AnalysisError(
            f"Indicator '{indicator_type}' must return a pandas Series, "
            f"received {type(indicator_result).__name__}"
        )
    if len(indicator_result) != row_count:
        raise AnalysisError(
            f"Indicator '{indicator_type}' returned {len(indicator_result)} rows; "
            f"expected {row_count}"
        )
    if indicator_result.isna().all():
        raise AnalysisError(
            f"Indicator '{indicator_type}' produced all NaN values for the selected input."
        )



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


def build_default_output_path(input_path: Path) -> Path:
    return Path("output") / f"{input_path.stem}_processed.csv"


def render_filtered_rows(dataframe: pd.DataFrame) -> str:
    displayed = dataframe[ordered_output_columns(dataframe)]
    if displayed.empty:
        return "No rows are available for display."
    return displayed.to_string(index=False)


def save_dataframe(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_supported_file_suffix(output_path.suffix.lower(), kind="output")
    output_dataframe = ensure_symbol_column(dataframe)
    output_dataframe = output_dataframe[ordered_output_columns(output_dataframe)]
    output_dataframe.to_csv(output_path, index=False, date_format="%Y-%m-%d")


def get_trailing_derived_columns(dataframe: pd.DataFrame) -> list[str]:
    trailing_columns: list[str] = []
    indicator_columns = [
        column
        for column in dataframe.columns
        if INDICATOR_COLUMN_PATTERN.fullmatch(column)
    ]
    if "Moving_Average" in dataframe.columns:
        indicator_columns.append("Moving_Average")

    for column in [*indicator_columns, *TRAILING_DERIVED_COLUMNS]:
        if column in dataframe.columns and column not in trailing_columns:
            trailing_columns.append(column)

    return trailing_columns


def ordered_output_columns(dataframe: pd.DataFrame) -> list[str]:
    leading_columns = [column for column in LEADING_OUTPUT_COLUMNS if column in dataframe.columns]
    trailing_columns = [
        column
        for column in get_trailing_derived_columns(dataframe)
        if column not in leading_columns
    ]
    middle_columns = [
        column
        for column in dataframe.columns
        if column not in leading_columns and column not in trailing_columns
    ]
    return [*leading_columns, *middle_columns, *trailing_columns]

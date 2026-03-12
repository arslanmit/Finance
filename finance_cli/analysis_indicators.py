"""Indicator registry and calculator helpers."""

from __future__ import annotations

from typing import Protocol

import pandas as pd

from .errors import AnalysisError


class IndicatorCalculator(Protocol):
    """Protocol for indicator calculation functions."""

    def __call__(self, series: pd.Series, window: int) -> pd.Series:
        ...


class IndicatorRegistry:
    """Registry of available technical indicators."""

    def __init__(self) -> None:
        self._calculators: dict[str, IndicatorCalculator] = {}

    def register(self, name: str, calculator: IndicatorCalculator) -> None:
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
        normalized_name = name.strip().lower()
        if normalized_name not in self._calculators:
            available = ", ".join(sorted(self._calculators.keys()))
            raise AnalysisError(
                f"Unknown indicator type '{normalized_name}'. Available: {available}"
            )
        return self._calculators[normalized_name]

    def list_indicators(self) -> list[str]:
        return sorted(self._calculators.keys())


def get_indicator_registry() -> IndicatorRegistry:
    return _INDICATOR_REGISTRY


def calculate_sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window).mean()


def calculate_ema(series: pd.Series, window: int) -> pd.Series:
    return series.ewm(span=window, adjust=False).mean()


def calculate_wma(series: pd.Series, window: int) -> pd.Series:
    def weighted_mean(values: pd.Series) -> float:
        if len(values) < window:
            return float("nan")
        weights = pd.Series(range(1, window + 1))
        return (values.values * weights.values).sum() / weights.sum()

    return series.rolling(window=window).apply(weighted_mean, raw=False)


def format_indicator_column_name(indicator_type: str, months: int) -> str:
    return f"{indicator_type.upper()}_{months}_months"


def validate_indicator_result(
    indicator_result: pd.Series,
    *,
    indicator_type: str,
    row_count: int,
) -> None:
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


_INDICATOR_REGISTRY = IndicatorRegistry()
_INDICATOR_REGISTRY.register("sma", calculate_sma)
_INDICATOR_REGISTRY.register("ema", calculate_ema)
_INDICATOR_REGISTRY.register("wma", calculate_wma)

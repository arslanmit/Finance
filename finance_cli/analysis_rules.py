"""Rule parsing and evaluation helpers for analysis workflows."""

from __future__ import annotations

import pandas as pd

from .errors import AnalysisError
from .models import ParsedRule


def parse_rule(rule_str: str) -> ParsedRule:
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
        right_operand=parts[2],
    )


def format_rule(rule: ParsedRule) -> str:
    return f"{rule.left_operand} {rule.operator} {rule.right_operand}"


def _resolve_rule_operand(operand: str, indicator_column: str) -> str:
    if operand.lower() == "indicator":
        return indicator_column
    return operand


def evaluate_rule(
    dataframe: pd.DataFrame,
    rule: ParsedRule,
    indicator_column: str,
) -> pd.Series:
    left_col = _resolve_rule_operand(rule.left_operand, indicator_column)
    right_col = _resolve_rule_operand(rule.right_operand, indicator_column)

    for col_name, operand in [(left_col, rule.left_operand), (right_col, rule.right_operand)]:
        if col_name not in dataframe.columns:
            available = ", ".join(sorted(dataframe.columns))
            raise AnalysisError(
                f"Rule operand '{operand}' references non-existent column '{col_name}'. "
                f"Available columns: {available}"
            )

    left = dataframe[left_col]
    right = dataframe[right_col]

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

    return result.fillna(False).astype(int)

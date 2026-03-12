"""Argument parser construction for the Finance CLI."""

from __future__ import annotations

import argparse

from .analysis import get_indicator_registry


def build_parser() -> argparse.ArgumentParser:
    available_indicators = ", ".join(get_indicator_registry().list_indicators())
    parser = argparse.ArgumentParser(
        description="Finance dataset analysis CLI with guided and command-based workflows."
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run configurable indicator analysis.")
    run_group = run_parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument("--dataset", help="Dataset id from data/generated.")
    run_group.add_argument("--file", help="Path to a CSV file.")
    run_parser.add_argument("--months", type=int, required=True, help="Indicator window size.")
    run_parser.add_argument(
        "--indicator",
        default="sma",
        help=f"Indicator type to calculate (available: {available_indicators}).",
    )
    run_parser.add_argument(
        "--rule",
        default="indicator > open",
        help="Screening rule in the form 'left_operand operator right_operand'.",
    )
    run_parser.add_argument("--output", help="Optional output CSV path.")
    run_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh a generated symbol-backed dataset before analysis.",
    )

    matrix_parser = subparsers.add_parser(
        "matrix",
        help="Run the fixed indicator/rule/month matrix across all generated datasets.",
    )
    matrix_parser.add_argument(
        "--output-dir",
        help="Optional directory for matrix outputs and manifest.",
    )

    datasets_parser = subparsers.add_parser("datasets", help="Manage generated datasets.")
    datasets_subparsers = datasets_parser.add_subparsers(dest="datasets_command", required=True)

    datasets_subparsers.add_parser("list", help="List all generated datasets.")

    add_parser = datasets_subparsers.add_parser(
        "add",
        help="Copy a CSV into data/generated using its filename.",
    )
    add_parser.add_argument("--path", required=True, help="Path to the dataset CSV file.")
    add_parser.add_argument(
        "--refresh-symbol",
        help="Yahoo Finance symbol for live refresh support.",
    )

    create_parser = datasets_subparsers.add_parser(
        "create",
        help="Create a new dataset from a Yahoo Finance symbol.",
    )
    create_parser.add_argument("--symbol", required=True, help="Yahoo Finance symbol, e.g. SPY or AAPL.")

    remove_parser = datasets_subparsers.add_parser("remove", help="Remove a generated dataset.")
    remove_parser.add_argument("--id", required=True, help="Dataset id to remove.")

    refresh_parser = datasets_subparsers.add_parser(
        "refresh",
        help="Refresh generated symbol-backed datasets from Yahoo Finance.",
    )
    refresh_group = refresh_parser.add_mutually_exclusive_group(required=True)
    refresh_group.add_argument("--id", help="Dataset id to refresh.")
    refresh_group.add_argument(
        "--all",
        action="store_true",
        help="Refresh all generated datasets that support live refresh.",
    )

    return parser

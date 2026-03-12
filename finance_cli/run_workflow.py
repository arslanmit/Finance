"""Shared analysis and refresh workflows used by the CLI."""

from __future__ import annotations

from pathlib import Path

from .analysis import (
    analyze_dataframe,
    analyze_dataframe_with_config,
    format_rule,
    parse_rule,
    prepare_dataframe,
    render_filtered_rows,
    save_dataframe,
)
from .catalog import get_dataset
from .errors import FinanceCliError
from .models import AnalysisConfig, DatasetConfig, RefreshSummary, ResolvedSource
from .presentation import print_refresh_summary
from .refresh import refresh_selected_source
from .sources import ensure_symbol_column, load_dataframe, resolve_dataset_source


def execute_analysis(
    source: ResolvedSource,
    *,
    config: AnalysisConfig,
    output_path: Path,
    refresh_requested: bool,
) -> None:
    if refresh_requested:
        summary = refresh_selected_source(source)
        print_refresh_summary(summary)

    raw_dataframe = load_dataframe(source.input_path)
    raw_dataframe = ensure_symbol_column(
        raw_dataframe,
        None if source.dataset is None else source.dataset.symbol,
    )
    prepared_dataframe = prepare_dataframe(raw_dataframe, config.months)
    normalized_rule = format_rule(parse_rule(config.rule))
    use_legacy_defaults = (
        config.indicator_type.strip().lower() == "sma"
        and normalized_rule == "indicator > open"
    )
    if use_legacy_defaults:
        analyzed_dataframe = analyze_dataframe(prepared_dataframe, config.months)
    else:
        analyzed_dataframe = analyze_dataframe_with_config(prepared_dataframe, config)

    print(f"Indicator: {config.indicator_type.upper()} (window={config.months})")
    print(f"Rule: {normalized_rule}\n")
    print(render_filtered_rows(analyzed_dataframe))
    save_dataframe(analyzed_dataframe, output_path)
    print(f"\nProcessed data saved to: {output_path}")


def refresh_generated_datasets(
    datasets: list[DatasetConfig],
    *,
    dataset_id: str | None,
    refresh_all: bool,
) -> list[tuple[DatasetConfig, RefreshSummary]]:
    if refresh_all:
        targets = [dataset for dataset in datasets if dataset.supports_refresh]
        if not targets:
            raise FinanceCliError("No generated datasets support live refresh.")
    else:
        if dataset_id is None:
            raise FinanceCliError("A dataset id is required unless --all is used.")
        dataset = get_dataset(dataset_id, datasets)
        if not dataset.supports_refresh:
            raise FinanceCliError(f"Dataset '{dataset.id}' does not support live refresh.")
        targets = [dataset]

    refreshed: list[tuple[DatasetConfig, RefreshSummary]] = []
    for dataset in targets:
        summary = refresh_selected_source(resolve_dataset_source(dataset))
        refreshed.append((dataset, summary))
    return refreshed

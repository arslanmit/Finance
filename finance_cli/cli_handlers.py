"""Command dispatch and command handlers for the Finance CLI."""

from __future__ import annotations

import argparse
from pathlib import Path

from .analysis import build_default_output_path
from .catalog import discover_datasets, get_dataset, import_dataset, remove_dataset
from .create import create_symbol_dataset
from .errors import FinanceCliError
from .matrix import build_matrix_jobs, build_matrix_output_dir, run_matrix_jobs, write_matrix_manifest
from .models import AnalysisConfig, DatasetConfig, ResolvedSource
from .presentation import print_dataset_list, print_dataset_refresh_summary
from .run_workflow import execute_analysis, refresh_generated_datasets
from .sources import resolve_custom_source, resolve_dataset_source


def dispatch_command(args: argparse.Namespace) -> int:
    if args.command == "run":
        handle_run_command(args)
        return 0
    if args.command == "matrix":
        handle_matrix_command(args)
        return 0
    if args.command == "datasets":
        handle_datasets_command(args)
        return 0
    raise FinanceCliError("Unknown command.")


def handle_run_command(args: argparse.Namespace) -> None:
    datasets = discover_datasets() if args.dataset else []
    source = resolve_run_source(args, datasets)
    output_path = Path(args.output).expanduser() if args.output else build_default_output_path(source.input_path)
    config = AnalysisConfig(
        months=args.months,
        indicator_type=args.indicator.strip().lower(),
        rule=args.rule,
    )
    execute_analysis(source, config=config, output_path=output_path, refresh_requested=args.refresh)


def handle_matrix_command(args: argparse.Namespace) -> None:
    datasets = discover_datasets()
    if not datasets:
        raise FinanceCliError("No generated datasets were found for matrix execution.")

    jobs = build_matrix_jobs()
    output_dir = build_matrix_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    total_jobs = len(datasets) * len(jobs)
    print(
        f"Matrix run starting: datasets={len(datasets)}, jobs_per_dataset={len(jobs)}, "
        f"total_jobs={total_jobs}, output_dir={output_dir}"
    )

    records = run_matrix_jobs(datasets, jobs, output_dir)
    manifest_path = write_matrix_manifest(records, output_dir)
    success_count = sum(record.status == "success" for record in records)
    failed_count = len(records) - success_count
    print(
        f"Matrix run complete: total_jobs={len(records)}, succeeded={success_count}, "
        f"failed={failed_count}, manifest={manifest_path}"
    )


def resolve_run_source(args: argparse.Namespace, datasets: list[DatasetConfig]) -> ResolvedSource:
    if args.dataset:
        dataset = get_dataset(args.dataset, datasets)
        return resolve_dataset_source(dataset)

    return resolve_custom_source(args.file)


def handle_datasets_command(args: argparse.Namespace) -> None:
    if args.datasets_command == "list":
        print_dataset_list(discover_datasets())
        return

    if args.datasets_command == "add":
        dataset = import_dataset(
            source_path=args.path,
            refresh_symbol=args.refresh_symbol,
        )
        print(f"Added dataset '{dataset.id}' -> {dataset.path}")
        return

    if args.datasets_command == "create":
        dataset = create_symbol_dataset(args.symbol)
        print(
            f"Created dataset '{dataset.id}' from symbol {dataset.refresh.symbol} -> {dataset.path}"
        )
        return

    if args.datasets_command == "remove":
        removed = remove_dataset(args.id)
        print(f"Removed dataset '{removed.id}'")
        return

    if args.datasets_command == "refresh":
        refreshed = refresh_generated_datasets(
            discover_datasets(),
            dataset_id=args.id,
            refresh_all=args.all,
        )
        for dataset, summary in refreshed:
            print_dataset_refresh_summary(dataset, summary)
        return

    raise FinanceCliError("Unknown datasets command.")

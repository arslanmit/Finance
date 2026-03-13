"""Execution loop for matrix jobs."""

from __future__ import annotations

from pathlib import Path

from .analysis import analyze_dataframe_with_config, prepare_dataframe, save_dataframe
from .matrix import MatrixJob, MatrixRunRecord, build_matrix_output_path, build_matrix_record
from .models import AnalysisConfig, DatasetConfig
from .sources import ensure_symbol_column, load_dataframe, resolve_dataset_source


def group_jobs_by_month(jobs: list[MatrixJob]) -> list[tuple[int, list[MatrixJob]]]:
    grouped: dict[int, list[MatrixJob]] = {}
    for job in jobs:
        grouped.setdefault(job.months, []).append(job)
    return list(grouped.items())


def run_matrix_jobs(
    datasets: list[DatasetConfig],
    jobs: list[MatrixJob],
    output_dir: Path,
) -> list[MatrixRunRecord]:
    records: list[MatrixRunRecord] = []
    jobs_by_month = group_jobs_by_month(jobs)

    for index, dataset in enumerate(datasets, start=1):
        print(f"[{index}/{len(datasets)}] Running dataset '{dataset.id}'")
        source = resolve_dataset_source(dataset)
        input_path = source.input_path
        try:
            raw_dataframe = load_dataframe(input_path)
            raw_dataframe = ensure_symbol_column(raw_dataframe, dataset.symbol)
        except Exception as exc:
            for job in jobs:
                output_path = build_matrix_output_path(output_dir, dataset.id, job)
                records.append(
                    build_matrix_record(
                        dataset=dataset,
                        input_path=input_path,
                        job=job,
                        output_path=output_path,
                        status="error",
                        row_count=None,
                        condition_true_count=None,
                        error=str(exc),
                    )
                )
            continue

        for months, month_jobs in jobs_by_month:
            try:
                prepared_dataframe = prepare_dataframe(raw_dataframe, months)
            except Exception as exc:
                for job in month_jobs:
                    output_path = build_matrix_output_path(output_dir, dataset.id, job)
                    records.append(
                        build_matrix_record(
                            dataset=dataset,
                            input_path=input_path,
                            job=job,
                            output_path=output_path,
                            status="error",
                            row_count=None,
                            condition_true_count=None,
                            error=str(exc),
                        )
                    )
                continue

            for job in month_jobs:
                output_path = build_matrix_output_path(output_dir, dataset.id, job)
                config = AnalysisConfig(
                    months=job.months,
                    indicator_type=job.indicator,
                    rule=job.rule,
                )
                try:
                    analyzed_dataframe = analyze_dataframe_with_config(prepared_dataframe, config)
                    save_dataframe(analyzed_dataframe, output_path)
                    records.append(
                        build_matrix_record(
                            dataset=dataset,
                            input_path=input_path,
                            job=job,
                            output_path=output_path,
                            status="success",
                            row_count=len(analyzed_dataframe),
                            condition_true_count=int(analyzed_dataframe["condition"].sum()),
                        )
                    )
                except Exception as exc:
                    records.append(
                        build_matrix_record(
                            dataset=dataset,
                            input_path=input_path,
                            job=job,
                            output_path=output_path,
                            status="error",
                            row_count=None,
                            condition_true_count=None,
                            error=str(exc),
                        )
                    )

    return records


__all__ = ["group_jobs_by_month", "run_matrix_jobs"]

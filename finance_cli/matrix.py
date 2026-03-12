"""Matrix job generation and execution helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from .analysis import analyze_dataframe_with_config, get_indicator_registry, parse_rule, prepare_dataframe, save_dataframe
from .models import AnalysisConfig, DatasetConfig
from .sources import ensure_symbol_column, load_dataframe, resolve_dataset_source

MATRIX_MONTHS = (1, 3, 6, 12, 24)
MATRIX_RULE_OPERATORS = (">", "<", ">=", "<=")
MATRIX_RULE_COLUMNS = ("open", "high", "low", "close")
RULE_SLUG_OPERATOR_MAP = {">": "gt", "<": "lt", ">=": "gte", "<=": "lte"}


@dataclass(frozen=True)
class MatrixJob:
    """Single matrix execution job."""

    months: int
    indicator: str
    rule: str


@dataclass(frozen=True)
class MatrixRunRecord:
    """Manifest row for a matrix execution result."""

    dataset_id: str
    symbol: str
    input_path: str
    months: int
    indicator: str
    rule: str
    status: str
    output_path: str
    row_count: int | None
    condition_true_count: int | None
    error: str


def build_matrix_jobs() -> list[MatrixJob]:
    jobs: list[MatrixJob] = []
    indicators = get_indicator_registry().list_indicators()
    for months in MATRIX_MONTHS:
        for indicator in indicators:
            for operator in MATRIX_RULE_OPERATORS:
                for column in MATRIX_RULE_COLUMNS:
                    jobs.append(
                        MatrixJob(
                            months=months,
                            indicator=indicator,
                            rule=f"indicator {operator} {column}",
                        )
                    )
    return jobs


def build_matrix_output_dir(output_dir: str | None) -> Path:
    if output_dir:
        return Path(output_dir).expanduser()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return Path("output") / "matrix" / timestamp


def slugify_rule(rule: str) -> str:
    parsed_rule = parse_rule(rule)
    operator_slug = RULE_SLUG_OPERATOR_MAP[parsed_rule.operator]
    return f"{parsed_rule.left_operand.lower()}-{operator_slug}-{parsed_rule.right_operand.lower()}"


def build_matrix_output_path(output_dir: Path, dataset_id: str, job: MatrixJob) -> Path:
    file_name = f"{dataset_id}__m{job.months}__{job.indicator}__{slugify_rule(job.rule)}.csv"
    return output_dir / dataset_id / file_name


def build_matrix_record(
    *,
    dataset: DatasetConfig,
    input_path: Path,
    job: MatrixJob,
    output_path: Path,
    status: str,
    row_count: int | None,
    condition_true_count: int | None,
    error: str = "",
) -> MatrixRunRecord:
    return MatrixRunRecord(
        dataset_id=dataset.id,
        symbol=dataset.symbol or "",
        input_path=str(input_path),
        months=job.months,
        indicator=job.indicator,
        rule=job.rule,
        status=status,
        output_path=str(output_path),
        row_count=row_count,
        condition_true_count=condition_true_count,
        error=error,
    )


def run_matrix_jobs(
    datasets: list[DatasetConfig],
    jobs: list[MatrixJob],
    output_dir: Path,
) -> list[MatrixRunRecord]:
    records: list[MatrixRunRecord] = []
    jobs_by_month = {
        months: [job for job in jobs if job.months == months]
        for months in MATRIX_MONTHS
    }

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

        for months in MATRIX_MONTHS:
            month_jobs = jobs_by_month[months]
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


def write_matrix_manifest(records: list[MatrixRunRecord], output_dir: Path) -> Path:
    manifest_path = output_dir / "manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([asdict(record) for record in records]).to_csv(manifest_path, index=False)
    return manifest_path

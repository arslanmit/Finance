from pathlib import Path

import pandas as pd
import pytest

from finance_cli.errors import AnalysisError
from finance_cli.matrix import (
    build_matrix_jobs,
    build_matrix_output_path,
    slugify_rule,
    write_matrix_manifest,
)
from finance_cli.matrix_runner import run_matrix_jobs
from finance_cli.models import AnalysisConfig, DatasetConfig, RefreshMetadata


def build_price_dataframe(row_count: int = 3) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=row_count, freq="MS")
    open_values = [float(10 + index) for index in range(row_count)]
    return pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "open": open_values,
            "high": [value + 1.0 for value in open_values],
            "low": [value - 1.0 for value in open_values],
            "close": [value + 0.5 for value in open_values],
            "volume": [100 + index for index in range(row_count)],
        }
    )


def write_csv(path: Path, *, symbol: str | None = None, row_count: int = 3) -> None:
    dataframe = build_price_dataframe(row_count)
    if symbol is not None:
        dataframe.insert(0, "symbol", symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path, index=False)


def build_dataset(tmp_path: Path, dataset_id: str = "nvda", symbol: str = "NVDA") -> DatasetConfig:
    path = f"data/generated/{dataset_id}.csv"
    return DatasetConfig(
        id=dataset_id,
        label=dataset_id,
        path=path,
        refresh=RefreshMetadata(provider="yahoo", symbol=symbol),
        base_dir=tmp_path,
    )


def test_build_matrix_jobs_returns_fixed_240_job_matrix() -> None:
    jobs = build_matrix_jobs()

    assert len(jobs) == 240
    assert len({(job.months, job.indicator, job.rule) for job in jobs}) == 240
    assert len({job.rule for job in jobs}) == 16
    assert jobs[0].months == 1
    assert jobs[0].indicator == "ema"
    assert jobs[0].rule == "indicator > open"
    assert jobs[-1].months == 24
    assert jobs[-1].indicator == "wma"
    assert jobs[-1].rule == "indicator <= close"


@pytest.mark.parametrize(
    ("rule", "expected"),
    [
        ("indicator > open", "indicator-gt-open"),
        ("indicator < high", "indicator-lt-high"),
        ("indicator >= low", "indicator-gte-low"),
        ("indicator <= close", "indicator-lte-close"),
    ],
)
def test_slugify_rule_maps_supported_operators(rule: str, expected: str) -> None:
    assert slugify_rule(rule) == expected


def test_build_matrix_output_path_is_deterministic(tmp_path: Path) -> None:
    job = build_matrix_jobs()[-1]

    path = build_matrix_output_path(tmp_path, "nvda", job)

    assert path == tmp_path / "nvda" / "nvda__m24__wma__indicator-lte-close.csv"


def test_write_matrix_manifest_writes_expected_columns(tmp_path: Path) -> None:
    dataset = build_dataset(tmp_path)
    output_dir = tmp_path / "matrix-output"
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA", row_count=3)
    records = run_matrix_jobs([dataset], build_matrix_jobs()[:1], output_dir)

    manifest_path = write_matrix_manifest(records, output_dir)

    manifest = pd.read_csv(manifest_path)
    assert list(manifest.columns) == [
        "dataset_id",
        "symbol",
        "input_path",
        "months",
        "indicator",
        "rule",
        "status",
        "output_path",
        "row_count",
        "condition_true_count",
        "error",
    ]


def test_run_matrix_jobs_records_prepare_failures_per_affected_month(
    tmp_path: Path,
) -> None:
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA", row_count=12)
    output_dir = tmp_path / "matrix-output"
    dataset = build_dataset(tmp_path)

    records = run_matrix_jobs([dataset], build_matrix_jobs(), output_dir)

    manifest = pd.DataFrame(record.__dict__ for record in records)
    failing_rows = manifest[manifest["status"] == "error"]
    success_rows = manifest[manifest["status"] == "success"]

    assert len(manifest) == 240
    assert len(failing_rows) == 48
    assert failing_rows["months"].eq(24).all()
    assert failing_rows["error"].str.contains("Months must be between 1 and 12").all()
    assert len(success_rows) == 192


def test_run_matrix_jobs_records_analysis_failure_and_continues(
    tmp_path: Path,
    monkeypatch,
) -> None:
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA", row_count=30)
    output_dir = tmp_path / "matrix-output"
    dataset = build_dataset(tmp_path)

    from finance_cli.analysis import analyze_dataframe_with_config as real_analyze_dataframe_with_config

    def fake_analyze(dataframe: pd.DataFrame, config: AnalysisConfig) -> pd.DataFrame:
        if config.months == 12 and config.indicator_type == "ema" and config.rule == "indicator <= close":
            raise AnalysisError("forced matrix analysis failure")
        return real_analyze_dataframe_with_config(dataframe, config)

    monkeypatch.setattr("finance_cli.matrix_runner.analyze_dataframe_with_config", fake_analyze)

    records = run_matrix_jobs([dataset], build_matrix_jobs(), output_dir)

    manifest = pd.DataFrame(record.__dict__ for record in records)
    failure = manifest[
        (manifest["months"] == 12)
        & (manifest["indicator"] == "ema")
        & (manifest["rule"] == "indicator <= close")
    ]

    assert len(manifest) == 240
    assert len(failure) == 1
    assert failure.iloc[0]["status"] == "error"
    assert "forced matrix analysis failure" in failure.iloc[0]["error"]
    assert manifest["status"].eq("success").sum() == 239
    assert (output_dir / "nvda" / "nvda__m12__ema__indicator-gt-open.csv").exists()

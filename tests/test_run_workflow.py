from pathlib import Path
from unittest.mock import patch

import pandas as pd

from finance_cli.models import AnalysisConfig, DatasetConfig, RefreshMetadata, RefreshSummary, ResolvedSource
from finance_cli.run_workflow import execute_analysis, refresh_generated_datasets


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


def test_execute_analysis_writes_legacy_output_for_default_config(
    tmp_path: Path,
    capsys,
) -> None:
    csv_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    write_csv(csv_path)

    execute_analysis(
        ResolvedSource(input_path=csv_path, dataset=None),
        config=AnalysisConfig(months=2),
        output_path=output_path,
        refresh_requested=False,
    )

    output = capsys.readouterr().out
    written = pd.read_csv(output_path)

    assert "Indicator: SMA (window=2)" in output
    assert "Rule: indicator > open" in output
    assert "Moving_Average" in written.columns
    assert "screening_rule" not in written.columns


def test_refresh_generated_datasets_returns_only_refreshable_matches() -> None:
    refreshable = DatasetConfig(
        id="nvda",
        label="nvda",
        path="data/generated/nvda.csv",
        refresh=RefreshMetadata(provider="yahoo", symbol="NVDA"),
        base_dir=Path("/tmp"),
    )
    non_refreshable = DatasetConfig(
        id="custom",
        label="custom",
        path="data/generated/custom.csv",
        refresh=None,
        base_dir=Path("/tmp"),
    )

    with patch("finance_cli.run_workflow.refresh_selected_source") as mock_refresh, patch(
        "finance_cli.run_workflow.resolve_dataset_source",
        return_value=ResolvedSource(input_path=Path("/tmp/data/generated/nvda.csv"), dataset=refreshable),
    ):
        mock_refresh.return_value = RefreshSummary(
            symbol="NVDA",
            row_count=30,
            min_date="2024-01-01",
            max_date="2026-01-01",
            backup_path="/tmp/backup.csv",
        )

        refreshed = refresh_generated_datasets(
            [refreshable, non_refreshable],
            dataset_id=None,
            refresh_all=True,
        )

    assert refreshed == [(refreshable, mock_refresh.return_value)]

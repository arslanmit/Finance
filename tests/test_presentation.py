from pathlib import Path

from finance_cli.models import DatasetConfig, RefreshMetadata, RefreshSummary
from finance_cli.presentation import print_dataset_list, print_refresh_summary


def test_print_dataset_list_formats_sorted_output(capsys) -> None:
    datasets = [
        DatasetConfig(
            id="nvda",
            label="nvda",
            path="data/generated/nvda.csv",
            refresh=RefreshMetadata(provider="yahoo", symbol="NVDA"),
            base_dir=Path("/tmp"),
        ),
        DatasetConfig(
            id="custom",
            label="custom",
            path="data/generated/custom.csv",
            refresh=None,
            base_dir=Path("/tmp"),
        ),
    ]

    print_dataset_list(datasets)
    output = capsys.readouterr().out

    assert "Available datasets:" in output
    assert "- custom | file: custom.csv | refresh: no" in output
    assert "- nvda | file: nvda.csv | refresh: yes" in output


def test_print_refresh_summary_formats_expected_fields(capsys) -> None:
    print_refresh_summary(
        RefreshSummary(
            symbol="NVDA",
            row_count=30,
            min_date="2024-01-01",
            max_date="2026-01-01",
            backup_path="/tmp/backup.csv",
        )
    )
    output = capsys.readouterr().out

    assert "Refresh summary: symbol=NVDA" in output
    assert "range=2024-01-01..2026-01-01" in output
    assert "rows=30" in output
    assert "backup=/tmp/backup.csv" in output

from pathlib import Path

import pytest

from finance_cli.errors import RefreshError
from finance_cli.models import DatasetConfig, RefreshMetadata, ResolvedSource
from finance_cli.refresh import validate_refreshable_source


def test_validate_refresh_rejects_custom_file(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("date,open\n2024-01-01,10\n", encoding="utf-8")
    source = ResolvedSource(input_path=csv_path, sheet_name=None, dataset=None)

    with pytest.raises(RefreshError, match="registered datasets"):
        validate_refreshable_source(source)


def test_validate_refresh_requires_sheet(tmp_path: Path) -> None:
    workbook_path = tmp_path / "sample.xlsx"
    workbook_path.write_text("", encoding="utf-8")
    dataset = DatasetConfig(
        id="sample",
        label="Sample",
        path="sample.xlsx",
        sheet=None,
        refresh=RefreshMetadata(provider="yahoo", symbol="500.PA"),
        base_dir=tmp_path,
    )
    source = ResolvedSource(input_path=workbook_path, sheet_name=None, dataset=dataset)

    with pytest.raises(RefreshError, match="configured Excel sheet"):
        validate_refreshable_source(source)


def test_validate_refresh_rejects_unsupported_provider(tmp_path: Path) -> None:
    workbook_path = tmp_path / "sample.xlsx"
    workbook_path.write_text("", encoding="utf-8")
    dataset = DatasetConfig(
        id="sample",
        label="Sample",
        path="sample.xlsx",
        sheet="Sheet1",
        refresh=RefreshMetadata(provider="custom", symbol="500.PA"),
        base_dir=tmp_path,
    )
    source = ResolvedSource(input_path=workbook_path, sheet_name="Sheet1", dataset=dataset)

    with pytest.raises(RefreshError, match="unsupported refresh provider"):
        validate_refreshable_source(source)

import argparse
from pathlib import Path
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest
from hypothesis import given, settings, strategies as st

from finance_cli.catalog import discover_datasets
from finance_cli.cli import (
    build_matrix_jobs,
    build_matrix_output_path,
    build_parser,
    build_wizard_menu_items,
    dispatch_command,
    main,
    slugify_rule,
)
from finance_cli.errors import AnalysisError
from finance_cli.models import AnalysisConfig, DatasetConfig, RefreshMetadata
from finance_cli.models import RefreshSummary


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


def monthly_frame(row_count: int = 3) -> pd.DataFrame:
    dataframe = build_price_dataframe(row_count)
    dataframe["date"] = pd.to_datetime(dataframe["date"])
    return dataframe


def test_main_without_args_runs_wizard(monkeypatch) -> None:
    called = {"wizard": False}

    def fake_run_wizard() -> None:
        called["wizard"] = True

    monkeypatch.setattr("finance_cli.cli.run_wizard", fake_run_wizard)

    assert main([]) == 0
    assert called["wizard"] is True


def test_main_returns_parser_exit_code_for_invalid_args(capsys) -> None:
    assert main(["run"]) == 2
    assert "usage:" in capsys.readouterr().err


def test_dispatch_command_routes_run_command(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr("finance_cli.cli.handle_run_command", lambda args: calls.append("run"))

    assert dispatch_command(argparse.Namespace(command="run")) == 0
    assert calls == ["run"]


def test_print_dataset_list_formats_sorted_output(capsys) -> None:
    from finance_cli.presentation import print_dataset_list

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
    from finance_cli.presentation import print_refresh_summary

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


def test_datasets_list_command_uses_generated_discovery_only(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "generated" / "500_pa.csv", symbol="500.PA")
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA")
    write_csv(tmp_path / "data" / "live" / "default.csv", symbol="500.PA")
    write_csv(tmp_path / "data" / "imported" / "sample_imported.csv")

    code = main(["datasets", "list"])
    output = capsys.readouterr().out

    assert code == 0
    assert "- 500_pa | file: 500_pa.csv | refresh: yes" in output
    assert "- nvda | file: nvda.csv | refresh: yes" in output
    assert "default" not in output
    assert "sample_imported" not in output


@pytest.mark.parametrize(
    ("relative_path", "dataset_id", "expected_symbol"),
    [
        ("data/generated/500_pa.csv", "500_pa", "500.PA"),
        ("data/generated/nvda.csv", "nvda", "NVDA"),
    ],
)
def test_run_command_with_generated_dataset(
    tmp_path: Path,
    monkeypatch,
    capsys,
    relative_path: str,
    dataset_id: str,
    expected_symbol: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / relative_path, symbol=expected_symbol)

    code = main(["run", "--dataset", dataset_id, "--months", "2"])
    output = capsys.readouterr().out

    assert code == 0
    assert (tmp_path / "output" / f"{dataset_id}_processed.csv").exists()
    assert expected_symbol in output
    written = pd.read_csv(tmp_path / "output" / f"{dataset_id}_processed.csv")
    assert "Moving_Average" in written.columns
    assert "screening_rule" not in written.columns


def test_run_command_with_custom_file(tmp_path: Path, capsys) -> None:
    csv_path = tmp_path / "unmanaged.csv"
    output_path = tmp_path / "custom_output.csv"
    write_csv(csv_path)

    code = main(["run", "--file", str(csv_path), "--months", "2", "--output", str(output_path)])
    output = capsys.readouterr().out

    assert code == 0
    assert "Processed data saved to:" in output
    assert output_path.exists()
    written = pd.read_csv(output_path)
    assert "Moving_Average" in written.columns
    assert "screening_rule" not in written.columns


def test_run_command_with_indicator_and_rule(tmp_path: Path, capsys) -> None:
    csv_path = tmp_path / "unmanaged.csv"
    output_path = tmp_path / "custom_output.csv"
    write_csv(csv_path)

    code = main(
        [
            "run",
            "--file",
            str(csv_path),
            "--months",
            "2",
            "--indicator",
            "ema",
            "--rule",
            "indicator < close",
            "--output",
            str(output_path),
        ]
    )
    output = capsys.readouterr().out

    assert code == 0
    assert "Indicator: EMA (window=2)" in output
    assert "Rule: indicator < close" in output
    written = pd.read_csv(output_path)
    assert "EMA_2_months" in written.columns
    assert "screening_rule" in written.columns


@given(
    indicator_type=st.sampled_from(["sma", "ema", "wma"]),
    operator=st.sampled_from([">", "<", ">=", "<="]),
    right_operand=st.sampled_from(["open", "close", "high", "low"]),
)
@settings(max_examples=20)
def test_property_cli_indicator_pass_through(
    indicator_type: str,
    operator: str,
    right_operand: str,
) -> None:
    captured: dict[str, AnalysisConfig] = {}
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = Path(temp_dir) / "input.csv"
        write_csv(csv_path)

        def fake_execute_analysis(source, *, config, output_path, refresh_requested):
            captured["config"] = config
            captured["output_path"] = output_path
            captured["refresh_requested"] = refresh_requested

        with patch("finance_cli.cli.execute_analysis", fake_execute_analysis):
            exit_code = main(
                [
                    "run",
                    "--file",
                    str(csv_path),
                    "--months",
                    "3",
                    "--indicator",
                    indicator_type,
                    "--rule",
                    f"indicator {operator} {right_operand}",
                ]
            )

        assert exit_code == 0
        assert captured["config"] == AnalysisConfig(
            months=3,
            indicator_type=indicator_type,
            rule=f"indicator {operator} {right_operand}",
        )
        assert captured["refresh_requested"] is False


def test_run_command_rejects_excel_custom_file(tmp_path: Path, capsys) -> None:
    workbook_path = tmp_path / "legacy.xlsx"
    workbook_path.write_text("placeholder", encoding="utf-8")

    code = main(["run", "--file", str(workbook_path), "--months", "2"])
    error = capsys.readouterr().err

    assert code == 1
    assert "Unsupported input format '.xlsx'" in error


def test_parser_rejects_removed_sheet_flag_for_run() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--file", "sample.csv", "--sheet", "Sheet1", "--months", "2"])


def test_parser_accepts_indicator_and_rule_flags() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "run",
            "--file",
            "sample.csv",
            "--months",
            "2",
            "--indicator",
            "wma",
            "--rule",
            "indicator >= close",
        ]
    )

    assert args.indicator == "wma"
    assert args.rule == "indicator >= close"


def test_parser_accepts_matrix_output_dir_and_dispatches() -> None:
    parser = build_parser()
    args = parser.parse_args(["matrix", "--output-dir", "tmp/matrix"])

    assert args.output_dir == "tmp/matrix"

    with patch("finance_cli.cli.handle_matrix_command") as handler:
        exit_code = dispatch_command(args)

    assert exit_code == 0
    handler.assert_called_once_with(args)


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


def test_parser_rejects_removed_sheet_flag_for_add() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["datasets", "add", "--path", "sample.csv", "--sheet", "Sheet1"])


def test_parser_rejects_removed_id_flag_for_add() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["datasets", "add", "--id", "sample", "--path", "sample.csv"])


def test_datasets_add_imports_into_generated_folder_using_source_filename(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "sample.csv"
    write_csv(source)

    code = main(["datasets", "add", "--path", str(source)])
    output = capsys.readouterr().out

    assert code == 0
    assert "Added dataset 'sample' -> data/generated/sample.csv" in output
    assert (tmp_path / "data" / "generated" / "sample.csv").exists()


def test_datasets_add_with_refresh_symbol_writes_generated_dataset_with_symbol_first(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "nvda.csv"
    write_csv(source)

    code = main(
        [
            "datasets",
            "add",
            "--path",
            str(source),
            "--refresh-symbol",
            "nvda",
        ]
    )
    capsys.readouterr()

    assert code == 0
    imported = pd.read_csv(tmp_path / "data" / "generated" / "nvda.csv")
    assert list(imported.columns)[:3] == ["symbol", "date", "open"]
    assert imported["symbol"].tolist() == ["NVDA", "NVDA", "NVDA"]


def test_datasets_add_fails_on_generated_filename_collision(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "sample.csv"
    write_csv(source)
    write_csv(tmp_path / "data" / "generated" / "sample.csv")

    code = main(["datasets", "add", "--path", str(source)])
    error = capsys.readouterr().err

    assert code == 1
    assert "Rename 'sample.csv'" in error


def test_datasets_create_command_writes_into_generated_folder(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "finance_cli.create.fetch_full_history_monthly_source",
        lambda symbol: monthly_frame(),
    )

    code = main(["datasets", "create", "--symbol", "SPY"])
    output = capsys.readouterr().out

    assert code == 0
    assert "Created dataset 'spy' from symbol SPY -> data/generated/spy.csv" in output
    assert (tmp_path / "data" / "generated" / "spy.csv").exists()


def test_datasets_remove_deletes_backing_generated_file(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "generated" / "sample.csv")

    code = main(["datasets", "remove", "--id", "sample"])
    output = capsys.readouterr().out

    assert code == 0
    assert "Removed dataset 'sample'" in output
    assert not (tmp_path / "data" / "generated" / "sample.csv").exists()


def test_datasets_refresh_single_command(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "generated" / "live.csv", symbol="SPY")

    def fake_refresh(source):
        assert source.dataset is not None
        assert source.dataset.id == "live"
        return RefreshSummary(
            symbol="SPY",
            row_count=123,
            min_date="2010-01-01",
            max_date="2026-03-01",
            backup_path="tmp/refresh_backups/live.backup.csv",
        )

    monkeypatch.setattr("finance_cli.cli.refresh_selected_source", fake_refresh)

    code = main(["datasets", "refresh", "--id", "live"])
    output = capsys.readouterr().out

    assert code == 0
    assert "Refreshed dataset 'live'" in output
    assert "symbol=SPY" in output


def test_datasets_refresh_all_uses_only_generated_symbol_backed_datasets(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "generated" / "500_pa.csv", symbol="500.PA")
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA")
    write_csv(tmp_path / "data" / "generated" / "sample.csv")
    write_csv(tmp_path / "data" / "live" / "default.csv", symbol="500.PA")
    refreshed_ids: list[str] = []

    def fake_refresh(source):
        assert source.dataset is not None
        refreshed_ids.append(source.dataset.id)
        return RefreshSummary(
            symbol=source.dataset.symbol or "UNKNOWN",
            row_count=10,
            min_date="2010-01-01",
            max_date="2026-03-01",
            backup_path=f"tmp/refresh_backups/{source.dataset.id}.backup.csv",
        )

    monkeypatch.setattr("finance_cli.cli.refresh_selected_source", fake_refresh)

    code = main(["datasets", "refresh", "--all"])
    output = capsys.readouterr().out

    assert code == 0
    assert refreshed_ids == ["500_pa", "nvda"]
    assert "sample" not in output
    assert "default" not in output


def test_datasets_refresh_all_errors_when_no_generated_dataset_is_refreshable(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "generated" / "sample.csv")

    code = main(["datasets", "refresh", "--all"])
    error = capsys.readouterr().err

    assert code == 1
    assert "No generated datasets support live refresh." in error


def test_wizard_menu_order_shows_actions_before_generated_datasets(tmp_path: Path) -> None:
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA")
    write_csv(tmp_path / "data" / "generated" / "500_pa.csv", symbol="500.PA")
    write_csv(tmp_path / "data" / "live" / "default.csv", symbol="500.PA")

    items = build_wizard_menu_items(discover_datasets(tmp_path))

    assert [item.alias for item in items] == ["create", "custom", "500_pa", "nvda"]
    assert items[0].label == "create - Create a new dataset from a Yahoo symbol"
    assert items[2].label == "500_pa [refresh available]"


def test_wizard_run_uses_indicator_and_rule_prompts(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA")
    responses = iter(["nvda", "2", "ema", "indicator < close", "n", ""])
    monkeypatch.setattr("builtins.input", lambda _: next(responses))

    code = main([])
    output = capsys.readouterr().out

    assert code == 0
    assert "Indicator: EMA (window=2)" in output
    assert "Rule: indicator < close" in output
    written = pd.read_csv(tmp_path / "output" / "nvda_processed.csv")
    assert "EMA_2_months" in written.columns
    assert "screening_rule" in written.columns


def test_wizard_default_indicator_and_rule_preserve_legacy_output(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA")
    responses = iter(["nvda", "2", "", "", "n", ""])
    monkeypatch.setattr("builtins.input", lambda _: next(responses))

    code = main([])
    output = capsys.readouterr().out

    assert code == 0
    assert "Indicator: SMA (window=2)" in output
    assert "Rule: indicator > open" in output
    written = pd.read_csv(tmp_path / "output" / "nvda_processed.csv")
    assert "Moving_Average" in written.columns
    assert "screening_rule" not in written.columns


def test_matrix_command_writes_all_outputs_and_manifest(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA", row_count=30)
    output_dir = tmp_path / "matrix-output"

    code = main(["matrix", "--output-dir", str(output_dir)])
    output = capsys.readouterr().out

    assert code == 0
    assert "Matrix run starting:" in output
    assert "Matrix run complete:" in output

    manifest_path = output_dir / "manifest.csv"
    assert manifest_path.exists()
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
    assert len(manifest) == 240
    assert manifest["status"].eq("success").all()
    assert manifest["row_count"].eq(30).all()
    assert manifest["condition_true_count"].notna().all()

    sample_output = Path(manifest.iloc[0]["output_path"])
    assert sample_output.exists()
    written = pd.read_csv(sample_output)
    assert "Moving_Average" not in written.columns
    assert "screening_rule" in written.columns
    assert any(column.endswith("_months") and column != "moving_average_window_months" for column in written.columns)


def test_matrix_command_records_prepare_failures_per_affected_month(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA", row_count=12)
    output_dir = tmp_path / "matrix-output"

    code = main(["matrix", "--output-dir", str(output_dir)])

    assert code == 0
    manifest = pd.read_csv(output_dir / "manifest.csv")
    failing_rows = manifest[manifest["status"] == "error"]
    success_rows = manifest[manifest["status"] == "success"]

    assert len(manifest) == 240
    assert len(failing_rows) == 48
    assert failing_rows["months"].eq(24).all()
    assert failing_rows["error"].str.contains("Months must be between 1 and 12").all()
    assert len(success_rows) == 192


def test_matrix_command_records_analysis_failure_and_continues(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    write_csv(tmp_path / "data" / "generated" / "nvda.csv", symbol="NVDA", row_count=30)
    output_dir = tmp_path / "matrix-output"

    from finance_cli.analysis import analyze_dataframe_with_config as real_analyze_dataframe_with_config

    def fake_analyze(dataframe: pd.DataFrame, config: AnalysisConfig) -> pd.DataFrame:
        if config.months == 12 and config.indicator_type == "ema" and config.rule == "indicator <= close":
            raise AnalysisError("forced matrix analysis failure")
        return real_analyze_dataframe_with_config(dataframe, config)

    monkeypatch.setattr("finance_cli.cli.analyze_dataframe_with_config", fake_analyze)

    code = main(["matrix", "--output-dir", str(output_dir)])

    assert code == 0
    manifest = pd.read_csv(output_dir / "manifest.csv")
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

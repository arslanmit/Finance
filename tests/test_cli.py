from pathlib import Path

import pandas as pd
import pytest

from finance_cli.catalog import discover_datasets
from finance_cli.cli import build_parser, build_wizard_menu_items, main
from finance_cli.models import RefreshSummary


def write_csv(path: Path, *, symbol: str | None = None) -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10, 12, 11],
            "high": [11, 13, 12],
            "low": [9, 11, 10],
            "close": [10.5, 12.5, 11.5],
            "volume": [100, 110, 120],
        }
    )
    if symbol is not None:
        dataframe.insert(0, "symbol", symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path, index=False)


def monthly_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
            "open": [10.0, 12.0, 11.0],
            "high": [11.0, 13.0, 12.0],
            "low": [9.0, 11.0, 10.0],
            "close": [10.5, 12.5, 11.5],
            "volume": [100, 110, 120],
        }
    )


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


def test_run_command_with_custom_file(tmp_path: Path, capsys) -> None:
    csv_path = tmp_path / "unmanaged.csv"
    output_path = tmp_path / "custom_output.csv"
    write_csv(csv_path)

    code = main(["run", "--file", str(csv_path), "--months", "2", "--output", str(output_path)])
    output = capsys.readouterr().out

    assert code == 0
    assert "Processed data saved to:" in output
    assert output_path.exists()


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

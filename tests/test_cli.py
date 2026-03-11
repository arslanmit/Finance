import json
from pathlib import Path

import pandas as pd
import pytest

from finance_cli.cli import build_parser, main
from finance_cli.models import DatasetConfig, RefreshMetadata, RefreshSummary
from finance_cli.registry import CONFIG_ENV_VAR, load_registry


def write_csv(path: Path, include_symbol: bool = False) -> None:
    dataframe = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10, 12, 11],
        }
    )
    if include_symbol:
        dataframe.insert(0, "symbol", "SPY")
    dataframe.to_csv(path, index=False)


def write_registry(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text(json.dumps({"datasets": records}, indent=2) + "\n", encoding="utf-8")


def test_datasets_list_command(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    write_registry(
        config_path,
        [
            {
                "id": "default",
                "label": "Sample CSV",
                "path": str(csv_path),
                "refresh": None,
            }
        ],
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    code = main(["datasets", "list"])
    output = capsys.readouterr().out

    assert code == 0
    assert "default" in output


def test_run_command_with_dataset(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    write_registry(
        config_path,
        [
            {
                "id": "default",
                "label": "Sample CSV",
                "path": "sample.csv",
                "refresh": None,
            }
        ],
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    code = main(["run", "--dataset", "default", "--months", "2"])
    output = capsys.readouterr().out

    assert code == 0
    assert "Processed data saved to:" in output
    assert (tmp_path / "output" / "sample_processed.csv").exists()


def test_run_command_with_refreshable_csv_dataset_writes_symbol_first(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "live.csv"
    write_csv(csv_path)
    write_registry(
        config_path,
        [
            {
                "id": "live",
                "label": "Live CSV",
                "path": "live.csv",
                "refresh": {"provider": "yahoo", "symbol": "NVDA"},
            }
        ],
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    code = main(["run", "--dataset", "live", "--months", "2"])
    output = capsys.readouterr().out

    assert code == 0
    assert "Processed data saved to:" in output
    assert "NVDA" in output

    processed = pd.read_csv(tmp_path / "output" / "live_processed.csv")
    assert list(processed.columns)[:3] == ["symbol", "date", "open"]
    assert processed.iloc[0]["symbol"] == "NVDA"


def test_run_command_with_custom_file(tmp_path: Path, monkeypatch, capsys) -> None:
    csv_path = tmp_path / "sample.csv"
    output_path = tmp_path / "custom_output.csv"
    write_csv(csv_path)
    monkeypatch.delenv(CONFIG_ENV_VAR, raising=False)

    code = main(["run", "--file", str(csv_path), "--months", "2", "--output", str(output_path)])
    output = capsys.readouterr().out

    assert code == 0
    assert "Processed data saved to:" in output
    assert output_path.exists()


def test_run_command_rejects_excel_custom_file(tmp_path: Path, monkeypatch, capsys) -> None:
    workbook_path = tmp_path / "legacy.xlsx"
    workbook_path.write_text("placeholder", encoding="utf-8")
    monkeypatch.delenv(CONFIG_ENV_VAR, raising=False)

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
        parser.parse_args(
            ["datasets", "add", "--id", "sample", "--label", "Sample", "--path", "sample.csv", "--sheet", "Sheet1"]
        )


def test_datasets_add_and_remove_commands(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    write_registry(config_path, [{"id": "default", "label": "Default", "path": str(csv_path), "refresh": None}])
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    add_code = main(
        [
            "datasets",
            "add",
            "--id",
            "extra",
            "--label",
            "Extra CSV",
            "--path",
            str(csv_path),
        ]
    )
    add_output = capsys.readouterr().out

    assert add_code == 0
    assert "Added dataset 'extra'" in add_output
    assert any(dataset.id == "extra" for dataset in load_registry(config_path))

    remove_code = main(["datasets", "remove", "--id", "extra"])
    remove_output = capsys.readouterr().out

    assert remove_code == 0
    assert "Removed dataset 'extra'" in remove_output
    assert all(dataset.id != "extra" for dataset in load_registry(config_path))


def test_datasets_create_command(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "datasets.json"
    write_registry(config_path, [])
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    def fake_create(datasets, symbol, config_path=None):
        csv_path = tmp_path / "data" / "generated" / "spy.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        write_csv(csv_path, include_symbol=True)

        dataset = DatasetConfig(
            id="spy",
            label="Yahoo symbol SPY",
            path="data/generated/spy.csv",
            refresh=RefreshMetadata(provider="yahoo", symbol="SPY"),
            base_dir=tmp_path,
        )
        datasets.append(dataset)

        from finance_cli.registry import save_registry

        save_registry(datasets, config_path=config_path)
        return dataset

    monkeypatch.setattr("finance_cli.cli.create_and_register_symbol_dataset", fake_create)

    code = main(["datasets", "create", "--symbol", "SPY"])
    output = capsys.readouterr().out

    assert code == 0
    assert "Created dataset 'spy' from symbol SPY" in output
    assert any(dataset.id == "spy" for dataset in load_registry(config_path))


def test_datasets_refresh_single_command(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "live.csv"
    write_csv(csv_path)
    write_registry(
        config_path,
        [
            {
                "id": "live",
                "label": "Live CSV",
                "path": "live.csv",
                "refresh": {"provider": "yahoo", "symbol": "SPY"},
            }
        ],
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

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


def test_datasets_refresh_all_command_skips_non_refreshable_entries(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    config_path = tmp_path / "datasets.json"
    first_csv = tmp_path / "first.csv"
    second_csv = tmp_path / "second.csv"
    sample_csv = tmp_path / "sample.csv"
    write_csv(first_csv)
    write_csv(second_csv)
    write_csv(sample_csv)
    write_registry(
        config_path,
        [
            {
                "id": "first",
                "label": "First Live CSV",
                "path": "first.csv",
                "refresh": {"provider": "yahoo", "symbol": "SPY"},
            },
            {
                "id": "sample",
                "label": "Sample CSV",
                "path": "sample.csv",
                "refresh": None,
            },
            {
                "id": "second",
                "label": "Second Live CSV",
                "path": "second.csv",
                "refresh": {"provider": "yahoo", "symbol": "NVDA"},
            },
        ],
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))
    refreshed_ids: list[str] = []

    def fake_refresh(source):
        assert source.dataset is not None
        refreshed_ids.append(source.dataset.id)
        return RefreshSummary(
            symbol=source.dataset.refresh.symbol,
            row_count=200,
            min_date="2010-01-01",
            max_date="2026-03-01",
            backup_path=f"tmp/refresh_backups/{source.dataset.id}.backup.csv",
        )

    monkeypatch.setattr("finance_cli.cli.refresh_selected_source", fake_refresh)

    code = main(["datasets", "refresh", "--all"])
    output = capsys.readouterr().out

    assert code == 0
    assert refreshed_ids == ["first", "second"]
    assert "Refreshed dataset 'first'" in output
    assert "Refreshed dataset 'second'" in output
    assert "sample" not in output


def test_datasets_refresh_all_command_requires_live_datasets(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    write_registry(
        config_path,
        [
            {
                "id": "sample",
                "label": "Sample CSV",
                "path": "sample.csv",
                "refresh": None,
            }
        ],
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    code = main(["datasets", "refresh", "--all"])
    error = capsys.readouterr().err

    assert code == 1
    assert "No registered datasets support live refresh" in error


def test_wizard_happy_path(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    write_registry(
        config_path,
        [
            {
                "id": "default",
                "label": "Sample CSV",
                "path": "sample.csv",
                "refresh": {"provider": "yahoo", "symbol": "500.PA"},
            }
        ],
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))
    responses = iter(["1", "2", "n", ""])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(responses))

    code = main([])
    output = capsys.readouterr().out

    assert code == 0
    assert "Processed data saved to:" in output
    assert (tmp_path / "output" / "sample_processed.csv").exists()


def test_wizard_create_symbol_and_run(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "datasets.json"
    write_registry(config_path, [])
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    def fake_create(datasets, symbol, config_path=None):
        csv_path = tmp_path / "data" / "generated" / "spy.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        write_csv(csv_path, include_symbol=True)

        dataset = DatasetConfig(
            id="spy",
            label="Yahoo symbol SPY",
            path="data/generated/spy.csv",
            refresh=RefreshMetadata(provider="yahoo", symbol="SPY"),
            base_dir=tmp_path,
        )
        datasets.append(dataset)

        from finance_cli.registry import save_registry

        save_registry(datasets, config_path=config_path)
        return dataset

    monkeypatch.setattr("finance_cli.cli.create_and_register_symbol_dataset", fake_create)
    responses = iter(["2", "SPY", "2", ""])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(responses))

    code = main([])
    output = capsys.readouterr().out

    assert code == 0
    assert "Created dataset 'spy' from symbol SPY" in output
    assert (tmp_path / "output" / "spy_processed.csv").exists()


def test_run_refresh_rejects_unsupported_dataset(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    write_registry(
        config_path,
        [
            {
                "id": "default",
                "label": "Sample CSV",
                "path": str(csv_path),
                "refresh": None,
            }
        ],
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    code = main(["run", "--dataset", "default", "--months", "2", "--refresh"])
    error = capsys.readouterr().err

    assert code == 1
    assert "does not support live refresh" in error


def test_run_refresh_rejects_unsupported_provider(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    write_registry(
        config_path,
        [
            {
                "id": "default",
                "label": "Sample CSV",
                "path": str(csv_path),
                "refresh": {"provider": "custom", "symbol": "500.PA"},
            }
        ],
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    code = main(["run", "--dataset", "default", "--months", "2", "--refresh"])
    error = capsys.readouterr().err

    assert code == 1
    assert "unsupported refresh provider" in error

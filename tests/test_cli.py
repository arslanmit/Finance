from pathlib import Path

import pandas as pd

from finance_cli.cli import main
from finance_cli.registry import CONFIG_ENV_VAR, load_registry


def write_csv(path: Path) -> None:
    pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "open": [10, 12, 11],
        }
    ).to_csv(path, index=False)


def write_workbook(path: Path) -> None:
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-02-01", "2024-03-01"],
                "open": [10, 12, 11],
            }
        ).to_excel(writer, sheet_name="Sheet1", index=False)


def write_registry(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text(
        __import__("json").dumps({"datasets": records}, indent=2) + "\n",
        encoding="utf-8",
    )


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
                "sheet": None,
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
                "sheet": None,
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


def test_datasets_add_and_remove_commands(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "datasets.json"
    csv_path = tmp_path / "sample.csv"
    write_csv(csv_path)
    write_registry(config_path, [{"id": "default", "label": "Default", "path": str(csv_path), "sheet": None, "refresh": None}])
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


def test_wizard_happy_path(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "datasets.json"
    workbook_path = tmp_path / "sample.xlsx"
    write_workbook(workbook_path)
    write_registry(
        config_path,
        [
            {
                "id": "default",
                "label": "Sample Workbook",
                "path": "sample.xlsx",
                "sheet": "Sheet1",
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
    assert (tmp_path / "output" / "sample_processed.xlsx").exists()


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
                "sheet": None,
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
    workbook_path = tmp_path / "sample.xlsx"
    write_workbook(workbook_path)
    write_registry(
        config_path,
        [
            {
                "id": "default",
                "label": "Sample Workbook",
                "path": str(workbook_path),
                "sheet": "Sheet1",
                "refresh": {"provider": "custom", "symbol": "500.PA"},
            }
        ],
    )
    monkeypatch.setenv(CONFIG_ENV_VAR, str(config_path))

    code = main(["run", "--dataset", "default", "--months", "2", "--refresh"])
    error = capsys.readouterr().err

    assert code == 1
    assert "unsupported refresh provider" in error

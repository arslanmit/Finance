from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IGNORED_OS_JUNK_PREFIXES = (".worktrees/",)


def test_repo_has_no_excel_files() -> None:
    excel_files = [
        path.relative_to(ROOT).as_posix()
        for path in ROOT.rglob("*")
        if path.is_file() and path.suffix.lower() in {".xlsx", ".xls"}
    ]

    assert excel_files == []


def test_repo_has_no_os_junk_files() -> None:
    junk_files = [
        path.relative_to(ROOT).as_posix()
        for path in ROOT.rglob("*")
        if path.is_file()
        and path.name == ".DS_Store"
        and not path.relative_to(ROOT).as_posix().startswith(IGNORED_OS_JUNK_PREFIXES)
    ]

    assert junk_files == []


def test_repo_has_no_registry_artifacts_or_excel_dependencies() -> None:
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    searchable_files = [
        ROOT / "README.md",
        *ROOT.glob("finance_cli/**/*.py"),
        *[path for path in ROOT.glob("tests/**/*.py") if path.name != "test_repo_csv_only.py"],
    ]

    assert not (ROOT / "datasets.json").exists()
    assert not (ROOT / "finance_cli" / "registry.py").exists()

    for dependency in ("openpyxl", "xlrd", "xlwt"):
        assert dependency not in requirements

    for text in ("datasets.json", "FINANCE_CLI_DATASETS_CONFIG"):
        assert all(text not in path.read_text(encoding="utf-8") for path in searchable_files)


def test_repo_uses_generated_only_named_dataset_flow() -> None:
    searchable_files = [
        ROOT / "README.md",
        *ROOT.glob("finance_cli/**/*.py"),
        *[path for path in ROOT.glob("tests/**/*.py") if path.name != "test_repo_csv_only.py"],
    ]

    assert (ROOT / "data" / "generated" / "500_pa.csv").exists()
    assert (ROOT / "output" / "500_pa_processed.csv").exists()
    assert not (ROOT / "output" / "default_processed.csv").exists()

    for text in ("data/live/default.csv", "source=live", "source=generated", "refresh_default_dataset"):
        assert all(text not in path.read_text(encoding="utf-8") for path in searchable_files)

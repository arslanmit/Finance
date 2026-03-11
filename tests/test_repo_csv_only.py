import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_repo_has_no_excel_files() -> None:
    excel_files = [
        path.relative_to(ROOT).as_posix()
        for path in ROOT.rglob("*")
        if path.is_file() and path.suffix.lower() in {".xlsx", ".xls"}
    ]

    assert excel_files == []


def test_registry_and_requirements_are_csv_only() -> None:
    registry = json.loads((ROOT / "datasets.json").read_text(encoding="utf-8"))
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")

    for entry in registry["datasets"]:
        assert "sheet" not in entry
        assert entry["path"].endswith(".csv")

    for dependency in ("openpyxl", "xlrd", "xlwt"):
        assert dependency not in requirements

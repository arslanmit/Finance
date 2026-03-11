"""Dataset registry loading, validation, and mutation."""

from __future__ import annotations

import json
import os
from pathlib import Path

from .errors import RegistryError
from .models import DatasetConfig, RefreshMetadata
from .sources import SUPPORTED_FILE_SUFFIXES

DEFAULT_CONFIG_PATH = Path("datasets.json")
CONFIG_ENV_VAR = "FINANCE_CLI_DATASETS_CONFIG"


def get_config_path(config_path: Path | None = None) -> Path:
    if config_path is not None:
        return Path(config_path)

    env_path = os.getenv(CONFIG_ENV_VAR)
    if env_path:
        return Path(env_path)

    return DEFAULT_CONFIG_PATH


def load_registry(config_path: Path | None = None) -> list[DatasetConfig]:
    resolved_config = get_config_path(config_path)
    if not resolved_config.exists():
        raise RegistryError(f"Dataset config file was not found: {resolved_config}")

    try:
        payload = json.loads(resolved_config.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RegistryError(f"Dataset config file is not valid JSON: {exc}") from exc

    entries = payload.get("datasets") if isinstance(payload, dict) else payload
    if not isinstance(entries, list):
        raise RegistryError("datasets.json must contain a top-level 'datasets' list.")

    datasets = [parse_dataset_entry(entry, resolved_config.parent) for entry in entries]
    dataset_ids = [dataset.id for dataset in datasets]
    if len(dataset_ids) != len(set(dataset_ids)):
        raise RegistryError("datasets.json contains duplicate dataset ids.")

    return datasets


def save_registry(datasets: list[DatasetConfig], config_path: Path | None = None) -> None:
    resolved_config = get_config_path(config_path)
    payload = {"datasets": [dataset.to_record() for dataset in datasets]}
    resolved_config.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_dataset_entry(entry: object, base_dir: Path) -> DatasetConfig:
    if not isinstance(entry, dict):
        raise RegistryError("Each dataset entry in datasets.json must be an object.")

    dataset_id = str(entry.get("id", "")).strip()
    label = str(entry.get("label", "")).strip()
    path = str(entry.get("path", "")).strip()
    if not dataset_id or not label or not path:
        raise RegistryError(
            "Each dataset entry must define non-empty 'id', 'label', and 'path' values."
        )

    refresh = parse_refresh_metadata(entry.get("refresh"), dataset_id)

    return DatasetConfig(
        id=dataset_id,
        label=label,
        path=path,
        refresh=refresh,
        base_dir=base_dir,
    )


def parse_refresh_metadata(refresh_entry: object, dataset_id: str) -> RefreshMetadata | None:
    if refresh_entry is None:
        return None
    if not isinstance(refresh_entry, dict):
        raise RegistryError(f"Dataset '{dataset_id}' has an invalid refresh object.")

    provider = str(refresh_entry.get("provider", "")).strip()
    symbol = str(refresh_entry.get("symbol", "")).strip()
    if not provider or not symbol:
        raise RegistryError(
            f"Dataset '{dataset_id}' refresh metadata must define provider and symbol."
        )

    return RefreshMetadata(provider=provider, symbol=symbol)


def get_dataset(datasets: list[DatasetConfig], dataset_id: str) -> DatasetConfig:
    for dataset in datasets:
        if dataset.id == dataset_id:
            return dataset
    available = ", ".join(dataset.id for dataset in datasets)
    raise RegistryError(f"Unknown dataset '{dataset_id}'. Available datasets: {available}")


def add_dataset(
    datasets: list[DatasetConfig],
    *,
    dataset_id: str,
    label: str,
    path: str,
    refresh_symbol: str | None = None,
    config_path: Path | None = None,
) -> DatasetConfig:
    resolved_config = get_config_path(config_path)
    base_dir = resolved_config.parent

    dataset_id = dataset_id.strip()
    label = label.strip()
    path = path.strip()
    if not dataset_id or not label or not path:
        raise RegistryError("Dataset id, label, and path are required.")
    if any(dataset.id == dataset_id for dataset in datasets):
        raise RegistryError(f"Dataset '{dataset_id}' already exists.")

    resolved_path = (base_dir / path).resolve(strict=False)
    if not resolved_path.exists():
        raise RegistryError(f"Dataset path was not found: {resolved_path}")

    suffix = resolved_path.suffix.lower()
    if suffix not in SUPPORTED_FILE_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_FILE_SUFFIXES))
        raise RegistryError(
            f"Unsupported dataset format '{suffix}'. Supported formats: {supported}."
        )

    refresh = resolve_registry_refresh(
        suffix=suffix,
        refresh_symbol=refresh_symbol,
        dataset_id=dataset_id,
    )

    dataset = DatasetConfig(
        id=dataset_id,
        label=label,
        path=path,
        refresh=refresh,
        base_dir=base_dir,
    )
    datasets.append(dataset)
    return dataset


def resolve_registry_refresh(
    *,
    suffix: str,
    refresh_symbol: str | None,
    dataset_id: str,
) -> RefreshMetadata | None:
    if refresh_symbol is None:
        return None
    if suffix != ".csv":
        raise RegistryError(
            f"Dataset '{dataset_id}' can use --refresh-symbol only with .csv files."
        )
    return RefreshMetadata(provider="yahoo", symbol=refresh_symbol.strip())


def remove_dataset(datasets: list[DatasetConfig], dataset_id: str) -> DatasetConfig:
    for index, dataset in enumerate(datasets):
        if dataset.id == dataset_id:
            return datasets.pop(index)
    raise RegistryError(f"Dataset '{dataset_id}' was not found.")

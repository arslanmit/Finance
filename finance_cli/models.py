"""Shared dataclasses for the finance CLI."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RefreshMetadata:
    """Refresh metadata for a configured dataset."""

    provider: str
    symbol: str

    def to_record(self) -> dict[str, str]:
        return {"provider": self.provider, "symbol": self.symbol}


@dataclass(frozen=True)
class DatasetConfig:
    """Configured dataset entry from datasets.json."""

    id: str
    label: str
    path: str
    refresh: RefreshMetadata | None
    base_dir: Path

    @property
    def resolved_path(self) -> Path:
        return (self.base_dir / self.path).resolve(strict=False)

    @property
    def file_name(self) -> str:
        return Path(self.path).name

    @property
    def supports_refresh(self) -> bool:
        return self.refresh is not None

    @property
    def symbol(self) -> str | None:
        return None if self.refresh is None else self.refresh.symbol

    def to_record(self) -> dict[str, object]:
        return {
            "id": self.id,
            "label": self.label,
            "path": self.path,
            "refresh": None if self.refresh is None else self.refresh.to_record(),
        }


@dataclass(frozen=True)
class ResolvedSource:
    """Resolved input source for a run."""

    input_path: Path
    dataset: DatasetConfig | None


@dataclass(frozen=True)
class RefreshSummary:
    """Summary of a successful CSV refresh."""

    symbol: str
    row_count: int
    min_date: str
    max_date: str
    backup_path: str

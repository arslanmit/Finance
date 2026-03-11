"""Shared dataclasses for the finance CLI."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar


@dataclass(frozen=True)
class RefreshMetadata:
    """Refresh metadata for a discovered dataset."""

    provider: str
    symbol: str


@dataclass(frozen=True)
class DatasetConfig:
    """Discovered dataset entry from a managed CSV directory."""

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


@dataclass(frozen=True)
class AnalysisConfig:
    """Configuration for indicator calculation and screening rules."""

    months: int
    indicator_type: str = "sma"
    rule: str = "indicator > open"

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        from finance_cli.errors import AnalysisError

        if self.months < 1:
            raise AnalysisError("Months must be at least 1")


@dataclass(frozen=True)
class ParsedRule:
    """Parsed screening rule components."""

    left_operand: str
    operator: str
    right_operand: str

    VALID_OPERATORS: ClassVar[set[str]] = {">", "<", ">=", "<="}

    def __post_init__(self) -> None:
        """Validate operator."""
        from finance_cli.errors import AnalysisError

        if self.operator not in self.VALID_OPERATORS:
            valid = ", ".join(sorted(self.VALID_OPERATORS))
            raise AnalysisError(
                f"Invalid operator '{self.operator}'. Valid operators: {valid}"
            )

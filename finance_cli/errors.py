"""User-facing error types for the finance CLI."""


class FinanceCliError(Exception):
    """Base class for expected CLI errors."""


class RegistryError(FinanceCliError):
    """Raised when dataset registry operations fail."""


class SourceError(FinanceCliError):
    """Raised when resolving or loading an input source fails."""


class AnalysisError(FinanceCliError):
    """Raised when data validation or analysis fails."""


class RefreshError(FinanceCliError):
    """Raised when live dataset refresh fails."""

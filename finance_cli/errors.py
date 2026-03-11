"""User-facing error types for the finance CLI."""


class FinanceCliError(Exception):
    """Base class for expected CLI errors."""


class CatalogError(FinanceCliError):
    """Raised when managed dataset discovery or mutation fails."""


class SourceError(FinanceCliError):
    """Raised when resolving or loading an input source fails."""


class AnalysisError(FinanceCliError):
    """Raised when data validation or analysis fails."""


class RefreshError(FinanceCliError):
    """Raised when generated-dataset refresh fails."""


class CreationError(FinanceCliError):
    """Raised when creating a new dataset from a symbol fails."""

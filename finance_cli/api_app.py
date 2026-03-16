"""Compatibility wrapper for the Finance CLI FastAPI application."""

from __future__ import annotations

from .api.app import create_app
from .api.settings import ApiSettings

app = create_app()

__all__ = ["ApiSettings", "app", "create_app"]

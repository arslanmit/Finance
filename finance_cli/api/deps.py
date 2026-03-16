"""FastAPI dependency helpers."""

from __future__ import annotations

from fastapi import Request

from .service import ApiContext


def get_api_context(request: Request) -> ApiContext:
    return request.app.state.api_context

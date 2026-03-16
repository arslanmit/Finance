"""Run routes for synchronous dataset analysis."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import FileResponse

from .deps import get_api_context
from .models import RunCreateRequest, RunResponse
from .service import ApiContext, create_run, get_run, resolve_artifact_path


router = APIRouter(prefix="/runs", tags=["runs"])


@router.post("", response_model=RunResponse, status_code=status.HTTP_201_CREATED)
def post_run(
    request: RunCreateRequest,
    context: Annotated[ApiContext, Depends(get_api_context)],
) -> RunResponse:
    return create_run(context, request)


@router.get("/{run_id}", response_model=RunResponse)
def get_run_by_id(
    run_id: str,
    context: Annotated[ApiContext, Depends(get_api_context)],
) -> RunResponse:
    return get_run(context, run_id)


@router.get("/{run_id}/artifact")
def get_run_artifact(
    run_id: str,
    context: Annotated[ApiContext, Depends(get_api_context)],
) -> FileResponse:
    artifact_path = resolve_artifact_path(context, owner_type="run", owner_id=run_id)
    return FileResponse(artifact_path, media_type="text/csv", filename=artifact_path.name)

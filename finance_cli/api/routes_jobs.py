"""Job routes for matrix and refresh execution."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import FileResponse

from .deps import get_api_context
from .models import JobResponse
from .service import ApiContext, get_job, queue_matrix_job, resolve_artifact_path


router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/matrix", response_model=JobResponse, status_code=status.HTTP_202_ACCEPTED)
def post_matrix_job(
    context: Annotated[ApiContext, Depends(get_api_context)],
) -> JobResponse:
    return queue_matrix_job(context)


@router.get("/{job_id}", response_model=JobResponse)
def get_job_by_id(
    job_id: str,
    context: Annotated[ApiContext, Depends(get_api_context)],
) -> JobResponse:
    return get_job(context, job_id)


@router.get("/{job_id}/artifact")
def get_job_artifact(
    job_id: str,
    context: Annotated[ApiContext, Depends(get_api_context)],
) -> FileResponse:
    artifact_path = resolve_artifact_path(context, owner_type="job", owner_id=job_id)
    return FileResponse(artifact_path, media_type="text/csv", filename=artifact_path.name)

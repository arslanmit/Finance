"""Dataset routes for the Finance CLI API."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, UploadFile, status

from .deps import get_api_context
from .models import CreateDatasetFromSymbolRequest, DatasetResponse, DeleteDatasetResponse, JobResponse
from .service import ApiContext, create_dataset_from_symbol, delete_dataset_entry, list_datasets, queue_refresh_job, upload_dataset


router = APIRouter(prefix="/datasets", tags=["datasets"])


@router.get("", response_model=list[DatasetResponse])
def get_datasets(context: Annotated[ApiContext, Depends(get_api_context)]) -> list[DatasetResponse]:
    return list_datasets(context)


@router.post("/upload", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
def post_dataset_upload(
    context: Annotated[ApiContext, Depends(get_api_context)],
    file: Annotated[UploadFile, File(...)],
    refresh_symbol: Annotated[str | None, Form()] = None,
) -> DatasetResponse:
    return upload_dataset(context, upload_file=file, refresh_symbol=refresh_symbol)


@router.post("/create-from-symbol", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
def post_create_from_symbol(
    request: CreateDatasetFromSymbolRequest,
    context: Annotated[ApiContext, Depends(get_api_context)],
) -> DatasetResponse:
    return create_dataset_from_symbol(context, request)


@router.post("/{dataset_id}/refresh", response_model=JobResponse, status_code=status.HTTP_202_ACCEPTED)
def post_refresh_dataset(
    dataset_id: str,
    context: Annotated[ApiContext, Depends(get_api_context)],
) -> JobResponse:
    return queue_refresh_job(context, dataset_id)


@router.delete("/{dataset_id}", response_model=DeleteDatasetResponse)
def delete_dataset(
    dataset_id: str,
    context: Annotated[ApiContext, Depends(get_api_context)],
) -> DeleteDatasetResponse:
    return delete_dataset_entry(context, dataset_id)

"""Pydantic models for the Finance CLI API."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    """Base model with strict extra-field handling."""

    model_config = ConfigDict(extra="forbid")


class RefreshMetadataResponse(ApiModel):
    provider: str
    symbol: str


class DatasetResponse(ApiModel):
    id: str
    label: str
    path: str
    source_type: str
    refresh: RefreshMetadataResponse | None
    created_at: str
    updated_at: str


class DeleteDatasetResponse(ApiModel):
    id: str


class CreateDatasetFromSymbolRequest(ApiModel):
    symbol: str = Field(min_length=1)


class RunCreateRequest(ApiModel):
    dataset_id: str = Field(min_length=1)
    months: int = Field(ge=1)
    indicator_type: str = "sma"
    rule: str = "indicator > open"
    refresh_requested: bool = False


class RunResponse(ApiModel):
    id: str
    dataset_id: str
    indicator_type: str
    months: int
    rule: str
    status: str
    output_path: str
    error: str
    created_at: str
    updated_at: str
    artifact_url: str | None


class JobResponse(ApiModel):
    id: str
    job_type: str
    dataset_id: str | None
    status: str
    output_path: str
    error: str
    created_at: str
    updated_at: str
    artifact_url: str | None

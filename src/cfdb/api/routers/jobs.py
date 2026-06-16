"""REST API router exposing workflow job status."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from cfdb import api
from cfdb.services import locks
from cfdb.workflows.lock import get_job

router = APIRouter(prefix="/jobs", tags=["jobs"])


class JobStatusResponse(BaseModel):
    """Workflow job status payload returned by ``GET /jobs/{job_id}``."""

    job_id: str = Field(..., description="Opaque job identifier.")
    status: str = Field(
        ..., description="One of pending / running / completed / failed."
    )
    stages_done: list[str] = Field(
        default_factory=list,
        description="Stage names committed so far.",
    )
    artifacts: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of artifact-kind value to cache key.",
    )
    progress: Optional[str] = Field(
        default=None, description="Human-readable progress hint, if any."
    )
    error: Optional[str] = Field(
        default=None, description="Failure reason, populated on FAILED."
    )
    superseded_by: Optional[str] = Field(
        default=None,
        description=(
            "If this job was stale-reclaimed by a later request, the "
            "successor's job_id. Clients polling a FAILED job can follow "
            "this pointer to find the live workflow."
        ),
    )


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str) -> JobStatusResponse:
    """Return the current state of a workflow job.

    Path Parameters:
        job_id: Opaque job identifier returned in the ``Location`` header
            of a 202 response from ``/data`` or ``/index``.

    Returns:
        A :class:`JobStatusResponse` carrying ``status`` plus the
        terminal-state fields (``artifacts`` on success, ``error`` on
        failure) and the in-flight ``stages_done`` / ``progress``.
    """
    await locks.wait_for_cutover()

    if api.db is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database not available",
        )

    record = await get_job(api.db, job_id)
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )

    return JobStatusResponse(
        job_id=record.job_id,
        status=record.status.value,
        stages_done=list(record.stages_done),
        artifacts=dict(record.artifact_cache_keys),
        progress=record.progress,
        error=record.error,
        superseded_by=record.superseded_by,
    )

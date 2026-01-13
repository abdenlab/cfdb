"""REST API router for sync operations."""

import logging
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel

from cfdb import api
from cfdb.services.locks import get_sync_task
from cfdb.services.sync import is_sync_running, start_sync

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sync", tags=["sync"])


async def verify_api_key(x_api_key: str = Header(..., alias="X-API-Key")):
    """Verify API key for sync endpoints."""
    if x_api_key != api.SYNC_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key


class SyncResponse(BaseModel):
    task_id: str
    status: str
    dcc_names: list[str]
    message: str


class SyncStatusResponse(BaseModel):
    task_id: str
    status: str
    dcc_names: list[str]
    started_at: str
    completed_at: str | None = None


@router.post("", response_model=SyncResponse, status_code=202)
async def sync(
    dccs: list[str] = Query(default=[]),
    _: str = Depends(verify_api_key),
):
    """
    Start a new sync task.

    The sync runs as a background task. Returns immediately with a task ID.
    Only one sync can run at a time - returns 409 if a sync is already in progress.

    Query Parameters:
        dccs: List of DCC names to sync (e.g., ?dccs=4dn&dccs=hubmap).
              If empty, all DCCs will be synced.

    Returns:
        202 Accepted with task details

    Raises:
        401: Invalid API key
        409: Sync already in progress
        500: Server configuration error
    """
    if await is_sync_running():
        raise HTTPException(
            status_code=409,
            detail="A sync task is already running. Please wait for it to complete.",
        )

    task_id = str(uuid.uuid4())

    try:
        task = await start_sync(task_id, dccs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        # Race condition - another sync started between check and start
        raise HTTPException(status_code=409, detail=str(e))

    logger.info(f"Started sync task {task_id} for DCCs: {task.dcc_names}")

    return SyncResponse(
        task_id=task.id,
        status=task.status.value,
        dcc_names=task.dcc_names,
        message=f"Sync started for {', '.join(task.dcc_names)}",
    )


@router.get("/{task_id}", response_model=SyncStatusResponse)
async def get_sync_status(task_id: str):
    """
    Get the status of a sync task.

    Path Parameters:
        task_id: The task ID returned when starting a sync.

    Returns:
        200 OK with task status

    Raises:
        404: Task not found
    """
    task = await get_sync_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Sync task {task_id} not found")

    status = "running" if task.get("active") else "completed"
    started_at = task.get("started_at")
    completed_at = task.get("completed_at")

    return SyncStatusResponse(
        task_id=task["task_id"],
        status=status,
        dcc_names=task.get("dcc_names", []),
        started_at=started_at.isoformat() if started_at else "",
        completed_at=completed_at.isoformat() if completed_at else None,
    )

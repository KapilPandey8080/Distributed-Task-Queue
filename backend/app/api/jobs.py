import logging
import uuid
from datetime import UTC, datetime
from functools import lru_cache

import redis
from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from app.core.queue import RedisQueue
from app.db import repository
from app.db.models import Job
from app.db.session import get_db
from app.schemas.job import JobCreate, JobRead

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["jobs"])


@lru_cache
def get_queue() -> RedisQueue:
    """One shared RedisQueue per process. Tests replace this with a fake."""
    return RedisQueue()


def _is_due(run_at: datetime) -> bool:
    if run_at.tzinfo is None:  # SQLite returns naive datetimes; we always store UTC
        run_at = run_at.replace(tzinfo=UTC)
    return run_at <= datetime.now(UTC)


def _publish(queue: RedisQueue, job: Job) -> None:
    """Push the job id to Redis. The job is already safe in Postgres, so a Redis failure
    must not fail the request: log it and let a later sweep re-enqueue the job."""
    try:
        queue.enqueue(job.queue, str(job.id))
    except redis.RedisError:
        logger.exception("job %s saved to Postgres but not pushed to Redis", job.id)


@router.post(
    "",
    response_model=JobRead,
    status_code=status.HTTP_201_CREATED,
    summary="Enqueue a job",
    responses={200: {"description": "Idempotency key matched an existing job"}},
)
def enqueue_job(
    body: JobCreate,
    response: Response,
    db: Session = Depends(get_db),
    queue: RedisQueue = Depends(get_queue),
):
    job, created = repository.create_job(db, body)
    if not created:
        response.status_code = status.HTTP_200_OK
    elif _is_due(job.run_at):
        # Delayed jobs (run_at in the future) stay in Postgres only; the Phase 4
        # scheduler pushes them to Redis when they become due.
        _publish(queue, job)
    return job


@router.get("/{job_id}", response_model=JobRead, summary="Get a job's status")
def get_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = repository.get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job
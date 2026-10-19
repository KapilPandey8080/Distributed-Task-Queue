import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import Job, JobStatus
from app.schemas.job import JobCreate


def get_job(db: Session, job_id: uuid.UUID) -> Job | None:
    return db.get(Job, job_id)


def get_job_by_idempotency_key(db: Session, queue: str, key: str) -> Job | None:
    stmt = select(Job).where(Job.queue == queue, Job.idempotency_key == key)
    return db.execute(stmt).scalar_one_or_none()


def create_job(db: Session, data: JobCreate) -> tuple[Job, bool]:
    """Insert a job. Returns (job, created).

    If an idempotency key is supplied and a job with the same (queue, key) already exists,
    the existing job is returned with created=False instead of inserting a duplicate.
    """
    if data.idempotency_key:
        existing = get_job_by_idempotency_key(db, data.queue, data.idempotency_key)
        if existing is not None:
            return existing, False

    now = datetime.now(UTC)
    if data.run_at is not None:
        run_at = data.run_at
    elif data.delay_seconds:
        run_at = now + timedelta(seconds=data.delay_seconds)
    else:
        run_at = now

    job = Job(
        queue=data.queue,
        task_name=data.task_name,
        payload=data.payload,
        status=JobStatus.QUEUED,
        priority=data.priority,
        attempts=0,
        max_attempts=data.max_attempts,
        run_at=run_at,
        idempotency_key=data.idempotency_key,
    )
    db.add(job)
    try:
        db.commit()
    except IntegrityError:
        # Two concurrent requests with the same key: the unique constraint is the real guard.
        db.rollback()
        if data.idempotency_key:
            existing = get_job_by_idempotency_key(db, data.queue, data.idempotency_key)
            if existing is not None:
                return existing, False
        raise
    db.refresh(job)
    return job, True

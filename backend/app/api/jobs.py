import uuid

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from app.db import repository
from app.db.session import get_db
from app.schemas.job import JobCreate, JobRead

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobRead,
    status_code=status.HTTP_201_CREATED,
    summary="Enqueue a job",
    responses={200: {"description": "Idempotency key matched an existing job"}},
)
def enqueue_job(body: JobCreate, response: Response, db: Session = Depends(get_db)):
    job, created = repository.create_job(db, body)
    if not created:
        response.status_code = status.HTTP_200_OK
    return job


@router.get("/{job_id}", response_model=JobRead, summary="Get a job's status")
def get_job(job_id: uuid.UUID, db: Session = Depends(get_db)):
    job = repository.get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job

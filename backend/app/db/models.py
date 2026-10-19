import enum
import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# JSONB on Postgres (indexable, compact); plain JSON elsewhere (e.g. SQLite in tests).
JSONType = JSON().with_variant(JSONB(), "postgresql")


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class JobStatus(enum.StrEnum):
    QUEUED = "queued"  # waiting to be picked up (run_at may be in the future)
    RUNNING = "running"  # claimed by a worker
    RETRYING = "retrying"  # last attempt failed, waiting for backoff to elapse
    SUCCEEDED = "succeeded"
    DEAD = "dead"  # exhausted max_attempts, moved to the dead-letter queue
    CANCELLED = "cancelled"


def _status_enum() -> Enum:
    # Stored as plain VARCHAR (no native PG enum) so adding a status is a code change,
    # not a painful ALTER TYPE migration.
    return Enum(
        JobStatus,
        native_enum=False,
        length=20,
        values_callable=lambda e: [m.value for m in e],
    )


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("queue", "idempotency_key", name="uq_jobs_queue_idempotency_key"),
        # Supports the Phase 2+ claim query: "next due job in this queue, highest priority first".
        Index("ix_jobs_claim", "queue", "status", "priority", "run_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    queue: Mapped[str] = mapped_column(String(100), nullable=False, default="default")
    task_name: Mapped[str] = mapped_column(String(200), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONType, nullable=False, default=dict)

    status: Mapped[JobStatus] = mapped_column(
        _status_enum(), nullable=False, default=JobStatus.QUEUED
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=5)  # 0-9, higher first
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)

    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    idempotency_key: Mapped[str | None] = mapped_column(String(200), nullable=True)

    result: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    attempt_log: Mapped[list["Attempt"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )


class Attempt(Base):
    """One row per execution try of a job. Written by workers from Phase 2/3 onward."""

    __tablename__ = "attempts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # succeeded / failed / timeout
    outcome: Mapped[str | None] = mapped_column(String(20), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped[Job] = relationship(back_populates="attempt_log")


class DeadLetter(Base):
    """Jobs that exhausted their retries. Kept for inspection and manual replay."""

    __tablename__ = "dead_letters"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    reason: Mapped[str] = mapped_column(String(50), nullable=False)  # e.g. max_attempts_exceeded
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    moved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    replayed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

import uuid
from datetime import UTC, datetime
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.db.models import JobStatus


class JobCreate(BaseModel):
    task_name: str = Field(min_length=1, max_length=200, examples=["send_email"])
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = Field(default="default", min_length=1, max_length=100)
    priority: int = Field(default=5, ge=0, le=9, description="0-9, higher runs first")
    max_attempts: int = Field(default=3, ge=1, le=20)
    run_at: datetime | None = Field(default=None, description="Run no earlier than this time")
    delay_seconds: int | None = Field(
        default=None, ge=0, le=60 * 60 * 24 * 365, description="Shortcut for run_at = now + N"
    )
    idempotency_key: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
        description="Re-submitting the same key to the same queue returns the original job",
    )

    @field_validator("run_at")
    @classmethod
    def _assume_utc(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    @model_validator(mode="after")
    def _run_at_xor_delay(self) -> Self:
        if self.run_at is not None and self.delay_seconds is not None:
            raise ValueError("provide either run_at or delay_seconds, not both")
        return self


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    queue: str
    task_name: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    attempts: int
    max_attempts: int
    run_at: datetime
    idempotency_key: str | None
    result: dict[str, Any] | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

"""initial schema: jobs, attempts, dead_letters

Revision ID: 0001
Revises:
Create Date: 2026-10-19
"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

JSONType = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("queue", sa.String(100), nullable=False),
        sa.Column("task_name", sa.String(200), nullable=False),
        sa.Column("payload", JSONType, nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("idempotency_key", sa.String(200), nullable=True),
        sa.Column("result", JSONType, nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("queue", "idempotency_key", name="uq_jobs_queue_idempotency_key"),
    )
    op.create_index("ix_jobs_claim", "jobs", ["queue", "status", "priority", "run_at"])

    op.create_table(
        "attempts",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "job_id", sa.Uuid(), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(100), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(20), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.create_index("ix_attempts_job_id", "attempts", ["job_id"])

    op.create_table(
        "dead_letters",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "job_id",
            sa.Uuid(),
            sa.ForeignKey("jobs.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("reason", sa.String(50), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("moved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("replayed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("dead_letters")
    op.drop_index("ix_attempts_job_id", table_name="attempts")
    op.drop_table("attempts")
    op.drop_index("ix_jobs_claim", table_name="jobs")
    op.drop_table("jobs")

import uuid

import pytest
from sqlalchemy import select

from app.db.models import Attempt, Job, JobStatus
from worker.runner import process_one

WORKER_ID = "test-worker"


def make_job(session_factory, fake_queue, task_name, payload=None, status=JobStatus.QUEUED, **cols):
    """Insert a job row and push its id onto the (fake) queue, like POST /jobs does."""
    with session_factory() as db:
        job = Job(
            queue="default",
            task_name=task_name,
            payload=payload or {},
            status=status,
            max_attempts=3,
            **cols,
        )
        db.add(job)
        db.commit()
        job_id = job.id
    fake_queue.enqueue("default", str(job_id))
    return job_id


def run_next(fake_queue, session_factory, timeout_seconds=10):
    return process_one(
        fake_queue,
        "default",
        WORKER_ID,
        session_factory,
        timeout_seconds=timeout_seconds,
        block_ms=0,
    )


def run_all(fake_queue, session_factory, timeout_seconds=10):
    processed = 0
    while run_next(fake_queue, session_factory, timeout_seconds):
        processed += 1
    return processed


def load(session_factory, job_id):
    """Read a job and its attempts through a brand-new session (no stale cache)."""
    with session_factory() as db:
        job = db.get(Job, job_id)
        attempts = db.scalars(select(Attempt).where(Attempt.job_id == job_id)).all()
        return job, attempts


# ---- the happy path -----------------------------------------------------------------------


def test_successful_job_is_saved_and_acked(session_factory, fake_queue):
    job_id = make_job(session_factory, fake_queue, "add", {"a": 2, "b": 3})

    assert run_all(fake_queue, session_factory) == 1

    job, attempts = load(session_factory, job_id)
    assert job.status is JobStatus.SUCCEEDED
    assert job.result == {"sum": 5}
    assert job.attempts == 1
    assert job.last_error is None
    assert job.started_at is not None and job.finished_at is not None
    assert len(attempts) == 1
    assert attempts[0].outcome == "succeeded"
    assert attempts[0].worker_id == WORKER_ID
    assert attempts[0].attempt_number == 1
    assert fake_queue.pending_count == 0


def test_returns_false_when_queue_is_empty(session_factory, fake_queue):
    assert run_next(fake_queue, session_factory) is False


def test_several_jobs_are_all_processed(session_factory, fake_queue):
    ids = [make_job(session_factory, fake_queue, "add", {"a": i, "b": i}) for i in range(3)]

    assert run_all(fake_queue, session_factory) == 3

    sums = [load(session_factory, i)[0].result["sum"] for i in ids]
    assert sums == [0, 2, 4]
    assert fake_queue.pending_count == 0


# ---- failures -----------------------------------------------------------------------------


def test_task_exception_marks_job_dead_and_records_the_error(session_factory, fake_queue):
    job_id = make_job(session_factory, fake_queue, "send_email", {"to": "not-an-email"})

    run_all(fake_queue, session_factory)

    job, attempts = load(session_factory, job_id)
    assert job.status is JobStatus.DEAD
    assert "ValueError: invalid email address" in job.last_error
    assert job.result is None
    assert attempts[0].outcome == "failed"
    assert "ValueError" in attempts[0].error
    assert fake_queue.pending_count == 0  # a failed job is still acked: its outcome is saved


def test_timeout_marks_job_dead_with_timeout_outcome(session_factory, fake_queue):
    job_id = make_job(session_factory, fake_queue, "sleep", {"seconds": 30})

    run_all(fake_queue, session_factory, timeout_seconds=1)

    job, attempts = load(session_factory, job_id)
    assert job.status is JobStatus.DEAD
    assert "timed out" in job.last_error
    assert attempts[0].outcome == "timeout"


def test_unknown_task_marks_job_dead(session_factory, fake_queue):
    job_id = make_job(session_factory, fake_queue, "no_such_task")

    run_all(fake_queue, session_factory)

    job, _ = load(session_factory, job_id)
    assert job.status is JobStatus.DEAD
    assert job.last_error == "unknown task: 'no_such_task'"


# ---- messages the worker must not run -----------------------------------------------------


def test_cancelled_job_is_skipped_but_acked(session_factory, fake_queue):
    job_id = make_job(
        session_factory, fake_queue, "add", {"a": 1, "b": 1}, status=JobStatus.CANCELLED
    )

    run_all(fake_queue, session_factory)

    job, attempts = load(session_factory, job_id)
    assert job.status is JobStatus.CANCELLED
    assert job.attempts == 0
    assert attempts == []
    assert fake_queue.pending_count == 0


def test_duplicate_delivery_of_finished_job_does_not_run_it_again(session_factory, fake_queue):
    job_id = make_job(
        session_factory,
        fake_queue,
        "add",
        {"a": 1, "b": 1},
        status=JobStatus.SUCCEEDED,
        attempts=1,
        result={"sum": 2},
    )

    run_all(fake_queue, session_factory)

    job, attempts = load(session_factory, job_id)
    assert job.attempts == 1  # unchanged: it did not run a second time
    assert attempts == []
    assert fake_queue.pending_count == 0


def test_message_for_missing_job_is_dropped_without_crashing(session_factory, fake_queue):
    fake_queue.enqueue("default", str(uuid.uuid4()))

    assert run_all(fake_queue, session_factory) == 1
    assert fake_queue.pending_count == 0


# ---- delivery guarantees ------------------------------------------------------------------


def test_message_is_acked_only_after_the_result_is_saved(session_factory, fake_queue):
    make_job(session_factory, fake_queue, "add", {"a": 1, "b": 2})
    statuses_at_ack = []
    real_ack = fake_queue.ack

    def spying_ack(delivery):
        with session_factory() as db:
            statuses_at_ack.append(db.get(Job, uuid.UUID(delivery.job_id)).status)
        real_ack(delivery)

    fake_queue.ack = spying_ack

    run_all(fake_queue, session_factory)

    assert statuses_at_ack == [JobStatus.SUCCEEDED]


def test_unexpected_error_leaves_message_unacked_so_it_is_not_lost(
    session_factory, fake_queue, monkeypatch
):
    job_id = make_job(session_factory, fake_queue, "add", {"a": 1, "b": 2})

    def crash(*args, **kwargs):
        raise RuntimeError("worker blew up mid-job")

    monkeypatch.setattr("worker.runner.execute", crash)

    with pytest.raises(RuntimeError):
        run_next(fake_queue, session_factory)

    # The message was read but never acked, so Redis would still hold it as "pending"
    # (the Phase 3 reaper recovers these), and the job is visibly stuck in RUNNING.
    assert fake_queue.pending_count == 1
    job, _ = load(session_factory, job_id)
    assert job.status is JobStatus.RUNNING


# ---- through the HTTP API -----------------------------------------------------------------


def test_job_enqueued_via_api_is_run_by_the_worker(client, db_session, session_factory, fake_queue):
    created = client.post("/jobs", json={"task_name": "add", "payload": {"a": 20, "b": 22}})
    job_id = created.json()["id"]
    assert created.json()["status"] == "queued"

    run_all(fake_queue, session_factory)

    # The test's API session cached the job when it was created; real requests get a fresh
    # session each time, so mimic that here.
    db_session.expire_all()
    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "succeeded"
    assert body["result"] == {"sum": 42}
    assert body["attempts"] == 1
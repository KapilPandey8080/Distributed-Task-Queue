import logging
import os
import signal
import socket
import threading
import uuid

import redis
from sqlalchemy.orm import Session, sessionmaker

import worker.tasks  # noqa: F401  (registers tasks so we can log what this worker supports)
from app.core.queue import Delivery, RedisQueue
from app.db.models import Attempt, Job, JobStatus, utcnow
from app.db.session import get_sessionmaker
from worker.executor import ExecutionResult, execute
from worker.registry import registered_tasks

logger = logging.getLogger("worker")

QUEUE_NAME = os.getenv("WORKER_QUEUE", "default")
TASK_TIMEOUT_SECONDS = float(os.getenv("TASK_TIMEOUT_SECONDS", "60"))


def _start_attempt(db: Session, job: Job, worker_id: str) -> Attempt:
    now = utcnow()
    job.status = JobStatus.RUNNING
    job.attempts += 1
    job.started_at = now
    attempt = Attempt(
        job_id=job.id, attempt_number=job.attempts, worker_id=worker_id, started_at=now
    )
    db.add(attempt)
    db.commit()
    return attempt


def _finish_attempt(db: Session, job: Job, attempt: Attempt, outcome: ExecutionResult) -> None:
    now = utcnow()
    attempt.finished_at = now
    job.finished_at = now
    if outcome.ok:
        job.status = JobStatus.SUCCEEDED
        job.result = outcome.result
        job.last_error = None
        attempt.outcome = "succeeded"
    else:
        # Phase 3 will retry with backoff first and only then mark the job dead.
        job.status = JobStatus.DEAD
        job.last_error = outcome.error
        attempt.outcome = "timeout" if outcome.timed_out else "failed"
        attempt.error = outcome.error
    db.commit()


def handle_delivery(
    delivery: Delivery,
    queue: RedisQueue,
    session_factory: sessionmaker[Session],
    worker_id: str,
    timeout_seconds: float,
) -> None:
    with session_factory() as db:
        job = db.get(Job, uuid.UUID(delivery.job_id))
        if job is None:
            logger.warning("job %s not found in Postgres; dropping message", delivery.job_id)
            queue.ack(delivery)
            return
        if job.status is not JobStatus.QUEUED:
            # e.g. cancelled, or a duplicate delivery of a job that already ran.
            logger.info("job %s is %s; skipping", job.id, job.status.value)
            queue.ack(delivery)
            return

        attempt = _start_attempt(db, job, worker_id)
        logger.info("job %s: running %s (attempt %d)", job.id, job.task_name, job.attempts)
        outcome = execute(job.task_name, job.payload, timeout_seconds)
        _finish_attempt(db, job, attempt, outcome)
        logger.info("job %s: %s", job.id, job.status.value)

    # Ack only after the result is saved: a crash before this line leaves the message
    # pending in Redis, so the job is retried rather than lost (at-least-once delivery).
    queue.ack(delivery)


def process_one(
    queue: RedisQueue,
    queue_name: str,
    worker_id: str,
    session_factory: sessionmaker[Session],
    timeout_seconds: float = TASK_TIMEOUT_SECONDS,
    block_ms: int = 2000,
) -> bool:
    """Wait for one job and process it. Returns False if nothing arrived in time."""
    delivery = queue.read(queue_name, worker_id, block_ms=block_ms)
    if delivery is None:
        return False
    handle_delivery(delivery, queue, session_factory, worker_id, timeout_seconds)
    return True


def run_forever(stop: threading.Event) -> None:
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    queue = RedisQueue()
    session_factory = get_sessionmaker()
    logger.info(
        "worker %s listening on queue %r; tasks: %s",
        worker_id,
        QUEUE_NAME,
        ", ".join(registered_tasks()),
    )
    while not stop.is_set():
        try:
            process_one(queue, QUEUE_NAME, worker_id, session_factory)
        except redis.RedisError:
            logger.exception("redis error; retrying in 2s")
            stop.wait(2)
        except Exception:
            logger.exception("unexpected error while processing a job; continuing")
            stop.wait(1)
    logger.info("worker %s stopped", worker_id)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    stop = threading.Event()

    def _request_stop(signum: int, _frame: object) -> None:
        logger.info("signal %s received: finishing the current job, then exiting", signum)
        stop.set()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    run_forever(stop)
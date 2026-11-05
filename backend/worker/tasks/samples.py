import logging
import time

from worker.registry import task

logger = logging.getLogger(__name__)


@task("add")
def add(a: float, b: float) -> dict:
    """Instant, deterministic task. Handy for tests."""
    return {"sum": a + b}


@task("send_email")
def send_email(to: str, subject: str = "", body: str = "") -> dict:
    """Pretends to send an email (no real email is sent)."""
    if "@" not in to:
        raise ValueError(f"invalid email address: {to!r}")
    time.sleep(0.5)  # simulate a slow network call
    logger.info("pretend-sent email to %s (subject=%r)", to, subject)
    return {"sent_to": to, "subject": subject}


@task("sleep")
def sleep(seconds: float = 1.0) -> dict:
    """Takes a while. Used to test timeouts and to kill a worker mid-job in Phase 3."""
    time.sleep(seconds)
    return {"slept": seconds}


@task("always_fail")
def always_fail(message: str = "this task always fails") -> dict:
    """Always raises. Used to test retries and the dead-letter queue in Phase 3."""
    raise RuntimeError(message)
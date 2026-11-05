import itertools

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api.jobs import get_queue
from app.core.queue import Delivery
from app.db.models import Base
from app.db.session import get_db
from app.main import app


class FakeQueue:
    """In-memory stand-in for RedisQueue with the same enqueue/read/ack interface.

    Lets tests run without a Redis server, and lets them inspect what was enqueued and
    what is still waiting for an ack.
    """

    def __init__(self) -> None:
        self.enqueued: list[tuple[str, str]] = []  # (queue, job_id), in order, never cleared
        self._ready: dict[str, list[str]] = {}
        self._pending: dict[str, Delivery] = {}
        self._ids = itertools.count(1)

    def enqueue(self, queue: str, job_id: str) -> str:
        self.enqueued.append((queue, job_id))
        self._ready.setdefault(queue, []).append(job_id)
        return f"{next(self._ids)}-0"

    def read(self, queue: str, consumer: str, block_ms: int = 0) -> Delivery | None:
        ready = self._ready.get(queue)
        if not ready:
            return None
        delivery = Delivery(entry_id=f"{next(self._ids)}-0", job_id=ready.pop(0), queue=queue)
        self._pending[delivery.entry_id] = delivery
        return delivery

    def ack(self, delivery: Delivery) -> None:
        self._pending.pop(delivery.entry_id, None)

    @property
    def pending_count(self) -> int:
        """Messages that were read but never acknowledged."""
        return len(self._pending)


@pytest.fixture
def session_factory():
    """A fresh in-memory SQLite database per test (fast; Postgres-specific behaviour is
    covered by running the migration against a real Postgres in CI/compose)."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    yield sessionmaker(bind=engine, expire_on_commit=False)
    engine.dispose()


@pytest.fixture
def db_session(session_factory):
    with session_factory() as session:
        yield session


@pytest.fixture
def fake_queue():
    return FakeQueue()


@pytest.fixture
def client(db_session, fake_queue):
    def _override_db():
        yield db_session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_queue] = lambda: fake_queue
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
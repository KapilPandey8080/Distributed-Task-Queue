import uuid
from datetime import UTC, datetime, timedelta

from app.db.models import Job


def _naive_utc(iso: str) -> datetime:
    """SQLite drops tzinfo; normalise so assertions work on both SQLite and Postgres."""
    dt = datetime.fromisoformat(iso)
    return dt.astimezone(UTC).replace(tzinfo=None) if dt.tzinfo else dt


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_enqueue_creates_queued_job_with_defaults(client):
    r = client.post("/jobs", json={"task_name": "send_email", "payload": {"to": "a@b.com"}})
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "queued"
    assert body["queue"] == "default"
    assert body["priority"] == 5
    assert body["attempts"] == 0
    assert body["max_attempts"] == 3
    assert body["payload"] == {"to": "a@b.com"}
    assert body["result"] is None and body["started_at"] is None
    uuid.UUID(body["id"])  # valid UUID


def test_get_job_returns_what_was_enqueued(client):
    created = client.post("/jobs", json={"task_name": "resize", "priority": 9}).json()
    fetched = client.get(f"/jobs/{created['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["id"] == created["id"]
    assert fetched.json()["priority"] == 9


def test_get_unknown_job_is_404(client):
    assert client.get(f"/jobs/{uuid.uuid4()}").status_code == 404


def test_get_malformed_id_is_422(client):
    assert client.get("/jobs/not-a-uuid").status_code == 422


def test_job_is_persisted(client, db_session):
    job_id = client.post("/jobs", json={"task_name": "x"}).json()["id"]
    assert db_session.get(Job, uuid.UUID(job_id)) is not None


def test_delay_seconds_pushes_run_at_into_the_future(client):
    before = datetime.now(UTC).replace(tzinfo=None)
    body = client.post("/jobs", json={"task_name": "later", "delay_seconds": 60}).json()
    assert _naive_utc(body["run_at"]) >= before + timedelta(seconds=59)


def test_explicit_run_at_is_respected(client):
    target = datetime.now(UTC) + timedelta(hours=1)
    body = client.post("/jobs", json={"task_name": "later", "run_at": target.isoformat()}).json()
    assert abs(_naive_utc(body["run_at"]) - target.replace(tzinfo=None)) < timedelta(seconds=1)


def test_run_at_and_delay_together_rejected(client):
    r = client.post(
        "/jobs",
        json={"task_name": "x", "delay_seconds": 5, "run_at": datetime.now(UTC).isoformat()},
    )
    assert r.status_code == 422


def test_validation_rejects_bad_input(client):
    assert client.post("/jobs", json={"task_name": ""}).status_code == 422
    assert client.post("/jobs", json={}).status_code == 422
    assert client.post("/jobs", json={"task_name": "x", "priority": 10}).status_code == 422
    assert client.post("/jobs", json={"task_name": "x", "priority": -1}).status_code == 422
    assert client.post("/jobs", json={"task_name": "x", "max_attempts": 0}).status_code == 422
    assert client.post("/jobs", json={"task_name": "x", "delay_seconds": -5}).status_code == 422


def test_idempotency_key_returns_original_job(client, db_session):
    first = client.post("/jobs", json={"task_name": "x", "idempotency_key": "k1"})
    second = client.post("/jobs", json={"task_name": "x", "idempotency_key": "k1"})
    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]
    assert db_session.query(Job).count() == 1


def test_same_idempotency_key_in_different_queues_is_independent(client):
    a = client.post("/jobs", json={"task_name": "x", "queue": "a", "idempotency_key": "k"})
    b = client.post("/jobs", json={"task_name": "x", "queue": "b", "idempotency_key": "k"})
    assert a.status_code == b.status_code == 201
    assert a.json()["id"] != b.json()["id"]


def test_jobs_without_idempotency_key_are_never_deduplicated(client, db_session):
    for _ in range(3):
        assert client.post("/jobs", json={"task_name": "x"}).status_code == 201
    assert db_session.query(Job).count() == 3

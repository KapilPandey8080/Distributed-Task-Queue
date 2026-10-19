# TaskQueue

A small distributed task queue (think a mini Celery/Sidekiq): clients enqueue jobs through an
HTTP API, workers execute them, and the system handles retries, crashes, priorities and
scheduling. Built to demonstrate concurrency, failure handling and delivery guarantees.

**Status: Phase 1 of 8 — data model and enqueue API.** Workers, retries, scheduling and the
dashboard come in later phases (see the roadmap).

## Quickstart

```bash
make up                       # Postgres + Redis + API (runs migrations on start)
curl -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"task_name": "send_email", "payload": {"to": "a@b.com"}, "priority": 9}'
curl localhost:8000/jobs/<id>
open http://localhost:8000/docs   # interactive API docs
```

Local development without Docker for the API:

```bash
cd backend
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
make -C .. test lint
```

## API (Phase 1)

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Enqueue a job. `201` on create, `200` if the idempotency key matched an existing job |
| `GET` | `/jobs/{id}` | Job status, attempts, result/error |
| `GET` | `/health` | Liveness |

`POST /jobs` body: `task_name` (required), `payload`, `queue` (default `default`),
`priority` (0-9, higher first, default 5), `max_attempts` (default 3),
`run_at` **or** `delay_seconds`, `idempotency_key`.

## Layout

See [docs/architecture.md](docs/architecture.md) for the schema and job lifecycle.

```
backend/app/   api/ (routes) · db/ (models, session, repository) · schemas/ · core/ (queue logic, Phase 2+)
backend/alembic/   migrations
tests/         unit/ · integration/ · chaos/ (Phase 3+)
```

## Roadmap

- [x] **P0/P1** Repo, Compose, CI, schema + migrations, enqueue/status API
- [ ] P2 Redis Streams queue + single worker
- [ ] P3 Retries, backoff, dead-letter queue, reaper, chaos test
- [ ] P4 Priorities, delayed and recurring jobs
- [ ] P5 Concurrency, graceful shutdown, rate limiting
- [ ] P6 Metrics and observability
- [ ] P7 Dashboard and API-key auth
- [ ] P8 Benchmarks, docs, demo

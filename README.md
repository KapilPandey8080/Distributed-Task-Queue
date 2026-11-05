# TaskQueue

A small distributed task queue (think a mini Celery/Sidekiq): clients enqueue jobs through an
HTTP API, workers execute them, and the system is being built to handle retries, crashes,
priorities and scheduling. It's a learning project that demonstrates concurrency, failure
handling and delivery guarantees.

**Status: Phase 2 of 8 — jobs are enqueued, delivered through Redis, and run by workers.**
Retries, the dead-letter queue and crash recovery come in Phase 3 (see the roadmap).

## How it works

```
 client ──POST /jobs──► API ──1. save job──► Postgres  (source of truth)
                         │
                         └──2. push job id──► Redis Stream
                                                  │
                          worker ◄──3. read───────┘
                            │
                            ├─ 4. mark running ──► Postgres
                            ├─ 5. run task in a child process (killed on timeout)
                            ├─ 6. save result / error ──► Postgres
                            └─ 7. ack the message in Redis (only now)
```

- **Postgres** holds every job and its history. **Redis** only carries job ids to workers.
- The message is acknowledged **after** the result is saved, so a worker that dies mid-job leaves
  the message pending in Redis rather than losing the job (at-least-once delivery).
- Each task runs in its own child process, so a task that hangs can be killed at the timeout.

## Quickstart

```bash
make up        # Postgres + Redis + API + worker (migrations run on start)
curl -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"task_name": "add", "payload": {"a": 2, "b": 3}}'
curl localhost:8000/jobs/<id>      # status "succeeded", result {"sum": 5}
```

Interactive API docs: http://localhost:8000/docs

Run more workers: `docker compose up -d --scale worker=3`
Watch them: `docker compose logs -f worker`

### Without Docker for the app

Start only the databases, then run the API and a worker yourself (two terminals):

```bash
docker compose up -d postgres redis
cd backend
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
alembic upgrade head
uvicorn app.main:app --reload          # terminal 1
python -m worker                       # terminal 2
```

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Enqueue a job. `201` on create, `200` if the idempotency key matched an existing job |
| `GET` | `/jobs/{id}` | Job status, attempts, result / error |
| `GET` | `/health` | Liveness |

`POST /jobs` body: `task_name` (required), `payload`, `queue` (default `default`),
`priority` (0-9, higher first, default 5), `max_attempts` (default 3),
`run_at` **or** `delay_seconds`, `idempotency_key`.

## Writing a task

Tasks are plain functions registered by name. The job's `payload` becomes keyword arguments, and
the return value (JSON-serializable) is stored as the job's `result`.

```python
# backend/worker/tasks/samples.py
from worker.registry import task

@task("add")
def add(a: float, b: float) -> dict:
    return {"sum": a + b}
```

Built-in sample tasks: `add`, `send_email` (fake), `sleep`, `always_fail`.
Raising an exception marks the attempt as failed.

## Worker configuration

| Environment variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | local Postgres | SQLAlchemy URL |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `WORKER_QUEUE` | `default` | Queue this worker consumes |
| `TASK_TIMEOUT_SECONDS` | `60` | A task running longer than this is killed |

On `SIGTERM` / `Ctrl+C` the worker finishes its current job, then exits.

## Development

```bash
make test      # pytest (unit + integration, no Redis or Postgres needed)
make lint      # ruff
```

Tests run on in-memory SQLite with a fake queue. The migration and the idempotency race were
also verified against real Postgres, and the worker against real Redis.

## Layout

See [docs/architecture.md](docs/architecture.md) for the schema and job lifecycle.

```
backend/
  app/        api/ (routes) · db/ (models, repository) · core/queue.py (Redis Streams) · schemas/
  worker/     registry.py · executor.py · runner.py · tasks/
  alembic/    migrations
tests/        unit/ · integration/
```

## Known limitations (until later phases)

- A job whose task fails goes straight to `dead`. **Retries with backoff arrive in Phase 3.**
- If a worker is killed mid-job, the message stays pending in Redis and the job stays `running`.
  **The Phase 3 reaper will detect and re-queue these.**
- If Redis is down when a job is created, the job is saved but not delivered. The API still
  returns `201`; a Phase 3 sweep will re-enqueue it.
- Jobs with a future `run_at` / `delay_seconds` are stored but not delivered yet (Phase 4).

## Roadmap

- [x] **P0/P1** Repo, Compose, CI, schema + migrations, enqueue/status API
- [x] **P2** Redis Streams queue + worker (task registry, timeouts, ack-after-save)
- [ ] P3 Retries, backoff, dead-letter queue, reaper, chaos test
- [ ] P4 Priorities, delayed and recurring jobs
- [ ] P5 Concurrency, graceful shutdown, rate limiting
- [ ] P6 Metrics and observability
- [ ] P7 Dashboard and API-key auth
- [ ] P8 Benchmarks, docs, demo
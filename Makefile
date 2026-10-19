.PHONY: up down logs test lint fmt migrate revision

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f api

test:
	cd backend && python -m pytest

lint:
	cd backend && ruff check . ../tests

fmt:
	cd backend && ruff format . ../tests

# Run migrations against the database in DATABASE_URL (defaults to local compose Postgres)
migrate:
	cd backend && alembic upgrade head

# make revision m="add something"
revision:
	cd backend && alembic revision -m "$(m)"

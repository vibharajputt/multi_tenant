#!/usr/bin/env bash
# One-command setup: load data + build rollups. Run AFTER `docker compose up -d`.
set -euo pipefail
echo "==> waiting for database..."
until docker compose exec -T db pg_isready -U "${POSTGRES_USER:-hackathon}" >/dev/null 2>&1; do sleep 2; done

echo "==> creating rollup tables..."
docker compose exec -T db psql -U "${POSTGRES_USER:-hackathon}" -d "${POSTGRES_DB:-hackathon}" \
  -f - < app/rollups.sql

echo "==> loading CSVs from ./out into orders table..."
docker compose run --rm api python -m app.loader

echo "==> re-creating rollup tables (in case of reload) + full backfill..."
docker compose exec -T db psql -U "${POSTGRES_USER:-hackathon}" -d "${POSTGRES_DB:-hackathon}" \
  -f - < app/rollups.sql
docker compose run --rm api python -m app.refresh backfill

echo "==> done. API at http://localhost:${API_PORT:-8000}  |  monitor at /stats"

# Multi-Tenant Aggregation Layer

A layer between a large multi-tenant `orders` database and the dashboards. It
answers the four dashboard queries for **any tenant in under a second**, keeps
numbers **fresh within seconds**, **never touches the base table** from the API
(so the DB stays fast), keeps every tenant's data **separate**, and stays
**correct** (numbers equal a direct database query), and keeps all of that when
the data **scales 10×**.

Stack: PostgreSQL (main DB) + pre-computed rollup tables + a change-tracking
refresh worker + Redis cache + FastAPI. **To run it you change only `.env` —
never the code.**

---

## Data model

The provided generator writes four CSV tables: `tenants`, `customers`,
`products`, `orders`. Category lives in `products`; the loader denormalizes it
onto `orders` once at load time so every query stays single-table-fast.
Order status is one of `placed`, `shipped`, `cancelled`.

---

## Quick start (any laptop — needs only Docker)

```bash
# 1. configuration (defaults work; edit if you like)
cp .env.example .env

# 2. generate data (organiser's generator). Small run first:
pip install numpy pandas
python generate_data.py --preset small --out ./out_small       # 1M orders, 10 tenants
#   full run:  python generate_data.py --preset main --out ./out_main --workers 8
#              then set HOST_DATA_DIR=./out_main in .env

# 3. start everything (db, cache, api, worker)
docker compose up -d --build

# 4. load the 4 tables + build rollups (one command)
./setup.sh

# 5. check it
curl "http://localhost:8000/tenants/1/orders/count?from=2025-01-01&to=2026-10-31"
```

Open `http://localhost:8000/stats` for the live monitoring page and
`http://localhost:8000/docs` for the interactive API.

> Data spans 2024-11-01 to 2026-10-31. To change scale, data location, refresh
> speed, cache TTL or query limits, edit **`.env` only**.

---

## API endpoints

| Requirement | Endpoint | Example |
|---|---|---|
| FR-1 counts & totals | `GET /tenants/{id}/orders/count?from=&to=&status=` | `/tenants/1/orders/count?from=2025-01-01&to=2026-10-31` |
| FR-2 trend over time | `GET /tenants/{id}/orders/trend?from=&to=&metric=` | `/tenants/1/orders/trend?metric=revenue` |
| FR-3 top-N products | `GET /tenants/{id}/products/top?n=&by=` | `/tenants/1/products/top?n=10&by=revenue` |
| FR-4 breakdown | `GET /tenants/{id}/orders/breakdown?by=` (status \| category) | `/tenants/1/orders/breakdown?by=category` |
| FR-5 tenant filter | every endpoint is scoped by `{id}` in the path | — |
| FR-6 fresh new/updated data | handled by the worker; see `/freshness` | — |
| FR-7 simple REST + README | this file | — |
| Bonus B-1 "updated Xs ago" | `GET /freshness` | — |
| Bonus B-2 rebuild from scratch | `docker compose run --rm api python -m app.refresh rebuild` | — |
| Bonus B-3 per-tenant query limit | `MAX_CONCURRENT_QUERIES_PER_TENANT` in `.env` (HTTP 429 when exceeded) | — |
| Bonus B-4 config-driven metric | `GET /tenants/{id}/metrics/{name}` (add metrics by one SQL row, no code) | `/tenants/1/metrics/avg_order_value` |
| Bonus B-5 monitoring page | `GET /stats` | — |

Every query response includes `query_ms` so query speed is visible.

---

## How it works (and why it hits the targets)

- **Fast (<1s):** the API reads only small **rollup tables** (`rollup_daily`,
  `rollup_product_daily`), never the multi-million-row base table.
- **Fresh (seconds):** every `INSERT`/`UPDATE` on `orders` gets a monotonic
  `change_id` (a trigger). A **refresh worker** runs every
  `REFRESH_INTERVAL_SEC`, reads only rows with `change_id > last_seen`, and
  recomputes the affected **(tenant, day) buckets** from the base table. Cost is
  proportional to what changed — independent of tenant count, so no Kafka.
  Using `change_id` (not `updated_at`) means a live change is never missed even
  if the laptop clock differs from the data's timestamps.
- **Correct (== direct DB):** a changed bucket is **recomputed in full** from the
  base table (not delta-added), so inserts, status changes
  (`placed → shipped → cancelled`) and any late update all stay exact. Verify:
  ```bash
  ./verify.sh
  ```
- **DB safe:** dashboards hit rollups + Redis, so normal reads/writes on
  `orders` are untouched.
- **Separate tenants:** `tenant_id` comes from the URL and is applied to every
  query; rollup tables are primary-keyed by tenant — no endpoint can return more
  than one tenant.
- **Scales 10×:** rollups grow with (tenants × days × categories), not with raw
  row count, so 1M → 10M barely changes query time.

---

## Verifying freshness live (FR-6 demo)

```bash
# a brand-new order (only base columns — the trigger fills category):
docker compose exec -T db psql -U hackathon -d hackathon -c \
 "INSERT INTO orders(order_id,tenant_id,customer_id,product_id,quantity,unit_price,amount,status,created_at,updated_at)
  VALUES (900000001,1,1,1,1,100,100,'placed',now(),now());"
# wait REFRESH_INTERVAL_SEC, then the count goes up:
curl "http://localhost:8000/tenants/1/orders/count?from=2024-11-01&to=2026-12-31"
curl "http://localhost:8000/freshness"

# change its status and watch the breakdown update the same way:
docker compose exec -T db psql -U hackathon -d hackathon -c \
 "UPDATE orders SET status='cancelled' WHERE order_id=900000001;"
```

---

## Configuration reference (`.env`)

| Key | Meaning |
|---|---|
| `POSTGRES_*`, `REDIS_PORT`, `API_PORT` | connection + ports |
| `HOST_DATA_DIR` | folder on your machine with the CSVs (`./out_small` or `./out_main`) |
| `REFRESH_INTERVAL_SEC` | how often the worker refreshes (default 10s) |
| `REFRESH_BATCH_SIZE` | rows per worker batch |
| `CACHE_TTL_SEC` | Redis cache lifetime for query results |
| `MAX_CONCURRENT_QUERIES_PER_TENANT` | per-tenant heavy-query limit (B-3) |
| `DATA_DIR` | container path (do not change) |

---

## Centralized platform view (all tenants, operator role)

The per-tenant endpoints are strictly isolated (a tenant only ever sees its own
data). The platform operator gets a **centralized view across all tenants** from
the *same* shared rollups — one database, one rollup store, one worker, one API:

| View | Endpoint |
|---|---|
| Whole-platform totals | `GET /platform/summary?from=&to=` |
| Per-tenant summary (all tenants in one call) | `GET /platform/tenants?from=&to=` |
| Platform counts / trend / top / breakdown / metric | `GET /platform/orders/count`, `/platform/orders/trend`, `/platform/products/top`, `/platform/orders/breakdown`, `/platform/metrics/{name}` |

In the dashboard, pick **"All tenants (Platform)"** from the tenant dropdown to
switch every widget to the centralized view. `platform total == sum of all
tenants` (verified).

## Live data: continuous inserts + updates + cancellations

Real traffic is a constant mix of new orders, status changes and cancellations
happening at once. The engine handles this without any streaming system:

- Every `INSERT`/`UPDATE`/cancel gets a monotonic `change_id` (trigger), stamped
  inside its own transaction — concurrent writes each get a unique id, none lost.
- The worker reads `change_id > watermark ORDER BY change_id` in batches, so any
  burst is processed in order and the watermark catches up. Cost is proportional
  to what changed, independent of tenant count.
- Each touched `(tenant, day)` bucket is **recomputed in full from the base
  table**, so simultaneous inserts/ships/cancels on the same bucket can never
  double-count — the rollup always equals a direct query.
- Freshness is bounded by `REFRESH_INTERVAL_SEC` (default 10s — within the
  "Great <30s" target). Under heavier load, lower the interval or raise
  `REFRESH_BATCH_SIZE`.

Demo it live (stack up, in a second terminal):
```bash
docker compose run --rm api python -m app.simulate --rate 50 --seconds 120
```
Watch `/freshness`, the dashboard, and `./verify.sh` stay correct while the
simulator pumps inserts + ships + cancels every second.

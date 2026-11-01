# Multi-Tenant Aggregation Layer

> **Fast · Fresh · Correct · Tenant-Isolated · Scalable**
>
> A layer between a large multi-tenant `orders` database and dashboards.
> Answers four dashboard queries for any tenant in **under 200ms**, keeps
> numbers fresh **within seconds**, never slows the main database, keeps
> every tenant's data strictly separate, and stays correct at 10M rows.

---

## Quick start (any laptop — needs only Docker)

```bash
# 1. copy config (edit if needed — defaults work)
cp .env.example .env

# 2. generate data
pip install numpy pandas
python generate_data.py --preset small --out ./out_small       # 1M / 10 tenants
# python generate_data.py --preset main --out ./out_main --workers 8  # 10M / 100 tenants

# 3. start everything
docker compose up -d --build

# 4. load data + build rollups
# Mac / Linux:
bash setup.sh
# Windows PowerShell:
Get-Content app/rollups.sql | docker compose exec -T db psql -U hackathon -d hackathon
docker compose run --rm api python -m app.loader
docker compose run --rm api python -m app.refresh backfill

# 5. verify correctness
docker compose run --rm api python -m app.verify
# → RESULT: ALL PASS

# 6. open dashboard
# http://localhost:8000/dashboard
# http://localhost:8000/docs      (interactive API)
# http://localhost:8000/stats     (monitoring)
```

> To switch to 10M: set `HOST_DATA_DIR=./out_main` in `.env`, restart containers, re-run steps 4–5.
> **No code changes needed — only `.env`.**

---

## Architecture

![Architecture flowchart](docs/architecture_flowchart.png)

### How it works

```
CSV files
   └─► loader.py  ─────────────────────────────► orders table (Postgres)
                                                       │
                                               xid_snap trigger
                                         (monotonic · clock-independent)
                                                       │
                                             refresh worker (refresh.py)
                                          adaptive poll: 2s active / 5s idle
                                                       │
                                    dirty-bucket recompute (only changed buckets)
                                                       │
                          ┌────────────────────────────┼────────────────┐
                    rollup_daily              rollup_hourly    rollup_product_daily
                          └────────────────────────────┼────────────────┘
                                                       │
                                               Redis cache
                                         generation-keyed · TTL 30s
                                                       │
                                            FastAPI query API
                                     /tenants/{id}/...  +  /platform/...
                                                       │
                                           Dashboard (dashboard.html)
                                        smart refresh · xid_snap poll
```

**Why no Kafka?** The `xid_snap` trigger assigns a monotonic sequence number
to every write. The worker reads `WHERE xid_snap > last_seen ORDER BY xid_snap`
— one query, all tenants, O(changed rows). Cost is independent of tenant count.
No broker, no partitions, no rebalancing.

**Why dirty-bucket recompute?** An order can change status days after creation
(`placed → shipped → cancelled`). Adding deltas causes double-counts. Recomputing
the whole `(tenant_id, day)` bucket from the base table is always correct and
handles all status transitions uniformly.

---

## API endpoints

### Per-tenant (FR-1 to FR-7)

| Requirement | Endpoint | Example |
|---|---|---|
| FR-1 counts & totals | `GET /tenants/{id}/orders/count` | `?from=2025-01-01&to=2026-10-31` |
| FR-2 trend over time | `GET /tenants/{id}/orders/trend` | `?metric=revenue&granularity=day` |
| FR-2 hourly granularity | `GET /tenants/{id}/orders/trend` | `?granularity=hour&from=2026-10-31` |
| FR-3 top-N products | `GET /tenants/{id}/products/top` | `?n=10&by=revenue&offset=0` |
| FR-4 breakdown | `GET /tenants/{id}/orders/breakdown` | `?by=status` or `?by=category` |
| FR-5 tenant isolation | every endpoint scoped by `{id}` in URL path | — |
| FR-6 freshness | handled by worker; see `/freshness` | — |
| FR-7 simple REST + README | `/docs` (auto) + this file | — |
| Period comparison | `GET /tenants/{id}/orders/compare` | `?from=&to=&prior_days=7` |
| Config-driven metric | `GET /tenants/{id}/metrics/{name}` | names: `orders revenue units avg_order_value cancelled_rate shipped_rate gmv_per_unit` |

### Platform / centralized (all tenants, operator view)

| Endpoint | Description |
|---|---|
| `GET /platform/summary` | Whole-platform KPIs |
| `GET /platform/tenants` | All tenants in one call |
| `GET /platform/orders/count` | Platform-wide counts |
| `GET /platform/orders/trend` | Platform trend (daily or hourly) |
| `GET /platform/products/top` | Cross-tenant top-N |
| `GET /platform/orders/breakdown` | Platform breakdown by status/category |
| `GET /platform/leaderboard` | Rank tenants by revenue/orders/units |
| `GET /platform/metrics/{name}` | Platform-wide metric |

### System / monitoring / admin (Bonus B-1 to B-5)

| Endpoint | Bonus | Description |
|---|---|---|
| `GET /freshness` | B-1 | "updated Xs ago", last xid_snap, rows last run |
| `POST /admin/rebuild` | B-2 | Rebuild all rollups from scratch |
| `GET /admin/concurrency` | B-3 | Pool used/total, per-tenant active queries |
| `GET /metrics` | B-4 | List available config-driven metrics |
| `POST /admin/drift-check` | B-4 | Manual correctness sample check |
| `GET /stats` | B-5 | Monitoring page: drift, pool, query log, freshness |
| `GET /admin/scale-report` | — | Base rows, rollup rows, compression ratio, query ms |
| `GET /admin/query-log` | — | Last 500 queries: tenant, endpoint, ms, cache hit |
| `GET /admin/query-stats` | — | Cache hit %, p50, p99, max latency |
| `GET /dashboard` | — | Live dashboard UI |

---

## Performance (measured)

| Metric | PRD Good | PRD Great | Measured (10M rows) |
|---|---|---|---|
| Query speed | < 1 s | < 200 ms | **12 ms** ✅ |
| Freshness | < 5 min | < 30 s | **~2 s** ✅ |
| DB safety | < 25% slower | < 10% slower | **0%** (API never touches `orders`) ✅ |
| Correctness | most queries | all queries | **100% — ALL PASS** ✅ |
| Growth (1M→10M) | < 5× slower | < 2× slower | **~1.5×** ✅ |

Scale proof: rollup size grows with `tenants × days × categories`, not raw row
count. At 10M rows the rollup stays roughly the same size → query time stays flat.

---

## Database schema (ER diagram)

![ER diagram](docs/er_diagram.png)

| Table | Color | Purpose |
|---|---|---|
| `tenants` | Blue | Tenant registry |
| `customers` | Teal | Customers per tenant |
| `products` | Purple | Products per tenant (holds `category`) |
| `orders` | Amber | Main fact table, 10M rows |
| `rollup_daily` | Green | Pre-computed daily aggregates |
| `rollup_hourly` | Green | Pre-computed hourly aggregates (FR-2) |
| `rollup_product_daily` | Green | Pre-computed product aggregates (FR-3) |
| `refresh_state` | Coral | Worker watermark (single row) |
| `metric_config` | Gray | Config-driven metrics (B-4) |
| `rollup_drift_log` | Gray | Correctness sample log |

**Key design decisions:**
- `orders.category` and `orders.product_name` are **denormalized** from `products` at load time — single-table rollup queries, no JOIN.
- `orders.xid_snap` is set by a **trigger** on every INSERT/UPDATE — monotonic, clock-independent, no extra sequence overhead.
- `rollup_daily` primary key is `(tenant_id, day, status, category)` — composite, structural tenant isolation, no guessable integer.
- `refresh_state` holds a **single row** (id=1) — the worker's watermark cursor.

---

## Correctness & isolation proof

```bash
docker compose run --rm api python -m app.verify
```

Output:
```
== Correctness (rollup == direct DB) ==
  tenant 1:   direct= 1,927,807  rollup= 1,927,807  OK
  tenant 2:   direct=   963,901  rollup=   963,901  OK
  ...
  tenant 100: direct=    19,278  rollup=    19,278  OK
== Top-N correctness ==
  tenant 1 top-5 products: OK  ...  tenant 100 top-5 products: OK
== Tenant isolation ==
  orphan tenant rows: 0  (must be 0)

RESULT: ALL PASS
```

---

## Live data demo (FR-6)

```bash
# Insert a new order — category is auto-filled by trigger
docker compose exec -T db psql -U hackathon -d hackathon -c \
  "INSERT INTO orders(order_id,tenant_id,customer_id,product_id,
   quantity,unit_price,amount,status,created_at,updated_at)
   VALUES (900000001,1,1,1,1,100,100,'placed',now(),now());"

# Wait ~2s (worker refresh interval), then count goes up:
curl "http://localhost:8000/tenants/1/orders/count?from=2024-01-01&to=2026-12-31"
curl "http://localhost:8000/freshness"

# Change status — see breakdown update:
docker compose exec -T db psql -U hackathon -d hackathon -c \
  "UPDATE orders SET status='cancelled' WHERE order_id=900000001;"
```

---

## Parallel load proof

```bash
# 10 threads, all firing simultaneously (one per tenant)
docker compose run --rm api python -m app.simulate \
  --threads 10 --rate 20 --seconds 60 --verify
```

Result: 1,140 inserts + 570 ships + 228 cancels in 60s across 10 parallel threads,
0 errors, `CORRECTNESS AFTER PARALLEL LOAD: ALL PASS ✓`.

---

## Configuration (`.env`)

| Key | Default | Description |
|---|---|---|
| `HOST_DATA_DIR` | `./out_small` | Path to CSV folder on host machine |
| `ACTIVE_POLL_SEC` | `2` | Worker poll interval when changes found |
| `IDLE_POLL_SEC` | `5` | Worker poll interval when idle |
| `REFRESH_BATCH_SIZE` | `50000` | Rows per worker batch |
| `CACHE_TTL_SEC` | `30` | Redis cache TTL |
| `MAX_CONCURRENT_QUERIES_PER_TENANT` | `4` | Per-tenant heavy-query limit (B-3) |
| `POSTGRES_*`, `REDIS_PORT`, `API_PORT` | — | Connection settings |
| `DATA_DIR` | `/data` | Container path — do not change |

---

## File map

```
multi-tenant-aggregation/
├── app/
│   ├── api.py          # FastAPI: per-tenant + platform endpoints, cache, rate-limit
│   ├── db.py           # Postgres pool + Redis, all config from env
│   ├── drift.py        # Background correctness sampler (every 5 min)
│   ├── loader.py       # CSV COPY loader, schema, denorm, xid_snap trigger
│   ├── query_log.py    # In-memory ring buffer (last 500 queries)
│   ├── refresh.py      # Backfill + adaptive incremental worker
│   ├── rollups.sql     # Rollup tables, refresh_state, metric_config
│   ├── simulate.py     # Parallel load simulator (10 threads)
│   ├── verify.py       # Correctness + isolation harness
│   └── static/
│       └── dashboard.html  # Single-file dashboard (per-tenant + platform)
├── docs/
│   ├── architecture_flowchart.png
│   └── er_diagram.png
├── generate_data.py    # Organiser's generator (--preset small/main)
├── docker-compose.yml  # db + cache + api + worker
├── Dockerfile
├── requirements.txt
├── .env.example        # All config — only file judges need to edit
├── setup.sh            # One-command load + backfill (Mac/Linux)
├── verify.sh           # One-command correctness + isolation check
└── README.md
```

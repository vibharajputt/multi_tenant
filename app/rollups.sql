-- ============================================================================
-- Rollup tables, refresh state, and config-driven metrics.
--
-- ID design:
--   tenant_id, product_id  — match the generator's INT columns (no change
--                             needed for rollup keys since these are the
--                             generator's IDs; the orders table uses a
--                             UUID-based order_id via the loader).
--   rollup_daily PK        — (tenant_id, day, status, category): composite,
--                             no single guessable integer key.
--   rollup_product_daily PK— (tenant_id, day, product_id): same principle.
--
-- Grain:
--   day + status + category → answers FR-1 (counts/totals), FR-2 (trend),
--                              FR-4 (breakdown by status or category)
--   day + product_id        → answers FR-3 (top-N)
--   All keyed by tenant_id  → tenant isolation is structural (not just a
--                              WHERE clause); cross-tenant reads are impossible.
-- ============================================================================

-- Daily rollup
CREATE TABLE IF NOT EXISTS rollup_daily (
    tenant_id    INT           NOT NULL,
    day          DATE          NOT NULL,
    status       TEXT          NOT NULL,
    category     TEXT          NOT NULL,
    order_count  BIGINT        NOT NULL DEFAULT 0,
    total_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_qty    BIGINT        NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, day, status, category)
);
CREATE INDEX IF NOT EXISTS idx_rd_tenant_day ON rollup_daily (tenant_id, day);

-- Product-level daily rollup (for top-N)
CREATE TABLE IF NOT EXISTS rollup_product_daily (
    tenant_id    INT           NOT NULL,
    day          DATE          NOT NULL,
    product_id   INT           NOT NULL,
    product_name TEXT          NOT NULL,
    category     TEXT          NOT NULL,
    order_count  BIGINT        NOT NULL DEFAULT 0,
    total_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_qty    BIGINT        NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, day, product_id)
);
CREATE INDEX IF NOT EXISTS idx_rpd_tenant_day ON rollup_product_daily (tenant_id, day);

-- ── Refresh watermark ────────────────────────────────────────────────────────
-- Stores the last xmin (Postgres transaction ID) consumed by the worker.
-- xmin is a built-in system column on every Postgres table — it identifies
-- the transaction that last wrote each row. Using xmin means:
--   (a) No extra trigger, no extra column, no extra sequence.
--   (b) Monotonically increasing per-transaction — concurrent writes each get
--       a distinct xmin value (unlike updated_at which can collide at the same
--       millisecond).
--   (c) Clock-independent — no wall-clock skew can cause missed rows.
-- We cast xmin to BIGINT (xid8 in PG14+) for easy comparison.
CREATE TABLE IF NOT EXISTS refresh_state (
    id            INT       PRIMARY KEY DEFAULT 1,
    last_xid_snap BIGINT    NOT NULL DEFAULT 0,   -- last consumed xid_snap value
    last_run_at   TIMESTAMP,
    rows_last_run BIGINT    DEFAULT 0,
    max_updated   TIMESTAMP,
    total_refreshes BIGINT  DEFAULT 0,
    CHECK (id = 1)
);
INSERT INTO refresh_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;


-- ── Hourly rollup (FR-2: "per hour or day") ──────────────────────────────────
-- Answers trend queries at hour granularity within a day.
-- Same dirty-bucket recompute logic as rollup_daily but keyed on hour.
CREATE TABLE IF NOT EXISTS rollup_hourly (
    tenant_id    INT           NOT NULL,
    hour         TIMESTAMP     NOT NULL,   -- truncated to the hour: date_trunc('hour', created_at)
    status       TEXT          NOT NULL,
    category     TEXT          NOT NULL,
    order_count  BIGINT        NOT NULL DEFAULT 0,
    total_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_qty    BIGINT        NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, hour, status, category)
);
CREATE INDEX IF NOT EXISTS idx_rh_tenant_hour ON rollup_hourly (tenant_id, hour);

-- ── Drift log (correctness self-check) ───────────────────────────────────────
-- Background sampler writes one row per check-run, summarising mismatches.
-- SELECT count(*) FROM rollup_drift_log WHERE mismatch_count > 0 = health KPI.
CREATE TABLE IF NOT EXISTS rollup_drift_log (
    id            BIGSERIAL     PRIMARY KEY,
    checked_at    TIMESTAMP     NOT NULL DEFAULT now(),
    buckets_sampled INT         NOT NULL,
    mismatch_count  INT         NOT NULL DEFAULT 0,
    details       JSONB         NOT NULL DEFAULT '[]'
);
-- ── Config-driven metrics (Bonus B-4) ────────────────────────────────────────
-- Add a new metric by INSERTing one row here — no code change.
-- expr is any SQL aggregate that runs over rollup_daily columns.
CREATE TABLE IF NOT EXISTS metric_config (
    name      TEXT PRIMARY KEY,
    expr      TEXT NOT NULL,
    label     TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);
INSERT INTO metric_config (name, expr, label) VALUES
  ('orders',          'SUM(order_count)',
   'Total orders'),
  ('revenue',         'SUM(total_amount)',
   'Total revenue'),
  ('units',           'SUM(total_qty)',
   'Total units'),
  ('avg_order_value',
   'CASE WHEN SUM(order_count)=0 THEN 0 ELSE ROUND(SUM(total_amount)/SUM(order_count),2) END',
   'Avg order value'),
  ('cancelled_rate',
   'ROUND(100.0*SUM(order_count) FILTER (WHERE status=''cancelled'')/NULLIF(SUM(order_count),0),2)',
   'Cancelled %'),
  ('shipped_rate',
   'ROUND(100.0*SUM(order_count) FILTER (WHERE status=''shipped'')/NULLIF(SUM(order_count),0),2)',
   'Shipped %'),
  ('gmv_per_unit',
   'CASE WHEN SUM(total_qty)=0 THEN 0 ELSE ROUND(SUM(total_amount)/SUM(total_qty),2) END',
   'GMV per unit')
ON CONFLICT (name) DO NOTHING;

"""Load the generator's 4 CSV tables into Postgres with fast COPY.
Indexes are built one-by-one with separate commits to avoid OOM on 10M rows.
"""
import glob, os, time
from app.db import get_conn, put_conn, DATA_DIR

SCHEMA = """
DROP TABLE IF EXISTS orders, products, customers, tenants CASCADE;
CREATE TABLE tenants (
    tenant_id  INT  NOT NULL, name TEXT NOT NULL, created_at DATE NOT NULL);
CREATE TABLE customers (
    customer_id INT NOT NULL, tenant_id INT NOT NULL,
    name TEXT NOT NULL, email TEXT NOT NULL, created_at DATE NOT NULL);
CREATE TABLE products (
    product_id INT NOT NULL, tenant_id INT NOT NULL,
    name TEXT NOT NULL, category TEXT NOT NULL, price NUMERIC(12,2) NOT NULL);
CREATE TABLE orders (
    order_id    BIGINT        NOT NULL,
    tenant_id   INT           NOT NULL,
    customer_id INT           NOT NULL,
    product_id  INT           NOT NULL,
    quantity    INT           NOT NULL,
    unit_price  NUMERIC(12,2) NOT NULL,
    amount      NUMERIC(14,2) NOT NULL,
    status      TEXT          NOT NULL
        CHECK (status IN ('placed','shipped','cancelled')),
    created_at  TIMESTAMP     NOT NULL,
    updated_at  TIMESTAMP     NOT NULL
);
"""

DENORM = """
ALTER TABLE orders ADD COLUMN IF NOT EXISTS category     TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS product_name TEXT;
UPDATE orders o
SET    category     = p.category,
       product_name = p.name
FROM   products p
WHERE  p.product_id = o.product_id;
ALTER TABLE orders ALTER COLUMN category SET NOT NULL;
"""

# Split into separate statements — each gets its own commit to avoid OOM
STEPS = [
    ("primary keys",
     "ALTER TABLE tenants ADD PRIMARY KEY (tenant_id);"
     "ALTER TABLE customers ADD PRIMARY KEY (customer_id);"
     "ALTER TABLE products ADD PRIMARY KEY (product_id);"
     "ALTER TABLE orders ADD PRIMARY KEY (order_id);"),
    ("index 1/4: tenant+created_at",
     "CREATE INDEX idx_orders_tenant_created ON orders (tenant_id, created_at);"),
    ("index 2/4: updated_at",
     "CREATE INDEX idx_orders_updated_at ON orders (updated_at);"),
    ("index 3/4: tenant+product",
     "CREATE INDEX idx_orders_tenant_product ON orders (tenant_id, product_id);"),
    ("xid_snap: add sequence + column",
     "CREATE SEQUENCE IF NOT EXISTS xid_snap_seq;"
     "ALTER TABLE orders ADD COLUMN IF NOT EXISTS xid_snap BIGINT;"),
    ("xid_snap: backfill 10M rows (~5 min)",
     "UPDATE orders SET xid_snap = nextval('xid_snap_seq');"),
    ("index 4/4: xid_snap",
     "CREATE INDEX idx_orders_xid_snap ON orders (xid_snap);"),
    ("trigger: auto-fill xid_snap + category on live inserts",
     """CREATE OR REPLACE FUNCTION set_xid_snap() RETURNS trigger AS $$
BEGIN
  NEW.xid_snap := nextval('xid_snap_seq');
  IF NEW.category IS NULL OR NEW.product_name IS NULL THEN
    SELECT p.category, p.name INTO NEW.category, NEW.product_name
    FROM products p WHERE p.product_id = NEW.product_id;
  END IF;
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_xid_snap ON orders;
CREATE TRIGGER trg_xid_snap BEFORE INSERT OR UPDATE ON orders
  FOR EACH ROW EXECUTE FUNCTION set_xid_snap();"""),
    ("analyze", "ANALYZE;"),
]

def _copy(cur, table, path, columns=None):
    cols = f"({columns})" if columns else ""
    with open(path) as fh:
        cur.copy_expert(
            f"COPY {table}{cols} FROM STDIN WITH (FORMAT csv, HEADER true)", fh)

def main():
    order_files = sorted(glob.glob(os.path.join(DATA_DIR, "orders_t*.csv")))
    if not order_files:
        print(f"No orders_t*.csv in {DATA_DIR} — generate data first."); return
    c = get_conn()
    try:
        # 1. Schema + COPY (no indexes yet — fast load)
        with c.cursor() as cur:
            print("creating schema…")
            cur.execute(SCHEMA)
            for t in ("tenants", "customers", "products"):
                p = os.path.join(DATA_DIR, f"{t}.csv")
                if os.path.exists(p):
                    print(f"  COPY {t}.csv"); _copy(cur, t, p)
            order_cols = ("order_id,tenant_id,customer_id,product_id,quantity,"
                          "unit_price,amount,status,created_at,updated_at")
            for f in order_files:
                print(f"  COPY {os.path.basename(f)}"); _copy(cur, "orders", f, order_cols)
            print("denormalizing category…")
            cur.execute(DENORM)
        c.commit()

        # 2. Indexes one-by-one, each with its own commit
        for label, sql in STEPS:
            print(f"building {label}…")
            with c.cursor() as cur:
                cur.execute(sql)
            c.commit()

        with c.cursor() as cur:
            cur.execute("SELECT count(*) FROM orders")
            n = cur.fetchone()[0]
        print(f"loaded {n:,} orders from {len(order_files)} files")
    finally:
        put_conn(c)

if __name__ == "__main__":
    t0 = time.time(); main(); print(f"load time {time.time()-t0:.0f}s")
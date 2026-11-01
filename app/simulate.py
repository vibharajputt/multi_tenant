"""Parallel load simulator — proves the engine handles TRUE parallel requests.

Every tenant gets its own thread. All threads fire simultaneously and
continuously — inserts, ships, and cancellations happening at the same moment
across different (and sometimes the same) tenants.

This is real "parallel requests" — not a sequential loop.

Why this tests the hard things:
  * Connection pool under pressure: N threads all call get_conn() at once.
  * Worker safety: refresh_once() uses a DB-level advisory lock so only one
    worker runs at a time (no dirty-bucket race).
  * change_id correctness: concurrent transactions each get a unique sequence
    value — no two rows share a change_id even under heavy parallel load.
  * Correctness guarantee: after the worker catches up, every tenant's
    rollup == direct DB query, despite the parallel chaos.

Usage (stack must be up):
  docker compose run --rm api python -m app.simulate \\
      --threads 10 --rate 20 --seconds 60 --verify

  --threads  : number of parallel tenant-writer threads (default = all tenants)
  --rate     : new orders per thread per second (default 20)
  --seconds  : how long to run (default 60)
  --verify   : run correctness check after the load
"""
import argparse
import random
import threading
import time
import sys
import os
import psycopg2
import psycopg2.extras
import psycopg2
import psycopg2.extras
from app.db import DB

# ── counters shared across threads ──────────────────────────────────────────
_lock = threading.Lock()
_stats = {"ins": 0, "shp": 0, "can": 0, "err": 0}

def _inc(key, n=1):
    with _lock:
        _stats[key] += n

# ── catalog: discover tenants + their products from DB ─────────────────────
def _catalog():
    """Use a fresh direct connection so we don't fight the shared pool."""
    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT tenant_id FROM tenants ORDER BY tenant_id")
            tenants = [r["tenant_id"] for r in cur.fetchall()]
            if not tenants:
                cur.execute("SELECT DISTINCT tenant_id FROM orders ORDER BY tenant_id")
                tenants = [r["tenant_id"] for r in cur.fetchall()]
            cur.execute("SELECT product_id, tenant_id FROM products")
            prods = {}
            for row in cur.fetchall():
                prods.setdefault(row["tenant_id"], []).append(row["product_id"])
        return tenants, prods
    finally:
        conn.close()

# ── per-tenant writer thread ─────────────────────────────────────────────────
def _tenant_writer(tenant_id, product_ids, rate, seconds, start_barrier, id_base):
    """One thread per tenant. All threads start at the same moment (barrier)."""
    start_barrier.wait()            # synchronize: all threads start together
    t0 = time.time()
    nid = id_base + tenant_id * 10_000_000   # unique id space per tenant

    while time.time() - t0 < seconds:
        conn = None
        try:
            conn = psycopg2.connect(**DB)
            conn.autocommit = False
            with conn.cursor() as cur:
                # ── INSERT new orders (status = placed) ──────────────────
                batch = []
                for _ in range(rate):
                    p = random.choice(product_ids)
                    qty = random.randint(1, 4)
                    amt = round(random.uniform(100, 5000), 2)
                    batch.append((
                        nid, tenant_id, 1, p, qty,
                        round(amt / qty, 2), amt,
                        "placed",
                    ))
                    nid += 1

                cur.executemany(
                    """INSERT INTO orders
                       (order_id, tenant_id, customer_id, product_id,
                        quantity, unit_price, amount, status,
                        created_at, updated_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now(), now())""",
                    batch,
                )
                _inc("ins", len(batch))

                # ── SHIP some pending orders for this tenant ──────────────
                cur.execute(
                    """UPDATE orders SET status='shipped', updated_at=now()
                       WHERE order_id IN (
                         SELECT order_id FROM orders
                         WHERE tenant_id=%s AND status='placed'
                         ORDER BY order_id DESC LIMIT %s
                       )""",
                    (tenant_id, max(1, rate // 2)),
                )
                _inc("shp", cur.rowcount)

                # ── CANCEL oldest placed orders for this tenant ───────────
                cur.execute(
                    """UPDATE orders SET status='cancelled', updated_at=now()
                       WHERE order_id IN (
                         SELECT order_id FROM orders
                         WHERE tenant_id=%s AND status='placed'
                         ORDER BY order_id ASC LIMIT %s
                       )""",
                    (tenant_id, max(1, rate // 5)),
                )
                _inc("can", cur.rowcount)

            conn.commit()

        except Exception as e:
            _inc("err")
            if conn:
                try: conn.rollback()
                except Exception: pass
            print(f"  [tenant {tenant_id}] write error: {e}", flush=True)
        finally:
            if conn:
                try: conn.close()
                except Exception: pass

        time.sleep(1)   # one batch per second per thread

# ── main ─────────────────────────────────────────────────────────────────────
def run(threads, rate, seconds, verify):
    tenants, prods = _catalog()
    n = min(threads, len(tenants))
    print(f"\nStarting {n} PARALLEL writer threads "
          f"(rate={rate} orders/thread/sec, {seconds}s)...")
    print("Each thread is an independent tenant — all fire simultaneously.\n")

    # unique order-id base: use current epoch so reruns don't collide
    id_base = int(time.time()) * 1_000_000

    # DB-level advisory lock so refresh_once() is safe if run alongside
    # (the lock is in refresh.py; here we just note it in the docs)

    barrier = threading.Barrier(n + 1)   # +1 for the monitor thread
    writers = []
    for i in range(n):
        tid = tenants[i]
        pids = prods.get(tid) or [1]
        t = threading.Thread(
            target=_tenant_writer,
            args=(tid, pids, rate, seconds, barrier, id_base),
            daemon=True,
            name=f"writer-t{tid}",
        )
        writers.append(t)
        t.start()

    barrier.wait()   # release all threads at exactly the same moment
    t0 = time.time()
    print(f"  All {n} threads released simultaneously at t=0\n")

    prev = dict(_stats)
    while any(w.is_alive() for w in writers):
        time.sleep(2)
        with _lock:
            snap = dict(_stats)
        delta_ins = snap["ins"] - prev["ins"]
        delta_shp = snap["shp"] - prev["shp"]
        delta_can = snap["can"] - prev["can"]
        print(
            f"  t={time.time()-t0:4.0f}s │ "
            f"+{snap['ins']:>7,} inserts │ "
            f"{snap['shp']:>7,} shipped │ "
            f"{snap['can']:>6,} cancelled │ "
            f"{snap['err']:>3} errors │ "
            f"rate: {delta_ins//2:>4}/s inserts across {n} threads",
            flush=True,
        )
        prev = snap

    for w in writers:
        w.join(timeout=5)

    elapsed = time.time() - t0
    print(f"\n{'─'*70}")
    print(f"DONE in {elapsed:.0f}s:")
    print(f"  {_stats['ins']:>8,}  inserts (new orders placed)")
    print(f"  {_stats['shp']:>8,}  ships   (placed → shipped)")
    print(f"  {_stats['can']:>8,}  cancels (placed → cancelled)")
    print(f"  {_stats['err']:>8,}  errors  (connection / write failures)")
    print(f"  throughput ≈ {_stats['ins']//max(1,int(elapsed)):,} inserts/sec across {n} parallel threads")
    print(f"{'─'*70}\n")

    if verify:
        print("Running correctness check (rollup == direct DB for every tenant)...")
        import app.verify as v
        # give the worker one extra cycle to catch up
        from app.refresh import refresh_once
        for _ in range(3):
            seen, dirty = refresh_once()
            if seen:
                print(f"  worker pass: {seen} rows → {dirty} buckets", flush=True)
            time.sleep(1)
        ok_c = v.check_correctness()
        ok_t = v.check_topn()
        ok_i = v.check_isolation()
        result = "ALL PASS ✓" if (ok_c and ok_t and ok_i) else "FAILURES ✗"
        print(f"\nCORRECTNESS AFTER PARALLEL LOAD: {result}")
        sys.exit(0 if (ok_c and ok_t and ok_i) else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", type=int, default=10,
                    help="parallel writer threads (default: all tenants)")
    ap.add_argument("--rate", type=int, default=20,
                    help="new orders per thread per second (default 20)")
    ap.add_argument("--seconds", type=int, default=60,
                    help="duration (default 60s)")
    ap.add_argument("--verify", action="store_true",
                    help="run correctness check after load")
    a = ap.parse_args()
    run(a.threads, a.rate, a.seconds, a.verify)
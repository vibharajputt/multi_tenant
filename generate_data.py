#!/usr/bin/env python3
"""
PRD-aligned synthetic data for the multi-tenant aggregation layer.

Presets (from the PRD):
  --preset small   1,000,000 orders, 10 tenants
  --preset main   10,000,000 orders, 100 tenants

Tables written as CSV: tenants, customers, products, orders.
Order status is one of: placed, shipped, cancelled.
Tenant sizes follow a Zipf curve (a few huge tenants, a long tail of small ones).
Orders fluctuate over 2 years: growth/decline, weekly pattern, seasonality,
sale events, flash spikes, outages, daily noise, hourly curve.
updated_at lags created_at (placed -> shipped/cancelled later), so incremental
refresh by updated_at works. Deterministic for a given --seed.

  python generate_data.py --preset small --out ./out_small
  python generate_data.py --preset main  --out ./out_main --workers 8
"""
import argparse, os, time
from multiprocessing import Pool
import numpy as np
import pandas as pd

START = np.datetime64("2024-11-01", "D")
N_DAYS = 730                                    # ends 2026-10-31
NOW = np.datetime64("2026-10-31T12:00:00", "s")
PRESETS = {"small": (1_000_000, 10), "main": (10_000_000, 100)}
ZIPF_S = 1.0
PART_ROWS = 1_000_000

CATEGORIES = [("Electronics", 4200), ("Fashion", 1100), ("Home", 1600), ("Grocery", 350),
              ("Beauty", 650), ("Sports", 1400), ("Toys", 500), ("Books", 380)]
WEEKLY = np.array([0.95, 0.92, 0.95, 1.00, 1.10, 1.25, 1.20])
HOUR_P = np.array([1, .6, .4, .3, .3, .5, 1, 2, 3, 4, 4.5, 5,
                   5.5, 5, 4.5, 4.5, 5, 6, 7, 8, 8.5, 7, 4, 2], dtype=float)
HOUR_P /= HOUR_P.sum()
EVENTS = [("2024-11-29", "2024-12-01", 3.0), ("2024-12-18", "2024-12-24", 1.8),
          ("2025-01-24", "2025-01-27", 2.2), ("2025-05-09", "2025-05-12", 1.9),
          ("2025-08-13", "2025-08-16", 1.8), ("2025-10-16", "2025-10-22", 3.2),
          ("2025-11-28", "2025-11-30", 3.2), ("2025-12-18", "2025-12-24", 1.9),
          ("2026-01-24", "2026-01-27", 2.4), ("2026-05-08", "2026-05-11", 2.0),
          ("2026-08-13", "2026-08-16", 1.9), ("2026-10-24", "2026-10-31", 2.6)]


def sizes(n_tenants, rows):
    w = 1.0 / np.arange(1, n_tenants + 1) ** ZIPF_S
    w /= w.sum()
    rows_t = np.floor(w * rows).astype(np.int64)
    rows_t[0] += rows - rows_t.sum()
    n_prod = np.clip((40 + 260 * (w / w[0]) ** 0.5).astype(int), 40, 300)
    n_cust = np.maximum(200, rows_t // 12)
    return rows_t, n_prod, n_cust


def day_profile(t):
    rng = np.random.default_rng(1000 + t)
    growth, weekend = rng.uniform(-0.5, 2.0), rng.uniform(0.7, 1.4)
    sens, phase = rng.uniform(0.6, 3.0), rng.uniform(0, 2 * np.pi)
    d = np.arange(N_DAYS)
    trend = np.maximum(1 + growth * d / N_DAYS, 0.15)
    dow = ((START + d).astype(int) + 3) % 7
    weekly = WEEKLY[dow].copy()
    weekly[dow >= 5] *= weekend
    season = 1 + 0.25 * np.sin(2 * np.pi * d / 365.25 + phase)
    ev = np.ones(N_DAYS)
    for s, e, m in EVENTS:
        i0, i1 = int((np.datetime64(s) - START).astype(int)), int((np.datetime64(e) - START).astype(int))
        ev[max(i0, 0):max(i1 + 1, 0)] *= 1 + (m - 1) * sens / 1.5
    flash = np.ones(N_DAYS)
    for x in rng.choice(N_DAYS, 3, replace=False):
        flash[x] *= rng.uniform(2.5, 5.0)
    for x in rng.choice(N_DAYS, 2, replace=False):
        flash[x] *= rng.uniform(0.02, 0.2)
    p = trend * weekly * season * ev * flash * rng.lognormal(0, 0.18, N_DAYS)
    return p / p.sum()


def product_table(t, n_prod):
    rng = np.random.default_rng(3000 + t)
    pref = rng.dirichlet(np.ones(len(CATEGORIES)) * 2)
    cat = rng.choice(len(CATEGORIES), n_prod, p=pref)
    base = np.array([c[1] for c in CATEGORIES], dtype=float)
    price = np.maximum(np.round(base[cat] * rng.lognormal(0, 0.5, n_prod), 2), 1.0)
    names = np.array([c[0] for c in CATEGORIES])[cat]
    return cat, names, price


def make_part(a):
    t, part, n, out, order_start, prod_start, n_prod, cust_start, n_cust, seed = a
    rng = np.random.default_rng(seed + t * 100003 + part)
    day = rng.choice(N_DAYS, size=n, p=day_profile(t))
    sec = rng.choice(24, size=n, p=HOUR_P) * 3600 + rng.integers(0, 3600, n)
    created = (START + day).astype("datetime64[s]") + sec.astype("timedelta64[s]")
    created = np.minimum(created, NOW - np.timedelta64(60, "s"))

    _, _, price = product_table(t, n_prod)
    rank = np.minimum((n_prod * rng.random(n) ** 2.2).astype(int), n_prod - 1)
    qty = np.minimum(1 + rng.poisson(0.5, n), 10)
    amount = np.round(price[rank] * qty, 2)
    cust = cust_start + np.minimum((n_cust * rng.random(n) ** 2.5).astype(int), n_cust - 1)

    age = (NOW - created) / np.timedelta64(1, "D")
    p_can = np.where(age < 2, 0.04, np.where(age < 7, 0.05, 0.09))
    p_pla = np.where(age < 2, 0.60, np.where(age < 7, 0.15, 0.03))
    u = rng.random(n)
    can, pla = u < p_can, (u >= p_can) & (u < p_can + p_pla)
    status = np.where(can, "cancelled", np.where(pla, "placed", "shipped"))
    delay = np.where(can, rng.integers(60, 12 * 3600, n),
                     np.where(pla, 0, rng.integers(3600, 72 * 3600, n)))
    updated = np.minimum(created + delay.astype("timedelta64[s]"), NOW)

    df = pd.DataFrame({
        "order_id": order_start + np.arange(n), "tenant_id": t + 1,
        "customer_id": cust, "product_id": prod_start + rank,
        "quantity": qty, "unit_price": price[rank], "amount": amount,
        "status": status, "created_at": created.astype(str), "updated_at": updated.astype(str)})
    path = os.path.join(out, f"orders_t{t + 1:03d}_p{part:03d}.csv")
    df.to_csv(path, index=False)
    return t, n, os.path.getsize(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preset", choices=PRESETS, default="small")
    ap.add_argument("--rows", type=int)
    ap.add_argument("--tenants", type=int)
    ap.add_argument("--out", default="./out")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    rows, n_t = PRESETS[a.preset]
    rows, n_t = a.rows or rows, a.tenants or n_t
    os.makedirs(a.out, exist_ok=True)
    t0 = time.time()

    rows_t, n_prod, n_cust = sizes(n_t, rows)
    prod_start = np.concatenate([[1], 1 + np.cumsum(n_prod)[:-1]])
    cust_start = np.concatenate([[1], 1 + np.cumsum(n_cust)[:-1]])

    pd.DataFrame({"tenant_id": np.arange(1, n_t + 1),
                  "name": [f"Store {i:03d}" for i in range(1, n_t + 1)],
                  "created_at": str(START)}).to_csv(f"{a.out}/tenants.csv", index=False)

    prods, custs = [], []
    for t in range(n_t):
        cat, cname, price = product_table(t, n_prod[t])
        ids = prod_start[t] + np.arange(n_prod[t])
        prods.append(pd.DataFrame({"product_id": ids, "tenant_id": t + 1,
                     "name": np.char.add(np.char.add(cname, " item "), (np.arange(n_prod[t]) + 1).astype(str)),
                     "category": cname, "price": price}))
        cid = cust_start[t] + np.arange(n_cust[t])
        loc = np.arange(n_cust[t]) + 1
        custs.append(pd.DataFrame({"customer_id": cid, "tenant_id": t + 1,
                     "name": np.char.add("Customer ", loc.astype(str)),
                     "email": np.char.add(np.char.add("customer", loc.astype(str)), f"@store{t + 1}.example"),
                     "created_at": str(START)}))
    pd.concat(prods).to_csv(f"{a.out}/products.csv", index=False)
    pd.concat(custs).to_csv(f"{a.out}/customers.csv", index=False)

    jobs, nxt = [], 1
    for t in range(n_t):
        off, part = 0, 0
        while off < rows_t[t]:
            n = int(min(PART_ROWS, rows_t[t] - off))
            jobs.append((t, part, n, a.out, nxt, int(prod_start[t]), int(n_prod[t]),
                         int(cust_start[t]), int(n_cust[t]), a.seed))
            nxt += n; off += n; part += 1

    print(f"{n_t} tenants, {rows:,} orders, {len(jobs)} order files, {a.workers} workers")
    cnt, size = np.zeros(n_t, dtype=np.int64), np.zeros(n_t, dtype=np.int64)
    with Pool(a.workers) as pool:
        for i, (t, n, b) in enumerate(pool.imap_unordered(make_part, jobs), 1):
            cnt[t] += n; size[t] += b
            if i % 10 == 0 or i == len(jobs):
                print(f"  {i}/{len(jobs)} ({time.time() - t0:.0f}s)", flush=True)

    print("\nBiggest tenants:  ", ", ".join(f"t{i + 1}={cnt[i]:,}" for i in range(min(5, n_t))))
    print("Smallest tenants: ", ", ".join(f"t{i + 1}={cnt[i]:,}" for i in range(max(n_t - 3, 5), n_t)))
    tot = sum(os.path.getsize(os.path.join(a.out, f)) for f in os.listdir(a.out))
    print(f"TOTAL orders {cnt.sum():,}, products {n_prod.sum():,}, customers {n_cust.sum():,}")
    print(f"Disk: {tot / 1e9:.2f} GB in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()

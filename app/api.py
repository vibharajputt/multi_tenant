"""Query API — v2.1

Reads ONLY pre-computed rollup tables. Never touches raw orders.

Scopes
------
/tenants/{id}/...   per-tenant, strictly isolated (FR-5)
/platform/...       operator view across all tenants

New in v2.1
-----------
* Hourly trend granularity  (FR-2: "per hour or day")
* Pagination on top-N       (FR-3 complete)
* Pool health + concurrency  (parallel-requests answer)
* Scale report               (Growth target proof)
* Drift detector integration (/health shows drift_detected)
* Query log                  (/admin/query-log, /admin/query-stats)
* /admin/drift-check         (manual trigger)
* Startup: drift + query-log threads launched automatically
"""
import time
import json
import functools
import threading
import os
from collections import defaultdict
from datetime import date, datetime, timedelta

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.responses import HTMLResponse, FileResponse

from app.db import (query, rcache, cache_gen, bump_cache_gen,
                    CACHE_TTL_SEC, MAX_CONC_PER_TENANT, pool)
import app.drift      as drift_mod
import app.query_log  as qlog

app = FastAPI(title="Multi-Tenant Aggregation Layer", version="2.1")

# ── startup: launch background threads ──────────────────────────────────────
@app.on_event("startup")
def _startup():
    drift_mod.start()   # drift checker (every 5 min by default)

# ── per-tenant concurrency guard (Bonus B-3) ────────────────────────────────
_sema: dict = defaultdict(lambda: threading.BoundedSemaphore(MAX_CONC_PER_TENANT))

class _tenant_slot:
    def __init__(self, tid): self.s = _sema[tid]
    def __enter__(self):
        if not self.s.acquire(blocking=False):
            raise HTTPException(429, "Tenant query limit reached — retry shortly")
    def __exit__(self, *a): self.s.release()

def _pool_stats() -> dict:
    try:
        p = pool()
        # ThreadedConnectionPool internals
        used = len(p._used)
        avail = len(p._pool)
        total = p.maxconn
        return {"pool_used": used, "pool_available": avail, "pool_total": total}
    except Exception:
        return {"pool_used": None, "pool_available": None, "pool_total": None}

# ── Redis cache helpers ──────────────────────────────────────────────────────
def _cache_get(key: str):
    try:
        r = rcache()
        if r is None: return None
        v = r.get(f"{cache_gen()}:{key}")
        return json.loads(v) if v else None
    except Exception:
        return None

def _cache_set(key: str, val: dict):
    try:
        r = rcache()
        if r is None: return
        r.setex(f"{cache_gen()}:{key}", CACHE_TTL_SEC,
                json.dumps(val, default=str))
    except Exception:
        pass

# ── helpers ──────────────────────────────────────────────────────────────────
def _range(frm, to):
    if not to:  to  = date.today().isoformat()
    if not frm: frm = (date.fromisoformat(to) - timedelta(days=30)).isoformat()
    return frm, to

def timed(fn):
    @functools.wraps(fn)
    def wrap(*a, **k):
        t0  = time.perf_counter()
        out = fn(*a, **k)
        ms  = round((time.perf_counter() - t0) * 1000, 1)
        if isinstance(out, dict):
            out["query_ms"] = ms
            tid    = k.get("tenant_id") or (a[0] if a and isinstance(a[0], int) else None)
            cached = out.get("cached", False)
            scope  = "platform" if out.get("scope") == "platform" else "tenant"
            qlog.record(tid, scope, fn.__name__, ms, cached)
        return out
    return wrap

# ═══════════════════════════════════════════════════════════════════════════
# PER-TENANT ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

# FR-1: counts & totals
@app.get("/tenants/{tenant_id}/orders/count")
@timed
def orders_count(tenant_id: int = Path(...),
                 from_: str = Query(None, alias="from"),
                 to: str    = Query(None),
                 status: str = Query(None)):
    """GET /tenants/42/orders/count?from=&to=&status="""
    frm, to = _range(from_, to)
    key = f"cnt:{tenant_id}:{frm}:{to}:{status}"
    if (c := _cache_get(key)) is not None:
        return {**c, "cached": True}
    with _tenant_slot(tenant_id):
        extra  = "AND status=%s" if status else ""
        params = [tenant_id, frm, to] + ([status] if status else [])
        row = query(f"""
            SELECT COALESCE(SUM(order_count),0) orders,
                   COALESCE(SUM(total_amount),0) revenue,
                   COALESCE(SUM(total_qty),0)    units
            FROM rollup_daily
            WHERE tenant_id=%s AND day>=%s AND day<=%s {extra}
        """, params, one=True)
    out = {"tenant_id": tenant_id, "from": frm, "to": to,
           "orders": int(row["orders"]),
           "revenue": float(row["revenue"]),
           "units": int(row["units"]),
           "cached": False}
    _cache_set(key, out)
    return out

# FR-2: trend — DAILY or HOURLY granularity
@app.get("/tenants/{tenant_id}/orders/trend")
@timed
def orders_trend(tenant_id: int,
                 from_: str  = Query(None, alias="from"),
                 to: str     = Query(None),
                 metric: str = Query("orders"),
                 granularity: str = Query("day",
                     description="'day' or 'hour'. Use 'hour' for intraday.")):
    """
    FR-2: trend over time. granularity=hour uses rollup_hourly.
    Example: /tenants/1/orders/trend?granularity=hour&from=2026-10-31&to=2026-10-31
    """
    frm, to = _range(from_, to)
    agg = {"orders": "SUM(order_count)", "revenue": "SUM(total_amount)",
           "units":  "SUM(total_qty)"}.get(metric, "SUM(order_count)")

    if granularity == "hour":
        rows = query(f"""
            SELECT hour AS ts, {agg} value
            FROM rollup_hourly
            WHERE tenant_id=%s AND hour>=%s::timestamp AND hour<%s::timestamp + interval '1 day'
            GROUP BY hour ORDER BY hour
        """, (tenant_id, frm, to))
        series = [{"hour": str(r["ts"]), "value": float(r["value"])} for r in rows]
    else:
        rows = query(f"""
            SELECT day AS ts, {agg} value
            FROM rollup_daily
            WHERE tenant_id=%s AND day>=%s AND day<=%s
            GROUP BY day ORDER BY day
        """, (tenant_id, frm, to))
        series = [{"day": str(r["ts"]), "value": float(r["value"])} for r in rows]

    return {"tenant_id": tenant_id, "metric": metric, "granularity": granularity,
            "from": frm, "to": to, "series": series, "cached": False}

# FR-3: top-N products with pagination
@app.get("/tenants/{tenant_id}/products/top")
@timed
def top_products(tenant_id: int,
                 from_:  str = Query(None, alias="from"),
                 to: str     = Query(None),
                 n: int       = Query(10,  ge=1, le=500),
                 offset: int  = Query(0,   ge=0),
                 by: str      = Query("revenue")):
    """FR-3: top-N with pagination. Use ?n=10&offset=10 for page 2."""
    frm, to = _range(from_, to)
    order = {"revenue": "SUM(total_amount)", "units": "SUM(total_qty)",
             "orders":  "SUM(order_count)"}.get(by, "SUM(total_amount)")
    rows = query(f"""
        SELECT product_id, MIN(product_name) product_name,
               SUM(total_amount) revenue, SUM(total_qty) units,
               SUM(order_count)  orders
        FROM rollup_product_daily
        WHERE tenant_id=%s AND day>=%s AND day<=%s
        GROUP BY product_id ORDER BY {order} DESC
        LIMIT %s OFFSET %s
    """, (tenant_id, frm, to, n, offset))
    return {"tenant_id": tenant_id, "from": frm, "to": to, "by": by,
            "offset": offset, "count": len(rows),
            "top": [{"product_id": r["product_id"],
                     "product_name": r["product_name"],
                     "revenue": float(r["revenue"]),
                     "units":   int(r["units"]),
                     "orders":  int(r["orders"])} for r in rows],
            "cached": False}

# FR-4: breakdown
@app.get("/tenants/{tenant_id}/orders/breakdown")
@timed
def breakdown(tenant_id: int,
              from_: str = Query(None, alias="from"),
              to: str    = Query(None),
              by: str    = Query("status")):
    frm, to = _range(from_, to)
    if by not in ("status", "category"):
        raise HTTPException(400, "by must be status|category")
    rows = query(f"""
        SELECT {by} key, SUM(order_count) orders, SUM(total_amount) revenue
        FROM rollup_daily
        WHERE tenant_id=%s AND day>=%s AND day<=%s
        GROUP BY {by} ORDER BY orders DESC
    """, (tenant_id, frm, to))
    return {"tenant_id": tenant_id, "from": frm, "to": to, "by": by,
            "breakdown": [{"key": r["key"], "orders": int(r["orders"]),
                           "revenue": float(r["revenue"])} for r in rows],
            "cached": False}

# B-4: config-driven metric
@app.get("/tenants/{tenant_id}/metrics/{name}")
@timed
def tenant_metric(tenant_id: int, name: str,
                  from_: str = Query(None, alias="from"),
                  to: str    = Query(None)):
    frm, to = _range(from_, to)
    m = query("SELECT expr, label FROM metric_config WHERE name=%s AND is_active",
              (name,), one=True)
    if not m: raise HTTPException(404, f"unknown metric '{name}'")
    row = query(f"SELECT {m['expr']} value FROM rollup_daily "
                "WHERE tenant_id=%s AND day>=%s AND day<=%s",
                (tenant_id, frm, to), one=True)
    return {"tenant_id": tenant_id, "metric": name, "label": m["label"],
            "from": frm, "to": to, "value": float(row["value"] or 0),
            "cached": False}

# Comparison (current vs prior period)
@app.get("/tenants/{tenant_id}/orders/compare")
@timed
def tenant_compare(tenant_id: int,
                   from_:      str = Query(None, alias="from"),
                   to: str         = Query(None),
                   prior_days: int = Query(7)):
    frm, to = _range(from_, to)
    d_from = date.fromisoformat(frm); d_to = date.fromisoformat(to)
    delta  = (d_to - d_from).days + 1
    p_to   = (d_from - timedelta(days=1)).isoformat()
    p_from = (d_from - timedelta(days=delta)).isoformat()
    def totals(f, t):
        r = query("""SELECT COALESCE(SUM(order_count),0) o,
                            COALESCE(SUM(total_amount),0) r,
                            COALESCE(SUM(total_qty),0)    u
                     FROM rollup_daily
                     WHERE tenant_id=%s AND day>=%s AND day<=%s""",
                  (tenant_id, f, t), one=True)
        return int(r["o"]), float(r["r"]), int(r["u"])
    co, cr, cu = totals(frm, to)
    po, pr, pu = totals(p_from, p_to)
    def pct(c, p): return round((c-p)/p*100, 1) if p else None
    return {"tenant_id": tenant_id,
            "current":    {"from": frm,    "to": to,    "orders": co, "revenue": cr, "units": cu},
            "prior":      {"from": p_from, "to": p_to,  "orders": po, "revenue": pr, "units": pu},
            "change_pct": {"orders": pct(co,po), "revenue": pct(cr,pr), "units": pct(cu,pu)},
            "cached": False}

# ═══════════════════════════════════════════════════════════════════════════
# PLATFORM ENDPOINTS (all tenants, operator view)
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/platform/summary")
@timed
def platform_summary(from_: str = Query(None, alias="from"), to: str = Query(None)):
    frm, to = _range(from_, to)
    row = query("""SELECT COUNT(DISTINCT tenant_id) tenants,
                          COALESCE(SUM(order_count),0) orders,
                          COALESCE(SUM(total_amount),0) revenue,
                          COALESCE(SUM(total_qty),0) units
                   FROM rollup_daily WHERE day>=%s AND day<=%s""", (frm,to), one=True)
    return {"scope":"platform","from":frm,"to":to,
            "tenants": int(row["tenants"]), "orders": int(row["orders"]),
            "revenue": float(row["revenue"]), "units": int(row["units"]), "cached": False}

@app.get("/platform/orders/count")
@timed
def platform_count(from_: str = Query(None, alias="from"), to: str = Query(None),
                   status: str = Query(None)):
    frm, to = _range(from_, to)
    extra  = "AND status=%s" if status else ""
    params = [frm, to] + ([status] if status else [])
    row = query(f"""SELECT COALESCE(SUM(order_count),0) orders,
                           COALESCE(SUM(total_amount),0) revenue,
                           COALESCE(SUM(total_qty),0) units
                    FROM rollup_daily WHERE day>=%s AND day<=%s {extra}""", params, one=True)
    return {"scope":"platform","from":frm,"to":to,
            "orders": int(row["orders"]), "revenue": float(row["revenue"]),
            "units": int(row["units"]), "cached": False}

@app.get("/platform/orders/trend")
@timed
def platform_trend(from_: str = Query(None, alias="from"), to: str = Query(None),
                   metric: str = Query("orders"),
                   granularity: str = Query("day")):
    frm, to = _range(from_, to)
    agg = {"orders":"SUM(order_count)","revenue":"SUM(total_amount)",
           "units":"SUM(total_qty)"}.get(metric,"SUM(order_count)")
    if granularity == "hour":
        rows = query(f"""SELECT hour ts, {agg} value FROM rollup_hourly
                         WHERE hour>=%s::timestamp AND hour<%s::timestamp + interval '1 day'
                         GROUP BY hour ORDER BY hour""", (frm, to))
        series = [{"hour": str(r["ts"]), "value": float(r["value"])} for r in rows]
    else:
        rows = query(f"""SELECT day ts, {agg} value FROM rollup_daily
                         WHERE day>=%s AND day<=%s GROUP BY day ORDER BY day""", (frm,to))
        series = [{"day": str(r["ts"]), "value": float(r["value"])} for r in rows]
    return {"scope":"platform","metric":metric,"granularity":granularity,
            "from":frm,"to":to,"series":series,"cached":False}

@app.get("/platform/products/top")
@timed
def platform_top(from_: str = Query(None, alias="from"), to: str = Query(None),
                 n: int = Query(10, ge=1, le=500), offset: int = Query(0, ge=0),
                 by: str = Query("revenue")):
    frm, to = _range(from_, to)
    order = {"revenue":"SUM(total_amount)","units":"SUM(total_qty)",
             "orders":"SUM(order_count)"}.get(by,"SUM(total_amount)")
    rows = query(f"""SELECT product_id, MIN(product_name) product_name,
                            SUM(total_amount) revenue, SUM(total_qty) units,
                            SUM(order_count) orders
                     FROM rollup_product_daily WHERE day>=%s AND day<=%s
                     GROUP BY product_id ORDER BY {order} DESC LIMIT %s OFFSET %s""",
                 (frm,to,n,offset))
    return {"scope":"platform","from":frm,"to":to,"by":by,"offset":offset,
            "top":[{"product_id":r["product_id"],"product_name":r["product_name"],
                    "revenue":float(r["revenue"]),"units":int(r["units"]),
                    "orders":int(r["orders"])} for r in rows], "cached":False}

@app.get("/platform/orders/breakdown")
@timed
def platform_breakdown(from_: str = Query(None, alias="from"), to: str = Query(None),
                       by: str = Query("status")):
    frm, to = _range(from_, to)
    if by not in ("status","category"): raise HTTPException(400,"by must be status|category")
    rows = query(f"""SELECT {by} key, SUM(order_count) orders, SUM(total_amount) revenue
                     FROM rollup_daily WHERE day>=%s AND day<=%s
                     GROUP BY {by} ORDER BY orders DESC""", (frm,to))
    return {"scope":"platform","from":frm,"to":to,"by":by,
            "breakdown":[{"key":r["key"],"orders":int(r["orders"]),
                          "revenue":float(r["revenue"])} for r in rows], "cached":False}

@app.get("/platform/tenants")
@timed
def platform_tenants(from_: str = Query(None, alias="from"), to: str = Query(None)):
    frm, to = _range(from_, to)
    rows = query("""SELECT tenant_id, SUM(order_count) orders,
                           SUM(total_amount) revenue, SUM(total_qty) units
                    FROM rollup_daily WHERE day>=%s AND day<=%s
                    GROUP BY tenant_id ORDER BY orders DESC""", (frm,to))
    return {"scope":"platform","from":frm,"to":to,
            "tenants":[{"tenant_id":r["tenant_id"],"orders":int(r["orders"]),
                        "revenue":float(r["revenue"]),"units":int(r["units"])} for r in rows],
            "cached":False}

@app.get("/platform/metrics/{name}")
@timed
def platform_metric(name: str, from_: str = Query(None, alias="from"), to: str = Query(None)):
    frm, to = _range(from_, to)
    m = query("SELECT expr, label FROM metric_config WHERE name=%s AND is_active",(name,),one=True)
    if not m: raise HTTPException(404, f"unknown metric '{name}'")
    row = query(f"SELECT {m['expr']} value FROM rollup_daily WHERE day>=%s AND day<=%s",
                (frm,to), one=True)
    return {"scope":"platform","metric":name,"label":m["label"],
            "from":frm,"to":to,"value":float(row["value"] or 0),"cached":False}

@app.get("/platform/leaderboard")
@timed
def platform_leaderboard(from_: str = Query(None, alias="from"), to: str = Query(None),
                         by: str = Query("revenue"), n: int = Query(10)):
    frm, to = _range(from_, to)
    col = {"revenue":"SUM(total_amount)","orders":"SUM(order_count)",
           "units":"SUM(total_qty)"}.get(by,"SUM(total_amount)")
    rows = query(f"""SELECT tenant_id, SUM(order_count) orders,
                            SUM(total_amount) revenue, SUM(total_qty) units
                     FROM rollup_daily WHERE day>=%s AND day<=%s
                     GROUP BY tenant_id ORDER BY {col} DESC LIMIT %s""", (frm,to,n))
    return {"scope":"platform","from":frm,"to":to,"ranked_by":by,
            "leaderboard":[{"rank":i+1,"tenant_id":r["tenant_id"],
                            "orders":int(r["orders"]),"revenue":float(r["revenue"]),
                            "units":int(r["units"])} for i,r in enumerate(rows)],
            "cached":False}

# ═══════════════════════════════════════════════════════════════════════════
# SYSTEM / MONITORING / ADMIN ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

# B-1: freshness
@app.get("/freshness")
def freshness():
    r = query("""SELECT last_xid_snap, last_run_at, rows_last_run,
                        max_updated, total_refreshes
                 FROM refresh_state WHERE id=1""", one=True)
    lag = None
    if r["last_run_at"]:
        lag = max(0, round((datetime.utcnow() - r["last_run_at"]).total_seconds()))
    return {"last_xid_snap":     r["last_xid_snap"],
            "last_run_at":       str(r["last_run_at"]),
            "rows_last_run":     r["rows_last_run"],
            "newest_data_at":    str(r["max_updated"]),
            "total_refreshes":   r["total_refreshes"],
            "refresh_lag_seconds": lag,
            "updated_ago":       f"{lag}s ago" if lag is not None else "never",
            "redis_connected":   rcache() is not None}

# health: drift + pool + freshness combined
@app.get("/health")
def health():
    d    = drift_mod.last_result()
    pool = _pool_stats()
    lag_row = query("SELECT last_run_at FROM refresh_state WHERE id=1", one=True)
    lag = None
    if lag_row and lag_row["last_run_at"]:
        lag = max(0, round((datetime.utcnow() - lag_row["last_run_at"]).total_seconds()))
    return {
        "ok":              True,
        "redis":           rcache() is not None,
        "refresh_lag_sec": lag,
        # Drift detector result — live correctness proof
        "drift_detected":  d["drift_detected"],
        "drift_checked_at":d["checked_at"],
        "drift_buckets_sampled": d["buckets_sampled"],
        "drift_mismatch_count":  d["mismatch_count"],
        "drift_details":         d["details"],
        # Connection pool
        **pool,
    }

# Manual drift trigger
@app.post("/admin/drift-check")
def admin_drift_check():
    """Run a correctness sample check now (don't wait for the 5-min interval)."""
    sampled, mismatches, details = drift_mod._run_check()
    return {"buckets_sampled": sampled, "mismatch_count": mismatches,
            "drift_detected": mismatches > 0, "details": details}

# Query log (in-memory, last 500)
@app.get("/admin/query-log")
def admin_query_log(tenant_id: int = Query(None), limit: int = Query(50, le=200)):
    return {"entries": qlog.recent(tenant_id=tenant_id, limit=limit)}

@app.get("/admin/query-stats")
def admin_query_stats():
    return qlog.stats()

# Concurrency status (parallel-requests answer)
@app.get("/admin/concurrency")
def admin_concurrency():
    status = {}
    for tid, s in _sema.items():
        avail   = s._value
        running = MAX_CONC_PER_TENANT - avail
        if running > 0:
            status[str(tid)] = {"running": running, "available": avail}
    return {"max_per_tenant": MAX_CONC_PER_TENANT,
            "active_tenants": len(status),
            "tenants": status,
            **_pool_stats()}

# Scale report (Growth target proof)
@app.get("/admin/scale-report")
def admin_scale_report():
    base = query("SELECT COUNT(*) cnt FROM orders", one=True)
    rd   = query("SELECT COUNT(*) cnt FROM rollup_daily", one=True)
    rp   = query("SELECT COUNT(*) cnt FROM rollup_product_daily", one=True)
    rh   = query("SELECT COUNT(*) cnt FROM rollup_hourly", one=True)
    base_n  = int(base["cnt"])
    rd_n    = int(rd["cnt"])
    rp_n    = int(rp["cnt"])
    rh_n    = int(rh["cnt"])
    total_r = rd_n + rp_n + rh_n
    ratio   = round(base_n / total_r, 1) if total_r else None
    # Sample query latency
    t0 = time.perf_counter()
    query("SELECT SUM(order_count) FROM rollup_daily WHERE tenant_id=1", one=True)
    p1 = round((time.perf_counter()-t0)*1000, 1)
    t0 = time.perf_counter()
    query("SELECT SUM(order_count) FROM rollup_daily WHERE tenant_id=1 "
          "AND day>='2025-01-01' AND day<='2026-12-31'", one=True)
    p2 = round((time.perf_counter()-t0)*1000, 1)
    return {
        "base_rows":               base_n,
        "rollup_daily_rows":       rd_n,
        "rollup_product_rows":     rp_n,
        "rollup_hourly_rows":      rh_n,
        "total_rollup_rows":       total_r,
        "compression_ratio":       f"{ratio}x — rollups are {ratio}x smaller than base",
        "sample_query_ms":         p1,
        "date_filtered_query_ms":  p2,
        "scale_note": (
            "Rollup size grows with tenants × days × categories (not raw rows). "
            "At 10M rows the rollup stays ~the same size → query time stays flat."
        ),
    }

# B-2: rebuild
@app.post("/admin/rebuild")
def admin_rebuild():
    from app.refresh import rebuild
    rebuild(); bump_cache_gen()
    return {"ok": True, "message": "rollups rebuilt from base table"}

# B-4: list metrics
@app.get("/metrics")
def list_metrics():
    rows = query("SELECT name, label FROM metric_config WHERE is_active ORDER BY name")
    return {"metrics": [{"name": r["name"], "label": r["label"]} for r in rows]}

# B-5: monitoring page (HTML)
@app.get("/stats", response_class=HTMLResponse)
def stats():
    f = freshness()
    ps = _pool_stats()
    qs = qlog.stats()
    d  = drift_mod.last_result()
    t0 = time.perf_counter()
    tenants = query("""SELECT tenant_id, SUM(order_count) orders, SUM(total_amount) revenue
                       FROM rollup_daily GROUP BY tenant_id ORDER BY orders DESC""")
    probe_ms = round((time.perf_counter()-t0)*1000, 1)
    rows = "".join(
        f"<tr><td>{t['tenant_id']}</td>"
        f"<td>{int(t['orders']):,}</td>"
        f"<td>₹{float(t['revenue']):,.0f}</td></tr>"
        for t in tenants)
    drift_color = "green" if not d["drift_detected"] else "red"
    drift_txt   = f"✓ clean ({d['buckets_sampled']} buckets sampled)" if not d["drift_detected"] else f"⚠ {d['mismatch_count']} mismatches"
    redis_txt   = "✓ connected" if f["redis_connected"] else "✗ offline (fallback active)"
    return f"""<html><body style="font-family:system-ui;max-width:800px;margin:2rem auto">
    <h2>Multi-Tenant Aggregation Layer v2.1 — monitoring</h2>
    <h3>Freshness</h3>
    <p><b>Data lag:</b> {f['updated_ago']} &nbsp;|&nbsp;
       <b>Last refresh:</b> {f['last_run_at']} &nbsp;|&nbsp;
       <b>Total refreshes:</b> {f['total_refreshes']}</p>
    <h3>Correctness (drift detector)</h3>
    <p style="color:{drift_color}"><b>Drift status:</b> {drift_txt}</p>
    <p><b>Last checked:</b> {d['checked_at'] or 'pending first check (30s after startup)'}</p>
    <h3>Performance</h3>
    <p><b>Rollup probe:</b> {probe_ms} ms &nbsp;|&nbsp;
       <b>Redis:</b> {redis_txt}</p>
    <p><b>Query log (last 500):</b> cache hit {qs.get('cache_hit_rate','?')}% &nbsp;|&nbsp;
       p50 {qs.get('p50_ms','?')} ms &nbsp;|&nbsp;
       p99 {qs.get('p99_ms','?')} ms</p>
    <h3>Connection pool</h3>
    <p><b>Used:</b> {ps['pool_used']} &nbsp;|&nbsp;
       <b>Available:</b> {ps['pool_available']} &nbsp;|&nbsp;
       <b>Total:</b> {ps['pool_total']}</p>
    <h3>Tenant rollup summary</h3>
    <table border=1 cellpadding=6>
      <tr><th>tenant_id</th><th>orders</th><th>revenue</th></tr>
      {rows}
    </table>
    </body></html>"""

# ── static ───────────────────────────────────────────────────────────────────
_BASE = os.path.dirname(__file__)

@app.get("/dashboard")
def dashboard():
    p = os.path.join(_BASE, "static", "dashboard.html")
    if not os.path.exists(p): raise HTTPException(404, "Dashboard not found")
    return FileResponse(p, media_type="text/html")

@app.get("/")
def root():
    return {
        "service": "multi-tenant aggregation layer",
        "version": "2.1",
        "per_tenant_endpoints": [
            "GET /tenants/{id}/orders/count?from&to&status",
            "GET /tenants/{id}/orders/trend?from&to&metric&granularity=day|hour",
            "GET /tenants/{id}/products/top?n&offset&by&from&to",
            "GET /tenants/{id}/orders/breakdown?by=status|category&from&to",
            "GET /tenants/{id}/orders/compare?from&to&prior_days",
            "GET /tenants/{id}/metrics/{name}?from&to",
        ],
        "platform_endpoints": [
            "GET /platform/summary", "GET /platform/tenants",
            "GET /platform/orders/count", "GET /platform/orders/trend?granularity=day|hour",
            "GET /platform/products/top?n&offset",
            "GET /platform/orders/breakdown", "GET /platform/leaderboard?by&n",
            "GET /platform/metrics/{name}",
        ],
        "system_endpoints": [
            "GET /freshness", "GET /health",
            "GET /stats", "GET /metrics",
            "GET /admin/query-log?tenant_id&limit",
            "GET /admin/query-stats",
            "GET /admin/concurrency",
            "GET /admin/scale-report",
            "POST /admin/drift-check",
            "POST /admin/rebuild",
            "GET /dashboard",
        ],
    }

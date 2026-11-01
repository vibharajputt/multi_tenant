"""Query API — v2.2

Reads ONLY pre-computed rollup tables. Never touches raw orders.

Auth (v2.2)
-----------
POST /auth/login  {tenant_id, password}  →  {access_token}
All /tenants/{id}/... endpoints require Bearer token.
Token tenant must match URL tenant_id — 403 otherwise.
Platform + system endpoints are open (operator access).

Scopes
------
/tenants/{id}/...   per-tenant, JWT-protected, strictly isolated
/platform/...       operator view across all tenants (no auth required)
/auth/...           login + token info
"""
import time
import json
import functools
import threading
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Path, Depends, Security, status, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.db import (query, rcache, cache_gen, bump_cache_gen,
                    CACHE_TTL_SEC, MAX_CONC_PER_TENANT, pool)
from app.auth import (do_login, get_current_tenant, require_tenant,
                      LoginRequest, TokenResponse)
import app.drift     as drift_mod
import app.query_log as qlog

app = FastAPI(title="Multi-Tenant Aggregation Layer", version="2.2")

# ── startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
def _startup():
    drift_mod.start()

# ── auth bearer scheme ────────────────────────────────────────────────────────
_bearer = HTTPBearer(auto_error=False)

def _make_auth(expected_tid: int):
    """Factory: returns a FastAPI dependency that checks token == expected_tid."""
    def _dep(creds: Optional[HTTPAuthorizationCredentials] = Security(_bearer)) -> int:
        current = get_current_tenant(creds)
        return require_tenant(expected_tid, current)
    return _dep

def _auth_dep(tenant_id: int = Path(...),
              creds: Optional[HTTPAuthorizationCredentials] = Security(_bearer)) -> int:
    """Single dependency used on all per-tenant endpoints."""
    current = get_current_tenant(creds)
    return require_tenant(tenant_id, current)

# ── per-tenant concurrency guard (B-3) ───────────────────────────────────────
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
        return {"pool_used": len(p._used), "pool_available": len(p._pool),
                "pool_total": p.maxconn}
    except Exception:
        return {"pool_used": None, "pool_available": None, "pool_total": None}

# ── Redis cache helpers ───────────────────────────────────────────────────────
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

# ── misc helpers ──────────────────────────────────────────────────────────────
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
# AUTH ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/auth/login", response_model=TokenResponse, tags=["auth"])
def login(req: LoginRequest):
    """
    Login with tenant_id + password. Returns a JWT Bearer token.

        POST /auth/login
        { "tenant_id": 1, "password": "tenant1" }

    Use the token on all /tenants/{id}/... endpoints:
        Authorization: Bearer <token>

    Default passwords (after running seed_passwords.py): tenant1, tenant2, ...
    """
    return do_login(req)

@app.get("/auth/me", tags=["auth"])
def auth_me(
    creds: Optional[HTTPAuthorizationCredentials] = Security(_bearer),
):
    """Returns the tenant_id encoded in the current token."""
    current = get_current_tenant(creds)
    return {"tenant_id": current, "authenticated": True}

# ═══════════════════════════════════════════════════════════════════════════
# PER-TENANT ENDPOINTS  (JWT required — token tenant must match URL tenant)
# ═══════════════════════════════════════════════════════════════════════════

# FR-1: counts & totals
@app.get("/tenants/{tenant_id}/orders/count", tags=["per-tenant"])
@timed
def orders_count(
    tenant_id: int = Path(...),
    _: int = Depends(_auth_dep),
    from_: str = Query(None, alias="from"),
    to: str    = Query(None),
    status: str = Query(None),
):
    """FR-1: counts & totals for a time range.
    Requires: Authorization: Bearer <token>  (token must be for this tenant_id)
    """
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
           "orders": int(row["orders"]), "revenue": float(row["revenue"]),
           "units": int(row["units"]), "cached": False}
    _cache_set(key, out)
    return out

# FR-2: trend
@app.get("/tenants/{tenant_id}/orders/trend", tags=["per-tenant"])
@timed
def orders_trend(
    tenant_id: int,
    _: int = Depends(_auth_dep),
    from_: str  = Query(None, alias="from"),
    to: str     = Query(None),
    metric: str = Query("orders"),
    granularity: str = Query("day"),
):
    frm, to = _range(from_, to)
    agg = {"orders": "SUM(order_count)", "revenue": "SUM(total_amount)",
           "units":  "SUM(total_qty)"}.get(metric, "SUM(order_count)")
    if granularity == "hour":
        rows = query(f"""
            SELECT hour AS ts, {agg} value FROM rollup_hourly
            WHERE tenant_id=%s AND hour>=%s::timestamp
              AND hour<%s::timestamp + interval '1 day'
            GROUP BY hour ORDER BY hour
        """, (tenant_id, frm, to))
        series = [{"hour": str(r["ts"]), "value": float(r["value"])} for r in rows]
    else:
        rows = query(f"""
            SELECT day AS ts, {agg} value FROM rollup_daily
            WHERE tenant_id=%s AND day>=%s AND day<=%s
            GROUP BY day ORDER BY day
        """, (tenant_id, frm, to))
        series = [{"day": str(r["ts"]), "value": float(r["value"])} for r in rows]
    return {"tenant_id": tenant_id, "metric": metric, "granularity": granularity,
            "from": frm, "to": to, "series": series, "cached": False}

# FR-3: top-N
@app.get("/tenants/{tenant_id}/products/top", tags=["per-tenant"])
@timed
def top_products(
    tenant_id: int,
    _: int = Depends(_auth_dep),
    from_:  str = Query(None, alias="from"),
    to: str     = Query(None),
    n: int      = Query(10, ge=1, le=500),
    offset: int = Query(0,  ge=0),
    by: str     = Query("revenue"),
):
    frm, to = _range(from_, to)
    order = {"revenue": "SUM(total_amount)", "units": "SUM(total_qty)",
             "orders":  "SUM(order_count)"}.get(by, "SUM(total_amount)")
    rows = query(f"""
        SELECT product_id, MIN(product_name) product_name,
               SUM(total_amount) revenue, SUM(total_qty) units, SUM(order_count) orders
        FROM rollup_product_daily
        WHERE tenant_id=%s AND day>=%s AND day<=%s
        GROUP BY product_id ORDER BY {order} DESC LIMIT %s OFFSET %s
    """, (tenant_id, frm, to, n, offset))
    return {"tenant_id": tenant_id, "from": frm, "to": to, "by": by,
            "offset": offset, "count": len(rows),
            "top": [{"product_id": r["product_id"], "product_name": r["product_name"],
                     "revenue": float(r["revenue"]), "units": int(r["units"]),
                     "orders": int(r["orders"])} for r in rows], "cached": False}

# FR-4: breakdown
@app.get("/tenants/{tenant_id}/orders/breakdown", tags=["per-tenant"])
@timed
def breakdown(
    tenant_id: int,
    _: int = Depends(_auth_dep),
    from_: str = Query(None, alias="from"),
    to: str    = Query(None),
    by: str    = Query("status"),
):
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

# B-4: config metric
@app.get("/tenants/{tenant_id}/metrics/{name}", tags=["per-tenant"])
@timed
def tenant_metric(
    tenant_id: int,
    name: str,
    _: int = Depends(_auth_dep),
    from_: str = Query(None, alias="from"),
    to: str    = Query(None),
):
    frm, to = _range(from_, to)
    m = query("SELECT expr, label FROM metric_config WHERE name=%s AND is_active",
              (name,), one=True)
    if not m: raise HTTPException(404, f"unknown metric '{name}'")
    row = query(f"SELECT {m['expr']} value FROM rollup_daily "
                "WHERE tenant_id=%s AND day>=%s AND day<=%s",
                (tenant_id, frm, to), one=True)
    return {"tenant_id": tenant_id, "metric": name, "label": m["label"],
            "from": frm, "to": to, "value": float(row["value"] or 0), "cached": False}

# Compare periods
@app.get("/tenants/{tenant_id}/orders/compare", tags=["per-tenant"])
@timed
def tenant_compare(
    tenant_id: int,
    _: int = Depends(_auth_dep),
    from_:      str = Query(None, alias="from"),
    to: str         = Query(None),
    prior_days: int = Query(7),
):
    frm, to = _range(from_, to)
    d_from = date.fromisoformat(frm); d_to = date.fromisoformat(to)
    delta  = (d_to - d_from).days + 1
    p_to   = (d_from - timedelta(days=1)).isoformat()
    p_from = (d_from - timedelta(days=delta)).isoformat()
    def totals(f, t):
        r = query("""SELECT COALESCE(SUM(order_count),0) o,
                            COALESCE(SUM(total_amount),0) r,
                            COALESCE(SUM(total_qty),0) u
                     FROM rollup_daily WHERE tenant_id=%s AND day>=%s AND day<=%s""",
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
# PLATFORM ENDPOINTS  (no auth — operator/admin access)
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/platform/summary", tags=["platform"])
@timed
def platform_summary(from_: str = Query(None, alias="from"), to: str = Query(None)):
    frm, to = _range(from_, to)
    row = query("""SELECT COUNT(DISTINCT tenant_id) tenants,
                          COALESCE(SUM(order_count),0) orders,
                          COALESCE(SUM(total_amount),0) revenue,
                          COALESCE(SUM(total_qty),0) units
                   FROM rollup_daily WHERE day>=%s AND day<=%s""", (frm, to), one=True)
    return {"scope": "platform", "from": frm, "to": to,
            "tenants": int(row["tenants"]), "orders": int(row["orders"]),
            "revenue": float(row["revenue"]), "units": int(row["units"]), "cached": False}

@app.get("/platform/orders/count", tags=["platform"])
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
    return {"scope": "platform", "from": frm, "to": to,
            "orders": int(row["orders"]), "revenue": float(row["revenue"]),
            "units": int(row["units"]), "cached": False}

@app.get("/platform/orders/trend", tags=["platform"])
@timed
def platform_trend(from_: str = Query(None, alias="from"), to: str = Query(None),
                   metric: str = Query("orders"), granularity: str = Query("day")):
    frm, to = _range(from_, to)
    agg = {"orders": "SUM(order_count)", "revenue": "SUM(total_amount)",
           "units": "SUM(total_qty)"}.get(metric, "SUM(order_count)")
    if granularity == "hour":
        rows = query(f"""SELECT hour ts, {agg} value FROM rollup_hourly
                         WHERE hour>=%s::timestamp AND hour<%s::timestamp + interval '1 day'
                         GROUP BY hour ORDER BY hour""", (frm, to))
        series = [{"hour": str(r["ts"]), "value": float(r["value"])} for r in rows]
    else:
        rows = query(f"""SELECT day ts, {agg} value FROM rollup_daily
                         WHERE day>=%s AND day<=%s GROUP BY day ORDER BY day""", (frm, to))
        series = [{"day": str(r["ts"]), "value": float(r["value"])} for r in rows]
    return {"scope": "platform", "metric": metric, "granularity": granularity,
            "from": frm, "to": to, "series": series, "cached": False}

@app.get("/platform/products/top", tags=["platform"])
@timed
def platform_top(from_: str = Query(None, alias="from"), to: str = Query(None),
                 n: int = Query(10, ge=1, le=500), offset: int = Query(0, ge=0),
                 by: str = Query("revenue")):
    frm, to = _range(from_, to)
    order = {"revenue": "SUM(total_amount)", "units": "SUM(total_qty)",
             "orders": "SUM(order_count)"}.get(by, "SUM(total_amount)")
    rows = query(f"""SELECT product_id, MIN(product_name) product_name,
                            SUM(total_amount) revenue, SUM(total_qty) units,
                            SUM(order_count) orders
                     FROM rollup_product_daily WHERE day>=%s AND day<=%s
                     GROUP BY product_id ORDER BY {order} DESC LIMIT %s OFFSET %s""",
                 (frm, to, n, offset))
    return {"scope": "platform", "from": frm, "to": to, "by": by, "offset": offset,
            "top": [{"product_id": r["product_id"], "product_name": r["product_name"],
                     "revenue": float(r["revenue"]), "units": int(r["units"]),
                     "orders": int(r["orders"])} for r in rows], "cached": False}

@app.get("/platform/orders/breakdown", tags=["platform"])
@timed
def platform_breakdown(from_: str = Query(None, alias="from"), to: str = Query(None),
                       by: str = Query("status")):
    frm, to = _range(from_, to)
    if by not in ("status", "category"): raise HTTPException(400, "by must be status|category")
    rows = query(f"""SELECT {by} key, SUM(order_count) orders, SUM(total_amount) revenue
                     FROM rollup_daily WHERE day>=%s AND day<=%s
                     GROUP BY {by} ORDER BY orders DESC""", (frm, to))
    return {"scope": "platform", "from": frm, "to": to, "by": by,
            "breakdown": [{"key": r["key"], "orders": int(r["orders"]),
                           "revenue": float(r["revenue"])} for r in rows], "cached": False}

@app.get("/platform/tenants", tags=["platform"])
@timed
def platform_tenants(from_: str = Query(None, alias="from"), to: str = Query(None)):
    frm, to = _range(from_, to)
    rows = query("""SELECT tenant_id, SUM(order_count) orders,
                           SUM(total_amount) revenue, SUM(total_qty) units
                    FROM rollup_daily WHERE day>=%s AND day<=%s
                    GROUP BY tenant_id ORDER BY orders DESC""", (frm, to))
    return {"scope": "platform", "from": frm, "to": to,
            "tenants": [{"tenant_id": r["tenant_id"], "orders": int(r["orders"]),
                         "revenue": float(r["revenue"]), "units": int(r["units"])}
                        for r in rows], "cached": False}

@app.get("/platform/metrics/{name}", tags=["platform"])
@timed
def platform_metric(name: str, from_: str = Query(None, alias="from"),
                    to: str = Query(None)):
    frm, to = _range(from_, to)
    m = query("SELECT expr, label FROM metric_config WHERE name=%s AND is_active",
              (name,), one=True)
    if not m: raise HTTPException(404, f"unknown metric '{name}'")
    row = query(f"SELECT {m['expr']} value FROM rollup_daily WHERE day>=%s AND day<=%s",
                (frm, to), one=True)
    return {"scope": "platform", "metric": name, "label": m["label"],
            "from": frm, "to": to, "value": float(row["value"] or 0), "cached": False}

@app.get("/platform/leaderboard", tags=["platform"])
@timed
def platform_leaderboard(from_: str = Query(None, alias="from"), to: str = Query(None),
                         by: str = Query("revenue"), n: int = Query(10)):
    frm, to = _range(from_, to)
    col = {"revenue": "SUM(total_amount)", "orders": "SUM(order_count)",
           "units": "SUM(total_qty)"}.get(by, "SUM(total_amount)")
    rows = query(f"""SELECT tenant_id, SUM(order_count) orders,
                            SUM(total_amount) revenue, SUM(total_qty) units
                     FROM rollup_daily WHERE day>=%s AND day<=%s
                     GROUP BY tenant_id ORDER BY {col} DESC LIMIT %s""", (frm, to, n))
    return {"scope": "platform", "from": frm, "to": to, "ranked_by": by,
            "leaderboard": [{"rank": i+1, "tenant_id": r["tenant_id"],
                             "orders": int(r["orders"]), "revenue": float(r["revenue"]),
                             "units": int(r["units"])} for i, r in enumerate(rows)],
            "cached": False}

# ═══════════════════════════════════════════════════════════════════════════
# SYSTEM / MONITORING / ADMIN
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/freshness", tags=["system"])
def freshness():
    r = query("""SELECT last_xid_snap, last_run_at, rows_last_run,
                        max_updated, total_refreshes
                 FROM refresh_state WHERE id=1""", one=True)
    lag = None
    if r["last_run_at"]:
        lag = max(0, round((datetime.utcnow() - r["last_run_at"]).total_seconds()))
    return {"last_xid_snap": r["last_xid_snap"], "last_run_at": str(r["last_run_at"]),
            "rows_last_run": r["rows_last_run"], "newest_data_at": str(r["max_updated"]),
            "total_refreshes": r["total_refreshes"], "refresh_lag_seconds": lag,
            "updated_ago": f"{lag}s ago" if lag is not None else "never",
            "redis_connected": rcache() is not None}

@app.get("/health", tags=["system"])
def health():
    d   = drift_mod.last_result()
    ps  = _pool_stats()
    lag_row = query("SELECT last_run_at FROM refresh_state WHERE id=1", one=True)
    lag = None
    if lag_row and lag_row["last_run_at"]:
        lag = max(0, round((datetime.utcnow() - lag_row["last_run_at"]).total_seconds()))
    return {"ok": True, "redis": rcache() is not None, "refresh_lag_sec": lag,
            "drift_detected": d["drift_detected"], "drift_checked_at": d["checked_at"],
            "drift_buckets_sampled": d["buckets_sampled"],
            "drift_mismatch_count": d["mismatch_count"],
            "drift_details": d["details"], **ps}

@app.post("/admin/drift-check", tags=["admin"])
def admin_drift_check():
    sampled, mismatches, details = drift_mod._run_check()
    return {"buckets_sampled": sampled, "mismatch_count": mismatches,
            "drift_detected": mismatches > 0, "details": details}

@app.get("/admin/query-log", tags=["admin"])
def admin_query_log(tenant_id: int = Query(None), limit: int = Query(50, le=200)):
    return {"entries": qlog.recent(tenant_id=tenant_id, limit=limit)}

@app.get("/admin/query-stats", tags=["admin"])
def admin_query_stats():
    return qlog.stats()

@app.get("/admin/concurrency", tags=["admin"])
def admin_concurrency():
    active = {}
    for tid, s in _sema.items():
        running = MAX_CONC_PER_TENANT - s._value
        if running > 0:
            active[str(tid)] = {"running": running, "available": s._value}
    return {"max_per_tenant": MAX_CONC_PER_TENANT, "active_tenants": len(active),
            "tenants": active, **_pool_stats()}

@app.get("/admin/scale-report", tags=["admin"])
def admin_scale_report():
    base = query("SELECT COUNT(*) cnt FROM orders", one=True)
    rd   = query("SELECT COUNT(*) cnt FROM rollup_daily", one=True)
    rp   = query("SELECT COUNT(*) cnt FROM rollup_product_daily", one=True)
    rh   = query("SELECT COUNT(*) cnt FROM rollup_hourly", one=True)
    base_n = int(base["cnt"]); rd_n = int(rd["cnt"])
    rp_n   = int(rp["cnt"]);   rh_n = int(rh["cnt"])
    total_r = rd_n + rp_n + rh_n
    ratio   = round(base_n / total_r, 1) if total_r else None
    t0 = time.perf_counter()
    query("SELECT SUM(order_count) FROM rollup_daily WHERE tenant_id=1", one=True)
    p1 = round((time.perf_counter()-t0)*1000, 1)
    t0 = time.perf_counter()
    query("SELECT SUM(order_count) FROM rollup_daily WHERE tenant_id=1 "
          "AND day>='2025-01-01' AND day<='2026-12-31'", one=True)
    p2 = round((time.perf_counter()-t0)*1000, 1)
    return {"base_rows": base_n, "rollup_daily_rows": rd_n,
            "rollup_product_rows": rp_n, "rollup_hourly_rows": rh_n,
            "total_rollup_rows": total_r,
            "compression_ratio": f"{ratio}x — rollups are {ratio}x smaller than base",
            "sample_query_ms": p1, "date_filtered_query_ms": p2,
            "scale_note": ("Rollup size grows with tenants × days × categories "
                           "(not raw rows). At 10M rows the rollup stays ~the same size.")}

@app.post("/admin/rebuild", tags=["admin"])
def admin_rebuild():
    from app.refresh import rebuild
    rebuild(); bump_cache_gen()
    return {"ok": True, "message": "rollups rebuilt from base table"}

@app.get("/metrics", tags=["system"])
def list_metrics():
    rows = query("SELECT name, label FROM metric_config WHERE is_active ORDER BY name")
    return {"metrics": [{"name": r["name"], "label": r["label"]} for r in rows]}

@app.get("/stats", response_class=HTMLResponse, tags=["system"])
def stats():
    f  = freshness()
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
    drift_txt   = (f"✓ clean ({d['buckets_sampled']} buckets sampled)"
                   if not d["drift_detected"] else f"⚠ {d['mismatch_count']} mismatches")
    redis_txt   = "✓ connected" if f["redis_connected"] else "✗ offline (fallback active)"
    return f"""<html><body style="font-family:system-ui;max-width:800px;margin:2rem auto">
    <h2>Multi-Tenant Aggregation Layer v2.2 — monitoring</h2>
    <h3>Freshness</h3>
    <p><b>Data lag:</b> {f['updated_ago']} &nbsp;|&nbsp;
       <b>Last refresh:</b> {f['last_run_at']} &nbsp;|&nbsp;
       <b>Total refreshes:</b> {f['total_refreshes']}</p>
    <h3>Correctness (drift detector)</h3>
    <p style="color:{drift_color}"><b>Drift status:</b> {drift_txt}</p>
    <p><b>Last checked:</b> {d['checked_at'] or 'pending first check (30s after startup)'}</p>
    <h3>Performance</h3>
    <p><b>Rollup probe:</b> {probe_ms} ms &nbsp;|&nbsp; <b>Redis:</b> {redis_txt}</p>
    <p><b>Query log:</b> cache hit {qs.get('cache_hit_rate','?')}% &nbsp;|&nbsp;
       p50 {qs.get('p50_ms','?')} ms &nbsp;|&nbsp; p99 {qs.get('p99_ms','?')} ms</p>
    <h3>Connection pool</h3>
    <p><b>Used:</b> {ps['pool_used']} &nbsp;|&nbsp;
       <b>Available:</b> {ps['pool_available']} &nbsp;|&nbsp;
       <b>Total:</b> {ps['pool_total']}</p>
    <h3>Tenant rollup summary</h3>
    <table border=1 cellpadding=6>
      <tr><th>tenant_id</th><th>orders</th><th>revenue</th></tr>
      {rows}
    </table></body></html>"""

_BASE = os.path.dirname(__file__)

@app.get("/dashboard", tags=["system"])
def dashboard():
    p = os.path.join(_BASE, "static", "dashboard.html")
    if not os.path.exists(p): raise HTTPException(404, "Dashboard not found")
    return FileResponse(p, media_type="text/html")

@app.get("/", tags=["system"])
def root():
    return {
        "service": "multi-tenant aggregation layer", "version": "2.2",
        "auth": "POST /auth/login  {tenant_id, password}  →  Bearer token",
        "isolation": "token tenant must match URL tenant_id — 403 otherwise",
        "per_tenant": [
            "GET /tenants/{id}/orders/count", "GET /tenants/{id}/orders/trend",
            "GET /tenants/{id}/products/top", "GET /tenants/{id}/orders/breakdown",
            "GET /tenants/{id}/orders/compare", "GET /tenants/{id}/metrics/{name}",
        ],
        "platform": [
            "GET /platform/summary", "GET /platform/tenants",
            "GET /platform/orders/count", "GET /platform/orders/trend",
            "GET /platform/products/top", "GET /platform/orders/breakdown",
            "GET /platform/leaderboard", "GET /platform/metrics/{name}",
        ],
        "system": [
            "GET /freshness", "GET /health", "GET /stats", "GET /metrics",
            "GET /admin/query-log", "GET /admin/query-stats",
            "GET /admin/concurrency", "GET /admin/scale-report",
            "POST /admin/drift-check", "POST /admin/rebuild",
            "GET /dashboard",
        ],
    }
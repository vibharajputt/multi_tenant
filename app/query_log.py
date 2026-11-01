"""In-memory query log — last N queries with tenant, endpoint, latency, cache.

A fixed-size ring buffer (no DB writes, zero overhead on the hot path).
Exposed via /admin/query-log for operator visibility and demo purposes.
Satisfies the PRD's "simple API with README" completeness (FR-7) and
provides a live record for the parallel-requests demo.
"""
import threading
import time
from collections import deque

_MAXLEN = 500
_buf: deque = deque(maxlen=_MAXLEN)
_lock = threading.Lock()

def record(tenant_id, scope: str, endpoint: str, query_ms: float,
           cached: bool, status: int = 200):
    """Call this from every API endpoint after it responds."""
    with _lock:
        _buf.append({
            "ts":         time.strftime("%H:%M:%S", time.gmtime()),
            "tenant_id":  tenant_id,
            "scope":      scope,       # "tenant" | "platform" | "system"
            "endpoint":   endpoint,
            "query_ms":   round(query_ms, 1),
            "cached":     cached,
            "status":     status,
        })

def recent(tenant_id=None, limit: int = 50) -> list:
    with _lock:
        rows = list(_buf)
    if tenant_id is not None:
        rows = [r for r in rows if r.get("tenant_id") == tenant_id]
    return list(reversed(rows))[:limit]

def stats() -> dict:
    with _lock:
        rows = list(_buf)
    if not rows:
        return {"total": 0}
    cache_hits = sum(1 for r in rows if r.get("cached"))
    latencies  = [r["query_ms"] for r in rows if r.get("query_ms") is not None]
    return {
        "total":           len(rows),
        "cache_hit_rate":  round(cache_hits / len(rows) * 100, 1),
        "p50_ms":          round(sorted(latencies)[len(latencies)//2], 1) if latencies else None,
        "p99_ms":          round(sorted(latencies)[int(len(latencies)*0.99)], 1) if latencies else None,
        "max_ms":          round(max(latencies), 1) if latencies else None,
    }

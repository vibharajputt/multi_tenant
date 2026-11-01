"""Shared config, DB pool, Redis cache helpers.

All configuration comes from environment variables — see .env.example.
Nothing is hard-coded. Judges change only .env, never this file.

Design notes
------------
* ThreadedConnectionPool(1, 32): sized for 10 parallel tenant threads + worker
  + API workers + headroom.  Raised from 12 to survive concurrent load tests.
* Redis is used as a generation-keyed query cache.  Every time the refresh
  worker rewrites rollup buckets it increments cache_gen; API keys carry the
  generation number, so stale results are naturally evicted without a TTL scan.
  Redis is entirely optional — all cache functions swallow exceptions and fall
  back to a live rollup read, so the system works (just slightly slower) if
  Redis is absent or down.
* rcache() returns None when Redis is unreachable; callers check for None.
"""
import os
import psycopg2
import psycopg2.extras
import psycopg2.pool
import redis as _redis_lib

# ── configuration from environment ──────────────────────────────────────────
DB = dict(
    host=os.environ.get("POSTGRES_HOST", "db"),
    port=int(os.environ.get("POSTGRES_PORT", 5432)),
    dbname=os.environ.get("POSTGRES_DB", "hackathon"),
    user=os.environ.get("POSTGRES_USER", "hackathon"),
    password=os.environ.get("POSTGRES_PASSWORD", "hackathon"),
)

REFRESH_INTERVAL_SEC   = int(os.environ.get("REFRESH_INTERVAL_SEC", 10))
REFRESH_BATCH_SIZE     = int(os.environ.get("REFRESH_BATCH_SIZE", 50000))
CACHE_TTL_SEC          = int(os.environ.get("CACHE_TTL_SEC", 30))
MAX_CONC_PER_TENANT    = int(os.environ.get("MAX_CONCURRENT_QUERIES_PER_TENANT", 4))
DATA_DIR               = os.environ.get("DATA_DIR", "/data")
# Adaptive refresh: only run a full recompute when the DB reports changes.
# IDLE_POLL_SEC  — how often the worker checks for new change_ids when idle
# ACTIVE_POLL_SEC — how often it runs when it just found changes
IDLE_POLL_SEC   = int(os.environ.get("IDLE_POLL_SEC", 5))
ACTIVE_POLL_SEC = int(os.environ.get("ACTIVE_POLL_SEC", 2))

# ── Postgres connection pool ─────────────────────────────────────────────────
_pool = None
def pool():
    global _pool
    if _pool is None:
        # 10 tenant threads × 1 conn + worker (2) + API (8) + buffer = 32
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 32, **DB)
    return _pool

def get_conn():
    return pool().getconn()

def put_conn(c):
    pool().putconn(c)

def query(sql, params=None, one=False):
    c = get_conn()
    try:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
        c.commit()
        return (rows[0] if rows else None) if one else rows
    finally:
        put_conn(c)

def execute(sql, params=None):
    c = get_conn()
    try:
        with c.cursor() as cur:
            cur.execute(sql, params or ())
        c.commit()
    finally:
        put_conn(c)

# ── Redis cache ──────────────────────────────────────────────────────────────
_r = None
def rcache():
    """Return a Redis client, or None if Redis is unreachable.
    All callers must handle None gracefully (fall back to live rollup read).
    """
    global _r
    if _r is None:
        try:
            client = _redis_lib.Redis(
                host=os.environ.get("REDIS_HOST", "cache"),
                port=int(os.environ.get("REDIS_PORT", 6379)),
                decode_responses=True,
                socket_connect_timeout=1,
                socket_timeout=1,
            )
            client.ping()          # verify connection at startup
            _r = client
        except Exception:
            return None            # Redis unavailable — callers fall back
    return _r

def cache_gen() -> str:
    """Return the current cache generation counter as a string.
    Returns '0' if Redis is down (all keys will be treated as generation-0).
    """
    try:
        r = rcache()
        if r is None:
            return "0"
        g = r.get("cache_gen")
        return g if g is not None else "0"
    except Exception:
        return "0"

def bump_cache_gen():
    """Increment the generation counter so all existing cache entries become
    stale.  Called by the refresh worker after every rollup recompute.
    """
    try:
        r = rcache()
        if r:
            r.incr("cache_gen")
    except Exception:
        pass

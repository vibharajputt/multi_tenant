"""Seed default passwords for demo.

Run once after loader:
  docker compose run --rm api python -m app.seed_passwords

Default password for tenant N = "tenant{N}"  (e.g. tenant1, tenant42)
Change in .env or pass --password to override.
"""
import argparse
from app.db import query, execute, get_conn, put_conn
from app.auth import hash_password

def seed(default_prefix: str = "tenant"):
    # Add password_hash column if missing
    execute("""
        ALTER TABLE tenants
        ADD COLUMN IF NOT EXISTS password_hash TEXT;
    """)
    tenants = [r["tenant_id"] for r in
               query("SELECT tenant_id FROM tenants ORDER BY tenant_id")]
    if not tenants:
        print("No tenants found — run loader first.")
        return
    c = get_conn()
    try:
        with c.cursor() as cur:
            for tid in tenants:
                pwd    = f"{default_prefix}{tid}"
                hashed = hash_password(pwd)
                cur.execute(
                    "UPDATE tenants SET password_hash = %s WHERE tenant_id = %s",
                    (hashed, tid)
                )
        c.commit()
    finally:
        put_conn(c)
    print(f"Seeded {len(tenants)} tenants.")
    print(f"Default password pattern: tenant1 / tenant2 / ... / tenant{tenants[-1]}")
    print("Change JWT_SECRET in .env before production use.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="tenant",
                    help="Password prefix (default: 'tenant' → tenant1, tenant2...)")
    a = ap.parse_args()
    seed(a.prefix)
"""Runs INSIDE the gatekeeper container: migrate the licensing sqlite
(lead/licenses/email_log) into the shared Postgres. Idempotent."""
import os
import sqlite3
import sys

import psycopg

SRC = "/tmp/leads.db"
url = os.environ["DATABASE_URL"]

lite = sqlite3.connect(SRC)

with psycopg.connect(url) as pg:
    # ensure the licensing schema exists (runs before the new container)
    pg.execute("""CREATE TABLE IF NOT EXISTS leads (
        id BIGSERIAL PRIMARY KEY,
        email TEXT NOT NULL,
        business_name TEXT DEFAULT '',
        serial TEXT DEFAULT '',
        edition TEXT DEFAULT 'community-trial',
        source TEXT DEFAULT 'website',
        ip TEXT DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT now()
    )""")
    pg.execute("""CREATE TABLE IF NOT EXISTS licenses (
        id BIGSERIAL PRIMARY KEY,
        serial TEXT UNIQUE NOT NULL,
        email TEXT NOT NULL,
        edition TEXT NOT NULL,
        kind TEXT NOT NULL,
        stripe_event TEXT DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT now(),
        license_json TEXT DEFAULT '',
        business_name TEXT DEFAULT '',
        revoked BOOLEAN DEFAULT FALSE
    )""")
    pg.execute("""CREATE TABLE IF NOT EXISTS email_log (
        id BIGSERIAL PRIMARY KEY,
        kind TEXT NOT NULL,
        to_addr TEXT NOT NULL,
        serial TEXT DEFAULT '',
        ok INTEGER DEFAULT 0,
        detail TEXT DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT now()
    )""")

    # idempotency: skip if licenses already migrated
    n = pg.execute("SELECT COUNT(*) FROM licenses").fetchone()[0]
    if n:
        print(f"already migrated ({n} licenses) — skipping")
        sys.exit(0)

    for t, cols in (
        ("leads", "email, business_name, serial, edition, source, ip, "
                  "created_at"),
        ("licenses", "serial, email, edition, kind, stripe_event, "
                     "created_at, license_json"),
        ("email_log", "kind, to_addr, serial, ok, detail, created_at"),
    ):
        try:
            rows = lite.execute(f"SELECT {cols} FROM {t}").fetchall()
        except sqlite3.OperationalError as e:
            print(f"{t}: skipped ({e})")
            continue
        for r in rows:
            r = tuple(x if x is not None else "" for x in r)
            ph = ", ".join(["%s"] * len(r))
            pg.execute(f"INSERT INTO {t} ({cols}) VALUES ({ph})", r)
        print(f"{t}: {len(rows)} rows migrated")

print("MIGRATION_DONE")

"""Admin service smoke test — boots against a local Postgres (5433) and
checks: login flow, dashboard render, conversations/licenses/leads/emails/
kb/gateway pages, KB save + reset, review-reply (mock Resend).

Run: docker run -d --name helpdesk-pg -p 5433:5432 -e POSTGRES_PASSWORD=test \
       -e POSTGRES_DB=helpdesk postgres:16-alpine
     ..\.venv\Scripts\python.exe test_admin.py   (from admin/)
"""
import os
import sys
import time

os.environ.setdefault("DATABASE_URL",
                      "postgresql://postgres:test@127.0.0.1:5433/helpdesk")
os.environ.setdefault("ADMIN_KEY", "test-admin-key")
os.environ.pop("RESEND_API_KEY", None)

import psycopg                                            # noqa: E402

# seed: one review-queued session + one license + one email log row
with psycopg.connect(os.environ["DATABASE_URL"]) as c:
    c.execute("""CREATE TABLE IF NOT EXISTS chat_sessions (
        id TEXT PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT now(),
        ip TEXT DEFAULT '', email TEXT, escalated BOOLEAN DEFAULT FALSE,
        needs_review BOOLEAN DEFAULT FALSE,
        message_count INT DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS chat_messages (
        id BIGSERIAL PRIMARY KEY, session_id TEXT, role TEXT,
        content TEXT, confident BOOLEAN, doc TEXT, model TEXT,
        created_at TIMESTAMPTZ DEFAULT now())""")
    c.execute("""CREATE TABLE IF NOT EXISTS kb_overrides (
        name TEXT PRIMARY KEY, content TEXT NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT now())""")
    c.execute("""CREATE TABLE IF NOT EXISTS leads (
        id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL,
        business_name TEXT DEFAULT '', serial TEXT DEFAULT '',
        edition TEXT DEFAULT '', source TEXT DEFAULT '',
        ip TEXT DEFAULT '', created_at TIMESTAMPTZ DEFAULT now())""")
    c.execute("""CREATE TABLE IF NOT EXISTS licenses (
        id BIGSERIAL PRIMARY KEY, serial TEXT UNIQUE NOT NULL,
        email TEXT NOT NULL, edition TEXT NOT NULL, kind TEXT NOT NULL,
        stripe_event TEXT DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT now(),
        license_json TEXT DEFAULT '',
        business_name TEXT DEFAULT '', revoked BOOLEAN DEFAULT FALSE)""")
    c.execute("""CREATE TABLE IF NOT EXISTS email_log (
        id BIGSERIAL PRIMARY KEY, kind TEXT NOT NULL,
        to_addr TEXT NOT NULL, serial TEXT DEFAULT '',
        ok INTEGER DEFAULT 0, detail TEXT DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT now())""")
    c.execute("""CREATE TABLE IF NOT EXISTS admin_audit (
        id BIGSERIAL PRIMARY KEY, action TEXT NOT NULL,
        detail TEXT DEFAULT '', created_at TIMESTAMPTZ DEFAULT now())""")
    # deterministic: own the tables for this run
    for t in ("chat_messages", "chat_sessions", "licenses", "leads",
              "email_log", "admin_audit", "kb_overrides"):
        c.execute(f"TRUNCATE {t}")
    c.execute("INSERT INTO chat_sessions (id, email, needs_review) "
              "VALUES ('testsess0001', 'cust@example.com', TRUE) "
              "ON CONFLICT (id) DO UPDATE SET needs_review = TRUE, "
              "email = 'cust@example.com'")
    c.execute("INSERT INTO chat_messages (session_id, role, content) "
              "VALUES ('testsess0001', 'user', 'how do I install a key?')")
    c.execute("INSERT INTO leads (email, business_name, ip) "
              "VALUES ('lead@example.com', 'Acme Corp', '1.2.3.4')")
    c.execute("INSERT INTO licenses (serial, email, edition, kind, "
              "license_json) VALUES ('GL-TEST00000001', 'x@y.co', "
              "'community', 'manual', '{\"license\": \"x.y.z\"}') "
              "ON CONFLICT (serial) DO NOTHING")
    c.execute("INSERT INTO licenses (serial, email, edition, kind, "
              "license_json) VALUES ('GL-PAID00000001', 'buyer@example.com',"
              " 'community', 'paid-key', '{\"license\": \"a.b.c\"}') "
              "ON CONFLICT (serial) DO NOTHING")
    c.execute("INSERT INTO email_log (kind, to_addr, ok) "
              "VALUES ('trial', 'a@b.co', 1)")

import app                                               # noqa: E402
from fastapi.testclient import TestClient                # noqa: E402

fails = []


def check(name, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + name +
          ("" if cond else f"   {extra}"))
    if not cond:
        fails.append(name)


with TestClient(app.app) as client:
    # 1. unauthenticated -> redirect to login
    r = client.get("/", follow_redirects=False)
    check("dashboard redirects to login", r.status_code == 303
          and "/login" in r.headers.get("location", ""))

    # 2. bad key rejected
    r = client.post("/login", data={"key": "wrong"},
                    follow_redirects=False)
    check("bad key rejected", r.status_code == 303
          and "/login" in r.headers.get("location", ""))

    # 3. good key -> cookie set
    r = client.post("/login", data={"key": "test-admin-key"},
                    follow_redirects=False)
    check("login sets cookie", r.status_code == 303
          and "lodge_office" in r.cookies)

    # 4. dashboard renders with seeded data
    r = client.get("/")
    check("dashboard 200", r.status_code == 200)
    check("dashboard shows license", "GL-TEST00000001" in r.text)
    check("dashboard shows revenue", "$99" in r.text)

    # 5. pages render
    for path, needle in (("/conversations", "testsess0001"),
                         ("/reviews", "install a key"),
                         ("/leads", "lead@example.com"),
                         ("/licenses", "GL-TEST00000001"),
                         ("/emails", "trial"),
                         ("/kb", "presales"),
                         ("/gateway", "not configured")):
        r = client.get(path)
        check(f"{path} renders", r.status_code == 200
              and needle in r.text.lower() or needle in r.text,
              f"{r.status_code} {needle}")

    # 6. conversation detail + review form
    r = client.get("/conversation", params={"id": "testsess0001"})
    check("conversation renders", r.status_code == 200
          and "APPROVED REPLY" in r.text)

    # 7. review-reply (no email in env -> skips email, adds KB)
    r = client.post("/review-reply", data={
        "session_id": "testsess0001",
        "reply": "Save license.json in the data folder and restart.",
        "add_to_kb": "1", "doc": "licensing"},
        follow_redirects=False)
    check("review-reply redirects", r.status_code == 303)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        rv = c.execute("SELECT needs_review FROM chat_sessions "
                       "WHERE id='testsess0001'").fetchone()[0]
        ov = c.execute("SELECT content FROM kb_overrides "
                       "WHERE name='licensing'").fetchone()
    check("session marked reviewed", rv is False)
    check("kb override created", ov is not None
          and "install a key" in ov[0], str(ov)[:80])

    # 8. KB save + reset
    r = client.post("/kb/save", data={
        "name": "presales", "content": "title: t\ndescription: d\nedited"},
        follow_redirects=False)
    check("kb save redirects", r.status_code == 303)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT content FROM kb_overrides "
                      "WHERE name='presales'").fetchone()
    check("kb override stored", v is not None and "edited" in v[0])
    client.get("/kb/reset", params={"name": "presales"})
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT COUNT(*) FROM kb_overrides "
                      "WHERE name='presales'").fetchone()[0]
    check("kb reset removes override", v == 0)

    # 9. leads CSV
    r = client.get("/leads.csv")
    check("leads csv", r.status_code == 200
          and "email,company" in r.text)

    # 10. revoke toggle
    r = client.post("/revoke", data={"serial": "GL-TEST00000001"},
                    follow_redirects=False)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        rv = c.execute("SELECT revoked FROM licenses "
                       "WHERE serial='GL-TEST00000001'").fetchone()[0]
    check("revoke toggles", rv is True)
    client.post("/revoke", data={"serial": "GL-TEST00000001"})
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        rv = c.execute("SELECT revoked FROM licenses "
                       "WHERE serial='GL-TEST00000001'").fetchone()[0]
    check("unrevoke toggles back", rv is False)

    # 11. healthz
    r = client.get("/healthz")
    check("healthz", r.status_code == 200 and r.json()["ok"])

    # 12. logout clears
    client.get("/logout")
    r = client.get("/", follow_redirects=False)
    check("logout clears session", r.status_code == 303)

print()
if fails:
    print("FAILED:", ", ".join(fails))
    sys.exit(1)
print("ALL PASS")

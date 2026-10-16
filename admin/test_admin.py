"""Admin service smoke test — boots against a local Postgres (5433) and
checks: login flow, dashboard render, conversations/licenses/leads/emails/
kb/gateway pages, KB save + reset, review-reply (mock Resend).

Run: docker run -d --name helpdesk-pg -p 5433:5432 -e POSTGRES_PASSWORD=test \
       -e POSTGRES_DB=helpdesk postgres:16-alpine
     ..\.venv\Scripts\python.exe test_admin.py   (from admin/)
"""
import os
import sys
import threading
import time

os.environ.setdefault("DATABASE_URL",
                      "postgresql://postgres:test@127.0.0.1:5433/helpdesk")
os.environ.setdefault("ADMIN_KEY", "test-admin-key")
os.environ.pop("RESEND_API_KEY", None)

# ── fixture "site" on :8436 — the FAQ-sync source ─────────────────────────
from http.server import BaseHTTPRequestHandler, HTTPServer  # noqa: E402

FAQ_HTML = """<html><body>
<div class="faq-item">
    <p class="faq-q">What happens when the trial ends?</p>
    <p class="faq-a">Local inference keeps working — <a href="#x">free</a>.
      Cloud serving pauses until you enter a key.</p>
</div>
<div class="faq-item">
    <p class="faq-q">Does our data pass through your servers?</p>
    <p class="faq-a">No. Gatelodge is self-hosted.</p>
</div>
</body></html>""".encode()


class _FixtureSite(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(FAQ_HTML)

    def log_message(self, *a):
        pass


_osrv = HTTPServer(("127.0.0.1", 8436), _FixtureSite)
threading.Thread(target=_osrv.serve_forever, daemon=True).start()
os.environ.setdefault("SITE_FAQ_URL", "http://127.0.0.1:8436/faq")

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
    c.execute("""CREATE TABLE IF NOT EXISTS inbound_emails (
        id TEXT PRIMARY KEY, from_addr TEXT NOT NULL,
        subject TEXT DEFAULT '', body TEXT DEFAULT '',
        status TEXT NOT NULL DEFAULT 'review', confident BOOLEAN,
        doc TEXT, topic TEXT, reply TEXT,
        created_at TIMESTAMPTZ DEFAULT now(),
        handled_at TIMESTAMPTZ)""")
    c.execute("""CREATE TABLE IF NOT EXISTS contacts (
        id BIGSERIAL PRIMARY KEY, email TEXT UNIQUE NOT NULL,
        name TEXT DEFAULT '', company TEXT DEFAULT '',
        notes TEXT DEFAULT '', tags TEXT DEFAULT '',
        source TEXT DEFAULT 'manual', first_seen TEXT DEFAULT '',
        last_seen TEXT DEFAULT '', message_count INTEGER DEFAULT 0)""")
    c.execute("ALTER TABLE chat_sessions "
              "ADD COLUMN IF NOT EXISTS last_topic TEXT")
    c.execute("ALTER TABLE email_log "
              "ADD COLUMN IF NOT EXISTS subject TEXT DEFAULT ''")
    c.execute("ALTER TABLE email_log "
              "ADD COLUMN IF NOT EXISTS body TEXT DEFAULT ''")
    c.execute("""CREATE TABLE IF NOT EXISTS portal_clients (
        id BIGSERIAL PRIMARY KEY,
        name TEXT NOT NULL, company TEXT DEFAULT '',
        email TEXT DEFAULT '', access_key TEXT UNIQUE NOT NULL,
        brand_name TEXT DEFAULT '', enabled BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT now())""")
    c.execute("""CREATE TABLE IF NOT EXISTS portal_tasks (
        id BIGSERIAL PRIMARY KEY,
        client_id BIGINT NOT NULL,
        name TEXT NOT NULL DEFAULT '',
        goal TEXT NOT NULL DEFAULT '',
        success_criteria TEXT DEFAULT '', inputs TEXT DEFAULT '',
        schedule TEXT DEFAULT 'manual', enabled BOOLEAN DEFAULT TRUE,
        next_run_at TIMESTAMPTZ, last_run_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT now())""")
    # deterministic: own the tables for this run
    for t in ("chat_messages", "chat_sessions", "licenses", "leads",
              "email_log", "admin_audit", "kb_overrides", "inbound_emails",
              "contacts", "portal_clients"):
        c.execute(f"TRUNCATE {t}")
    c.execute("INSERT INTO chat_sessions (id, email, needs_review, "
              "last_topic) VALUES ('testsess0001', 'cust@example.com', "
              "TRUE, 'trial-question') ON CONFLICT (id) DO UPDATE SET "
              "needs_review = TRUE, email = 'cust@example.com', "
              "last_topic = 'trial-question'")
    c.execute("INSERT INTO chat_messages (session_id, role, content) "
              "VALUES ('testsess0001', 'user', 'how do I install a key?')")
    c.execute("INSERT INTO chat_messages (session_id, role, content, "
              "confident, doc) VALUES ('testsess0001', 'assistant', "
              "'answered ok', TRUE, 'presales')")
    c.execute("INSERT INTO inbound_emails (id, from_addr, subject, body, "
              "status, topic, doc) VALUES "
              "('inb-a', 'waiting@example.com', 'Trial question', "
              "'How long is the trial?', 'review', 'trial', 'faq'), "
              "('inb-b', 'done@example.com', 'Answered', 'thanks', "
              "'answered', 'what-is', 'presales')")
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

    # 8. KB save (structured) + reset
    r = client.post("/kb/save", data={
        "name": "presales", "title": "Presales — what Gatelodge is",
        "description": "Product overview",
        "content": "## edited body"}, follow_redirects=False)
    check("kb save redirects", r.status_code == 303)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT content FROM kb_overrides "
                      "WHERE name='presales'").fetchone()
    check("kb override stored with header",
          v is not None and "edited body" in v[0]
          and v[0].startswith("# Presales")
          and "title: Presales — what Gatelodge is" in v[0]
          and "description: Product overview" in v[0], str(v)[:100])
    client.get("/kb/reset", params={"name": "presales"})
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT COUNT(*) FROM kb_overrides "
                      "WHERE name='presales'").fetchone()[0]
    check("kb reset removes override", v == 0)

    # 8b. header parse/assemble round-trip
    t, d, b = app._parse_doc(
        "# T\ntitle: T\ndescription: D\n\nbody line")
    check("parse_doc splits header", (t, d, b) == ("T", "D", "body line"),
          f"{t!r} {d!r} {b!r}")
    t, d, b = app._parse_doc("just a body, no header")
    check("parse_doc tolerant (no header)", (t, d, b) == ("", "", "just a body, no header"))
    check("assemble_doc round-trip",
          app._parse_doc(app._assemble_doc("T", "D", "body"))
          == ("T", "D", "body"))

    # 8c. new document (create) -> listed as custom
    r = client.post("/kb/new", data={
        "name": "channel-faq", "title": "Channel partners — FAQ",
        "description": "Partner questions about co-branded trials",
        "content": "## How long is the trial?\n\n90 days."},
        follow_redirects=False)
    check("kb new redirects to edit", r.status_code == 303
          and "/kb/edit?name=channel-faq" in r.headers.get("location", ""))
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT content FROM kb_overrides "
                      "WHERE name='channel-faq'").fetchone()
    check("custom doc stored with header", v is not None
          and "title: Channel partners — FAQ" in v[0]
          and "90 days" in v[0])
    r = client.get("/kb")
    check("custom doc listed", "channel-faq" in r.text
          and "custom" in r.text)
    r = client.get("/kb/edit", params={"name": "channel-faq"})
    check("edit form parses header fields",
          r.status_code == 200
          and 'value="Channel partners — FAQ"' in r.text
          and 'value="Partner questions about co-branded trials"' in r.text
          and "90 days" in r.text)

    # 8d. validation: bad name, missing fields, built-in collision
    r = client.post("/kb/new", data={
        "name": "Bad Name!", "title": "t", "description": "d",
        "content": "c"}, follow_redirects=False)
    check("kb new rejects bad name", r.status_code == 303
          and "name+must+be" in r.headers.get("location", ""))
    r = client.post("/kb/new", data={
        "name": "fine-name", "title": "", "description": "d",
        "content": "c"}, follow_redirects=False)
    check("kb new requires fields", r.status_code == 303
          and "required" in r.headers.get("location", ""))
    r = client.post("/kb/new", data={
        "name": "presales", "title": "t", "description": "d",
        "content": "c"}, follow_redirects=False)
    check("kb new rejects built-in collision", r.status_code == 303
          and "already+exists" in r.headers.get("location", ""))

    # 8e. remove the custom doc
    client.get("/kb/reset", params={"name": "channel-faq"})
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT COUNT(*) FROM kb_overrides "
                      "WHERE name='channel-faq'").fetchone()[0]
    check("kb reset removes custom doc", v == 0)

    # 8f. /kb/new GET page renders + prefills from gap suggestions
    r = client.get("/kb/new", params={
        "title": "Questions about Enterprise Pricing",
        "description": "clustered from 3 questions",
        "content": "## how much?\n\n(answer pending)"})
    check("kb new page prefills", r.status_code == 200
          and 'value="Questions about Enterprise Pricing"' in r.text
          and "(answer pending)" in r.text)

    # 8g. knowledge gaps — seeded questions cluster into a suggestion
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        for i, q in enumerate([
                "how much does the enterprise edition cost per month",
                "what is the enterprise pricing for 50 users",
                "do you offer enterprise plans and pricing",
                "what is the capital of France"]):
            sid = f"gaptest{i:04d}"
            c.execute("INSERT INTO chat_sessions (id, needs_review) "
                      "VALUES (%s, TRUE) ON CONFLICT (id) DO NOTHING",
                      (sid,))
            c.execute("INSERT INTO chat_messages (session_id, role, "
                      "content) VALUES (%s, 'user', %s)", (sid, q))
    r = client.get("/kb")
    check("gap cluster suggested", "draft document" in r.text
          and "enterprise" in r.text.lower()
          and "questions" in r.text.lower(), r.text[:200])
    check("gap examples rendered", "enterprise pricing" in r.text.lower())

    # 8h. FAQ HTML parser (site -> KB sync)
    items = app._parse_faq_html(FAQ_HTML.decode())
    check("faq parser extracts pairs", len(items) == 2
          and items[0][0] == "What happens when the trial ends?"
          and "Local inference keeps working — free" in items[0][1]
          and items[1][0].startswith("Does our data"), str(items)[:160])

    # 8i. FAQ sync end-to-end (fixture site -> faq doc)
    r = client.post("/kb/sync_faq", follow_redirects=False)
    check("sync redirects", r.status_code == 303
          and "synced+2" in r.headers.get("location", ""))
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT content FROM kb_overrides "
                      "WHERE name='faq'").fetchone()
    check("faq synced with header + Q&As",
          v is not None and "## What happens when the trial ends?" in v[0]
          and "## Does our data pass through your servers?" in v[0]
          and v[0].startswith("# FAQ"), str(v)[:120])
    client.get("/kb/reset", params={"name": "faq"})
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        for i in range(4):
            c.execute("DELETE FROM chat_messages WHERE session_id = %s",
                      (f"gaptest{i:04d}",))
            c.execute("DELETE FROM chat_sessions WHERE id = %s",
                      (f"gaptest{i:04d}",))

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

    # 11. inbox (inbound email review)
    r = client.get("/inbox")
    check("inbox renders", r.status_code == 200
          and "waiting@example.com" in r.text
          and "How long is the trial?" in r.text
          and "done@example.com" in r.text)
    r = client.post("/inbox/reply", data={
        "id": "inb-a", "reply": "The trial runs 30 days from first boot."},
        follow_redirects=False)
    check("inbox reply redirects", r.status_code == 303)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT status, reply FROM inbound_emails "
                      "WHERE id='inb-a'").fetchone()
        el = c.execute("SELECT COUNT(*) FROM email_log WHERE kind="
                       "'inbox-reply'").fetchone()[0]
    check("inbox reply stored + logged",
          v and v[0] == "answered" and "30 days" in v[1] and el == 1,
          str(v))
    r = client.get("/inbox/ignore", params={"id": "inb-b"},
                   follow_redirects=False)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT status FROM inbound_emails "
                      "WHERE id='inb-b'").fetchone()
    check("inbox ignore", r.status_code == 303 and v[0] == "ignored")

    # 12. analytics
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        c.execute("INSERT INTO chat_sessions (id, needs_review, "
                  "escalated, last_topic) VALUES ('analytics1', TRUE, "
                  "TRUE, 'trial-question') ON CONFLICT (id) DO UPDATE "
                  "SET needs_review=TRUE, escalated=TRUE, "
                  "last_topic='trial-question'")
    r = client.get("/analytics")
    check("analytics renders", r.status_code == 200
          and "chat sessions" in r.text
          and "answer confidence" in r.text
          and "Knowledge-base hit rates" in r.text
          and "bar-fill" in r.text)
    check("analytics shows doc hit (presales)",
          "presales" in r.text)
    check("analytics shows escalation topic",
          "trial-question" in r.text)

    # 13. healthz
    r = client.get("/healthz")
    check("healthz", r.status_code == 200 and r.json()["ok"])

    # 14. mail room — contacts CRUD + compose + viewers
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        c.execute("INSERT INTO contacts (email, name, company, tags, "
                  "source, first_seen, last_seen, message_count) VALUES "
                  "('booked@example.com', 'Book Owner', 'Acme', "
                  "'vip,partner', 'inbound', '2026-10-01', '2026-10-10', 3)"
                  " ON CONFLICT (email) DO NOTHING")
        c.execute("INSERT INTO contacts (email, source, first_seen, "
                  "last_seen) VALUES ('trial@example.com', 'trial', "
                  "'2026-10-02', '2026-10-02')")
        c.execute("INSERT INTO email_log (kind, to_addr, serial, ok, "
                  "detail, subject, body) VALUES ('trial', "
                  "'booked@example.com', '', 1, 'ok', 'Your download', "
                  "'thank you for downloading')")

    r = client.get("/contacts")
    check("contacts screen renders", r.status_code == 200
          and "booked@example.com" in r.text
          and "trial@example.com" in r.text
          and "Acme" in r.text)
    r = client.get("/contacts", params={"q": "acme"})
    check("contacts search filters", "booked@example.com" in r.text
          and "trial@example.com" not in r.text)

    r = client.post("/contacts/save", data={
        "id": "0", "email": "manual@example.com", "name": "Manual Add",
        "company": "Newco", "tags": "prospect", "notes": "met at expo"},
        follow_redirects=False)
    check("contact create", r.status_code == 303)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT name, company, source FROM contacts "
                      "WHERE email='manual@example.com'").fetchone()
    check("created contact stored", v and v[0] == "Manual Add"
          and v[1] == "Newco" and v[2] == "manual", str(v))

    cid = None
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        cid = c.execute("SELECT id FROM contacts WHERE "
                        "email='booked@example.com'").fetchone()[0]
    r = client.get("/contacts/edit", params={"id": cid})
    check("contact edit form", r.status_code == 200
          and 'value="Book Owner"' in r.text
          and 'value="vip,partner"' in r.text)
    r = client.post("/contacts/save", data={
        "id": str(cid), "email": "", "name": "Book Owner II",
        "company": "Acme", "tags": "vip", "notes": "notes here"},
        follow_redirects=False)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT name, notes FROM contacts WHERE id=%s",
                      (cid,)).fetchone()
    check("contact edit saves", v and v[0] == "Book Owner II"
          and v[1] == "notes here", str(v))

    r = client.get("/compose", params={"to": "booked@example.com",
                                       "ref": "Question about trial"})
    check("compose form prefills", r.status_code == 200
          and 'value="booked@example.com"' in r.text
          and 'value="Re: Question about trial"' in r.text)

    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        eid = c.execute("SELECT id FROM email_log WHERE "
                        "subject='Your download'").fetchone()[0]
    r = client.get("/email/view", params={"id": eid})
    check("sent email viewer", r.status_code == 200
          and "Your download" in r.text
          and "thank you for downloading" in r.text)
    r = client.get("/inbox/view", params={"id": "inb-a"})
    check("inbound email viewer", r.status_code == 200
          and "How long is the trial?" in r.text
          and "Compose follow-up" in r.text)

    r = client.post("/contacts/bulk", data={
        "ids": [str(cid)], "subject": "Bulk test",
        "body": "Hello from the bulk sender"},
        follow_redirects=False)
    check("bulk send starts", r.status_code == 303
          and "started" in r.headers.get("location", ""))
    time.sleep(2.5)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT COUNT(*) FROM email_log WHERE kind="
                      "'bulk' AND subject='Bulk test'").fetchone()[0]
    check("bulk email logged with content", v == 1, str(v))

    r = client.get("/contacts/delete", params={"id": cid},
                   follow_redirects=False)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT COUNT(*) FROM contacts WHERE id=%s",
                      (cid,)).fetchone()[0]
    check("contact delete", r.status_code == 303 and v == 0)

    # 15. tasks + tools screens
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        c.execute("TRUNCATE tasks, task_runs, task_steps, tools, "
                  "tool_proposals RESTART IDENTITY")
        c.execute("INSERT INTO tasks (name, goal, success_criteria, "
                  "inputs, schedule) VALUES ('sweep', 'find gaps', "
                  "'draft or report', 'gap_cluster', 'daily')")
        c.execute("INSERT INTO task_runs (task_id, status, "
                  "classification, summary, report) VALUES "
                  "(1, 'achieved', 'achieved', 'did it', 'report text')")
        c.execute("INSERT INTO task_steps (run_id, seq, tool, "
                  "description, input, output, status) VALUES "
                  "(1, 0, 'db_query', 'count', 'SELECT 1', "
                  "'DRAFT RECORDED fake', 'ok')")
        c.execute("INSERT INTO tools (name, description, kind) VALUES "
                  "('db_query', 'read-only sql', 'builtin')")
        c.execute("INSERT INTO tool_proposals (capability, context) "
                  "VALUES ('send_sms', 'wanted during sweep')")
    r = client.get("/tasks")
    check("tasks screen renders", r.status_code == 200
          and "sweep" in r.text and "daily" in r.text
          and "send_sms" in r.text)
    r = client.get("/tasks/edit", params={"id": 1})
    check("task edit form", r.status_code == 200
          and "find gaps" in r.text and "draft or report" in r.text)
    r = client.post("/tasks/save", data={
        "id": "1", "name": "sweep2", "goal": "find gaps faster",
        "success_criteria": "x", "inputs": "y", "schedule": "weekly",
        "notify": "1", "enabled": "1"}, follow_redirects=False)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT name, schedule FROM tasks WHERE id=1"
                      ).fetchone()
    check("task edit saves", r.status_code == 303
          and v == ("sweep2", "weekly"), str(v))
    r = client.get("/tasks/now", params={"id": 1}, follow_redirects=False)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT run_now FROM tasks WHERE id=1").fetchone()
    check("run-now queues", r.status_code == 303 and v[0] is True)
    r = client.get("/tasks/run", params={"id": 1})
    check("task runs list", r.status_code == 200 and "did it" in r.text)
    r = client.get("/tasks/runview", params={"id": 1})
    check("run viewer with steps", r.status_code == 200
          and "report text" in r.text and "db_query" in r.text)
    r = client.get("/tools")
    check("tools registry renders", r.status_code == 200
          and "db_query" in r.text and "builtin" in r.text)
    r = client.get("/tools/approve", params={"id": 1},
                   follow_redirects=False)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT status FROM tool_proposals WHERE id=1"
                      ).fetchone()
    check("proposal approve", r.status_code == 303 and v[0] == "approved")

    # 15b. portal client provisioning
    r = client.get("/clients")
    check("clients screen", r.status_code == 200
          and "Create client + key" in r.text, r.text[:120])
    r = client.post("/clients/new", data={
        "name": "Test Partner", "company": "Partner Co",
        "email": "partner@example.com", "brand": "Partner Desk",
        "email_key": ""}, follow_redirects=False)
    loc = r.headers.get("location", "")
    check("client created with key", r.status_code == 303
          and "key:+portal-" in loc, loc)
    key = loc.split("key:+")[1] if "key:+" in loc else ""
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT name, brand_name, enabled FROM "
                      "portal_clients WHERE access_key=%s",
                      (key,)).fetchone()
        cid = c.execute("SELECT id FROM portal_clients WHERE access_key=%s",
                        (key,)).fetchone()[0]
    check("client row + key valid", v == ("Test Partner", "Partner Desk",
                                          True), str(v))
    r = client.post("/clients/toggle", data={"id": str(cid)},
                    follow_redirects=False)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        v = c.execute("SELECT enabled FROM portal_clients WHERE id=%s",
                      (cid,)).fetchone()
    check("client pause toggles", r.status_code == 303 and v[0] is False)
    r = client.post("/clients/regen", data={"id": str(cid)},
                    follow_redirects=False)
    loc = r.headers.get("location", "")
    check("client key regen", r.status_code == 303
          and "new+key+portal-" in loc, loc)
    with psycopg.connect(os.environ["DATABASE_URL"]) as c:
        c.execute("DELETE FROM portal_clients WHERE id=%s", (cid,))

    # 16. logout clears
    client.get("/logout")
    r = client.get("/", follow_redirects=False)
    check("logout clears session", r.status_code == 303)

print()
if fails:
    print("FAILED:", ", ".join(fails))
    sys.exit(1)
print("ALL PASS")

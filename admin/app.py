"""Gatelodge admin — the lodge office (LAN-only, cookie-authed).

Server-rendered gatehouse-styled UI over the shared Postgres:
dashboard+stats, conversations, review queue with approve-reply
(automated email from support@ + optional KB append), leads (+CSV),
licenses (claims/expiry/features, verify, revoke, manual issue),
knowledge-base editor (live overrides), email log, gateway panel.

Env:
  DATABASE_URL   — shared Postgres (licensing + gatekeeper tables)
  ADMIN_KEY      — login key (also the licensing admin-issue bearer)
  RESEND_API_KEY — approve-reply delivery
  SUPPORT_EMAIL  — From address for replies (default support@datagatelodge.com)
  GATEWAY_URL / GATEWAY_KEY — gateway panel (healthz/metrics)
  LICENSING_URL  — licensing service base (manual issue), default
                   http://10.1.2.217:8100
  PORT           — default 8111

Deploy bound to the VM LAN address only — NO tunnel ingress, no DNS,
unreachable from the internet by construction.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import html as _html
import json
import math
import os
import re
import time
import urllib.parse

import httpx
import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
SUPPORT_EMAIL = os.environ.get("SUPPORT_EMAIL",
                               "support@datagatelodge.com")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "").rstrip("/")
GATEWAY_KEY = os.environ.get("GATEWAY_KEY", "")
LICENSING_URL = os.environ.get("LICENSING_URL",
                               "http://10.1.2.217:8100").rstrip("/")
PORT = int(os.environ.get("PORT", "8111"))

app = FastAPI(title="Gatelodge Admin", docs_url=None, redoc_url=None)

COOKIE = "lodge_office"
TOKEN = hmac.new(ADMIN_KEY.encode(), b"gatelodge-admin-v1",
                 hashlib.sha256).hexdigest() if ADMIN_KEY else ""


def _e(s) -> str:
    return _html.escape(str(s if s is not None else ""))


# ── db helpers ────────────────────────────────────────────────────────────


def _run(fn):
    return asyncio.to_thread(fn)


def _q(sql: str, params: tuple = (), fetch: str = "all"):
    def run():
        with psycopg.connect(DATABASE_URL) as conn:
            cur = conn.execute(sql, params)
            return cur.fetchall() if fetch == "all" else cur.fetchone()
    return _run(run)


def _x(sql: str, params: tuple = ()):
    def run():
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute(sql, params)
    return _run(run)


@app.on_event("startup")
async def _init() -> None:
    if not DATABASE_URL:
        print("FATAL: DATABASE_URL unset")
        return

    def run():
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS admin_audit (
                id BIGSERIAL PRIMARY KEY,
                action TEXT NOT NULL,
                detail TEXT DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS kb_overrides (
                name TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS inbound_emails (
                id TEXT PRIMARY KEY,
                from_addr TEXT NOT NULL,
                subject TEXT DEFAULT '',
                body TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'review',
                confident BOOLEAN, doc TEXT, topic TEXT, reply TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                handled_at TIMESTAMPTZ
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS contacts (
                id BIGSERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                name TEXT DEFAULT '',
                company TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                tags TEXT DEFAULT '',
                source TEXT DEFAULT 'manual',
                first_seen TEXT DEFAULT '',
                last_seen TEXT DEFAULT '',
                message_count INTEGER DEFAULT 0
            )""")
            conn.execute("ALTER TABLE email_log "
                         "ADD COLUMN IF NOT EXISTS subject TEXT DEFAULT ''")
            conn.execute("ALTER TABLE email_log "
                         "ADD COLUMN IF NOT EXISTS body TEXT DEFAULT ''")
            conn.execute("ALTER TABLE chat_sessions "
                         "ADD COLUMN IF NOT EXISTS last_topic TEXT")
            conn.execute("""CREATE TABLE IF NOT EXISTS tasks (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                goal TEXT NOT NULL DEFAULT '',
                success_criteria TEXT DEFAULT '',
                inputs TEXT DEFAULT '',
                schedule TEXT DEFAULT 'manual',
                notify BOOLEAN DEFAULT TRUE,
                enabled BOOLEAN DEFAULT TRUE,
                run_now BOOLEAN DEFAULT FALSE,
                next_run_at TIMESTAMPTZ,
                last_run_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT now()
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS task_runs (
                id BIGSERIAL PRIMARY KEY,
                task_id BIGINT NOT NULL,
                status TEXT DEFAULT 'running',
                classification TEXT DEFAULT '',
                summary TEXT DEFAULT '',
                report TEXT DEFAULT '',
                started_at TIMESTAMPTZ DEFAULT now(),
                finished_at TIMESTAMPTZ
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS task_steps (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL,
                seq INT DEFAULT 0,
                tool TEXT DEFAULT '',
                description TEXT DEFAULT '',
                input TEXT DEFAULT '',
                output TEXT DEFAULT '',
                status TEXT DEFAULT 'ok',
                created_at TIMESTAMPTZ DEFAULT now()
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS tools (
                name TEXT PRIMARY KEY,
                description TEXT DEFAULT '',
                kind TEXT DEFAULT 'builtin',
                enabled BOOLEAN DEFAULT TRUE
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS tool_proposals (
                id BIGSERIAL PRIMARY KEY,
                capability TEXT NOT NULL,
                context TEXT DEFAULT '',
                status TEXT DEFAULT 'open',
                created_at TIMESTAMPTZ DEFAULT now()
            )""")
    await _run(run)


async def _audit(action: str, detail: str = "") -> None:
    await _x("INSERT INTO admin_audit (action, detail) VALUES (%s, %s)",
             (action, detail[:400]))


# ── auth ──────────────────────────────────────────────────────────────────


def _authed(request: Request) -> bool:
    return bool(ADMIN_KEY) and request.cookies.get(COOKIE) == TOKEN


def _guard(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    return None


# ── page chrome ───────────────────────────────────────────────────────────

NAV = [("/", "Dashboard"), ("/conversations", "Conversations"),
       ("/reviews", "Reviews"), ("/inbox", "Inbox"), ("/leads", "Leads"),
       ("/licenses", "Licenses"), ("/kb", "Knowledge"),
       ("/analytics", "Analytics"), ("/contacts", "Contacts"),
       ("/compose", "Compose"), ("/emails", "Emails"), ("/tasks", "Tasks"),
       ("/tools", "Tools"), ("/gateway", "Gateway")]

CSS = """
:root { --ink:#0e1411; --ink2:#121a15; --surface:#1a241d; --line:#2c3a30;
  --brass:#d4af37; --brassb:#e8ca6a; --paper:#ece7dd; --muted:#9aa79d;
  --faint:#6d7a70; --ok:#57b481; --danger:#c4705f; }
* { margin:0; padding:0; box-sizing:border-box; }
body { background:radial-gradient(1100px 500px at 85% -5%,
  rgba(212,175,55,.06), transparent 60%), var(--ink); color:var(--paper);
  font-family:'Segoe UI',system-ui,sans-serif; font-size:15px; line-height:1.6; }
.mono { font-family:Consolas,'Courier New',monospace; }
a { color:var(--brassb); text-decoration:none; }
header { display:flex; align-items:center; gap:1.4rem; padding:.8rem 1.6rem;
  border-bottom:2px solid var(--line); background:var(--ink2);
  position:sticky; top:0; z-index:10; }
header .brand { font-family:Georgia,serif; font-size:1.05rem;
  letter-spacing:.05em; color:var(--brass); font-weight:600; }
nav { display:flex; gap:1.1rem; margin-left:auto; flex-wrap:wrap; }
nav a { color:var(--muted); font-size:.82rem; text-transform:uppercase;
  letter-spacing:.08em; }
nav a:hover, nav a.on { color:var(--brassb); }
main { max-width:1160px; margin:0 auto; padding:1.8rem 1.6rem 4rem; }
h1 { font-family:Georgia,serif; font-weight:500; font-size:1.7rem;
  margin-bottom:1.4rem; }
h1 em { color:var(--brassb); font-style:italic; }
.cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr));
  gap:.9rem; margin-bottom:1.8rem; }
.card { background:var(--surface); border:1px solid var(--line);
  border-radius:12px; padding:1rem 1.1rem; }
.card .num { font-family:Georgia,serif; font-size:1.9rem; color:var(--brassb); }
.card .lbl { font-size:.62rem; letter-spacing:.16em; text-transform:uppercase;
  color:var(--faint); margin-top:.15rem; }
table { width:100%; border-collapse:collapse; font-size:.86rem;
  margin-bottom:1.6rem; }
th { text-align:left; font-weight:400; color:var(--faint); font-size:.68rem;
  text-transform:uppercase; letter-spacing:.1em; padding:.5rem .6rem;
  border-bottom:1px solid var(--line); }
td { padding:.55rem .6rem; border-bottom:1px solid #243028;
  vertical-align:top; }
tr:hover td { background:rgba(212,175,55,.03); }
.tag { display:inline-block; font-size:.6rem; letter-spacing:.1em;
  text-transform:uppercase; padding:.12rem .45rem; border-radius:4px;
  border:1px solid var(--line); color:var(--muted); }
.tag.ok { color:var(--ok); border-color:var(--ok); }
.tag.warn { color:var(--brassb); border-color:var(--brass); }
.tag.bad { color:var(--danger); border-color:var(--danger); }
.pill { font-family:Consolas,monospace; font-size:.78rem; color:var(--brassb); }
.btn { display:inline-block; background:var(--brass); color:var(--ink);
  border:none; border-radius:8px; padding:.5rem 1.1rem; font-weight:600;
  font-size:.85rem; cursor:pointer; }
.btn:hover { background:var(--brassb); }
.btn.ghost { background:transparent; color:var(--paper);
  border:1px solid var(--line); }
input[type=text],input[type=email],select,textarea {
  background:var(--ink2); color:var(--paper); border:1px solid var(--line);
  border-radius:8px; padding:.5rem .7rem; font-size:.88rem;
  font-family:inherit; }
textarea { width:100%; min-height:340px; font-family:Consolas,monospace;
  font-size:.82rem; line-height:1.55; }
.msg { background:var(--surface); border:1px solid var(--line);
  border-left:3px solid var(--brass); border-radius:8px;
  padding:.7rem .9rem; margin-bottom:.7rem; font-size:.88rem;
  max-width:760px; white-space:pre-wrap; }
.msg.user { border-left-color:var(--muted); margin-left:2.5rem; }
.note { color:var(--faint); font-size:.75rem; }
form.inline { display:flex; gap:.6rem; align-items:center; flex-wrap:wrap; }
.box { background:var(--surface); border:1px solid var(--line);
  border-radius:12px; padding:1.3rem 1.4rem; margin-bottom:1.4rem; }
kbd.code { font-family:Consolas,monospace; background:var(--ink2);
  border:1px solid var(--line); border-radius:4px; padding:.1rem .35rem;
  font-size:.8rem; color:var(--brassb); }
.grid2 { display:grid; grid-template-columns:1fr 1fr; gap:1.2rem; }
@media (max-width:900px){ .grid2 { grid-template-columns:1fr; } }
.flash { background:rgba(87,180,129,.12); border:1px solid var(--ok);
  border-radius:8px; padding:.6rem .9rem; margin-bottom:1.2rem;
  font-size:.85rem; }
.bars { margin-bottom:1.6rem; }
.bar-row { display:flex; align-items:center; gap:.6rem; margin:.28rem 0;
  font-size:.8rem; }
.bar-label { width:130px; color:var(--muted); text-align:right; flex:none;
  overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.bar-track { flex:1; background:var(--ink2); border-radius:4px; height:14px;
  overflow:hidden; }
.bar-fill { height:100%; background:linear-gradient(90deg,var(--brass),
  var(--brassb)); border-radius:4px; }
.bar-fill.dim { background:var(--line); }
.bar-num { width:52px; color:var(--paper); font-size:.75rem; flex:none; }
.bigstats { display:grid; grid-template-columns:repeat(auto-fit,
  minmax(150px,1fr)); gap:.9rem; margin-bottom:1.6rem; }
.view-body { background:var(--ink2); border:1px solid var(--line);
  border-radius:10px; padding:1.1rem 1.3rem; white-space:pre-wrap;
  font-size:.88rem; line-height:1.65; max-width:860px; }
.bulk-status { background:rgba(212,175,55,.1); border:1px solid var(--brass);
  border-radius:8px; padding:.6rem .9rem; margin-bottom:1rem;
  font-size:.85rem; }
"""

SHELL = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>The Lodge Office — Gatelodge Admin</title>
<meta name="robots" content="noindex,nofollow">
<style>{css}</style></head><body>
<header><span class="brand">THE LODGE OFFICE</span>
<nav>{nav}</nav></header><main>{body}</main></body></html>"""


def page(request: Request, title: str, body: str, flash: str = "") -> str:
    active = request.url.path
    nav = "".join(
        f'<a href="{u}"{" class=on" if u == active else ""}>{l}</a>'
        for u, l in NAV)
    h1 = f"<h1>{title}</h1>" if title else ""
    fl = f'<div class="flash">{_e(flash)}</div>' if flash else ""
    return SHELL.format(css=CSS, nav=nav, body=h1 + fl + body)


# ── views ─────────────────────────────────────────────────────────────────


@app.get("/login", response_class=HTMLResponse)
async def login(request: Request):
    body = ('<div class="box" style="max-width:420px;margin:4rem auto">'
            '<p class="mono" style="color:var(--brass);font-size:.7rem;'
            'letter-spacing:.2em">RESTRICTED — LODGE OFFICE</p>'
            '<form method="post" action="/login" class="inline">'
            '<input type="password" name="key" placeholder="admin key" '
            'style="flex:1" autofocus> <button class="btn">Enter</button>'
            "</form></div>")
    return page(request, "", body)


@app.post("/login")
async def do_login(request: Request):
    form = await request.form()
    if form.get("key") == ADMIN_KEY:
        r = RedirectResponse("/", status_code=303)
        r.set_cookie(COOKIE, TOKEN, httponly=True, samesite="lax")
        return r
    await asyncio.sleep(0.8)
    return RedirectResponse("/login", status_code=303)


@app.get("/logout")
async def logout():
    r = RedirectResponse("/login", status_code=303)
    r.delete_cookie(COOKIE)
    return r


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    if (r := _guard(request)):
        return r

    async def one(sql):
        return (await _q(sql, fetch="one") or [0])[0]

    n_lic = await one("SELECT COUNT(*) FROM licenses "
                      "WHERE kind = 'paid-key'")
    n_manual = await one("SELECT COUNT(*) FROM licenses "
                         "WHERE kind = 'manual'")
    n_leads = await one("SELECT COUNT(*) FROM leads")
    n_chats = await one("SELECT COUNT(*) FROM chat_sessions")
    n_reviews = await one("SELECT COUNT(*) FROM chat_sessions "
                          "WHERE needs_review")
    n_msgs = await one("SELECT COUNT(*) FROM chat_messages "
                       "WHERE role = 'assistant'")
    conf = await one("SELECT COUNT(*) FROM chat_messages "
                     "WHERE role = 'assistant' AND confident")
    revenue = n_lic * 99

    top_docs = await _q(
        "SELECT doc, COUNT(*) FROM chat_messages "
        "WHERE role = 'assistant' AND doc IS NOT NULL "
        "GROUP BY doc ORDER BY 2 DESC LIMIT 6")
    week = await _q(
        "SELECT to_char(date_trunc('day', created_at), 'MM-DD') d, "
        "COUNT(*) FROM chat_sessions WHERE created_at > now() - "
        "interval '7 days' GROUP BY 1 ORDER BY 1")
    recent_lic = await _q(
        "SELECT serial, email, edition, kind, created_at FROM licenses "
        "ORDER BY id DESC LIMIT 6")
    recent_chat = await _q(
        "SELECT id, created_at, email, escalated, needs_review, "
        "message_count FROM chat_sessions ORDER BY created_at DESC LIMIT 6")

    rate = f"{100 * conf / n_msgs:.0f}%" if n_msgs else "—"

    body = f"""
<div class="cards">
  <div class="card"><div class="num">${revenue}</div><div class="lbl">revenue · {n_lic} paid keys</div></div>
  <div class="card"><div class="num">{n_manual}</div><div class="lbl">manual licenses</div></div>
  <div class="card"><div class="num">{n_leads}</div><div class="lbl">trial leads</div></div>
  <div class="card"><div class="num">{n_chats}</div><div class="lbl">chat sessions</div></div>
  <div class="card"><div class="num">{rate}</div><div class="lbl">answer confidence</div></div>
  <div class="card"><div class="num">{n_reviews}</div><div class="lbl">pending reviews</div></div>
</div>
<div class="grid2">
<div class="box"><p class="mono note" style="margin-bottom:.6rem">TOP KNOWLEDGE DOCS</p>
<table><tr><th>doc</th><th>answers</th></tr>
{''.join(f'<tr><td class="pill">{_e(d)}</td><td>{n}</td></tr>' for d, n in top_docs) or '<tr><td colspan=2 class="note">no chats yet</td></tr>'}
</table></div>
<div class="box"><p class="mono note" style="margin-bottom:.6rem">SESSIONS · LAST 7 DAYS</p>
<table><tr><th>day</th><th>new</th></tr>
{''.join(f'<tr><td class="pill">{_e(d)}</td><td>{n}</td></tr>' for d, n in week) or '<tr><td colspan=2 class="note">none</td></tr>'}
</table></div>
</div>
<div class="box"><p class="mono note" style="margin-bottom:.6rem">RECENT LICENSES</p>
<table><tr><th>serial</th><th>email</th><th>edition</th><th>kind</th><th>issued</th></tr>
{''.join(f'<tr><td class="pill">{_e(s)}</td><td>{_e(e)}</td><td>{_e(ed)}</td><td>{_e(k)}</td><td class="note">{_e(c)}</td></tr>' for s, e, ed, k, c in recent_lic) or '<tr><td colspan=5 class="note">none yet</td></tr>'}
</table></div>
<div class="box"><p class="mono note" style="margin-bottom:.6rem">RECENT CHATS</p>
<table><tr><th>session</th><th>when</th><th>email</th><th>msgs</th><th>state</th></tr>
{''.join(f'<tr><td><a href="/conversation?id={i}">{i[:10]}…</a></td><td class="note">{_e(c)}</td><td>{_e(e) or "—"}</td><td>{m}</td><td>{'<span class="tag bad">review</span>' if rv else '<span class="tag ok">ok</span>'}</td></tr>' for i, c, e, es, rv, m in recent_chat) or '<tr><td colspan=5 class="note">none yet</td></tr>'}
</table></div>"""
    return page(request, "The gatehouse <em>at a glance</em>", body)


@app.get("/conversations", response_class=HTMLResponse)
async def conversations(request: Request, q: str = ""):
    if (r := _guard(request)):
        return r
    if q:
        rows = await _q(
            "SELECT s.id, s.created_at, s.email, s.escalated, "
            "s.needs_review, s.message_count FROM chat_sessions s "
            "WHERE s.id IN (SELECT session_id FROM chat_messages "
            "WHERE content ILIKE %s) OR s.email ILIKE %s "
            "ORDER BY s.created_at DESC LIMIT 100",
            (f"%{q}%", f"%{q}%"))
    else:
        rows = await _q(
            "SELECT id, created_at, email, escalated, needs_review, "
            "message_count FROM chat_sessions "
            "ORDER BY created_at DESC LIMIT 100")
    body = f"""
<form class="inline" style="margin-bottom:1.2rem" method="get">
  <input type="text" name="q" value="{_e(q)}" placeholder="search messages or email…" style="flex:1">
  <button class="btn ghost">Search</button></form>
<table><tr><th>session</th><th>when</th><th>email</th><th>msgs</th><th>state</th></tr>
{''.join(f'<tr><td><a href="/conversation?id={i}">{i[:10]}…</a></td><td class="note">{_e(c)}</td><td>{_e(e) or "—"}</td><td>{m}</td><td>{'<span class="tag bad">review</span>' if rv else ('<span class="tag warn">escalated</span>' if es else '<span class="tag ok">ok</span>')}</td></tr>' for i, c, e, es, rv, m in rows) or '<tr><td colspan=5 class="note">no sessions</td></tr>'}
</table>"""
    return page(request, "Conversations", body)


@app.get("/conversation", response_class=HTMLResponse)
async def conversation(request: Request, id: str = ""):
    if (r := _guard(request)):
        return r
    sess = await _q("SELECT id, created_at, ip, email, escalated, "
                    "needs_review, message_count FROM chat_sessions "
                    "WHERE id = %s", (id,), fetch="one")
    if not sess:
        return page(request, "Not found", "<p class='note'>no such session</p>")
    msgs = await _q("SELECT role, content, confident, doc, created_at "
                    "FROM chat_messages WHERE session_id = %s ORDER BY id",
                    (id,))
    bubbles = "".join(
        f'<div class="msg {("user" if r == "user" else "")}">'
        f"{_e(c)}\n"
        + (f'<span class="note">doc={_e(d)} confident={int(bool(cf))}</span>'
           if r == "assistant" else "")
        + "</div>"
        for r, c, cf, d, at in msgs)
    review = ""
    if sess[6] or sess[5]:      # escalated or needs_review
        docs = await _q("SELECT name FROM kb_overrides")
        kb_names = sorted(set(
            ["presales", "licensing", "eula-summary", "console",
             "backends", "governance", "troubleshooting"]
            + [d[0] for d in docs]))
        opts = "".join(f"<option>{_e(n)}</option>" for n in kb_names)
        review = f"""
<div class="box"><p class="mono note" style="margin-bottom:.7rem">APPROVED REPLY — emailed from {_e(SUPPORT_EMAIL)}</p>
<form method="post" action="/review-reply">
<input type="hidden" name="session_id" value="{_e(id)}">
<textarea name="reply" style="min-height:120px" placeholder="Type the answer the customer should receive…"></textarea>
<div class="inline" style="margin-top:.7rem">
<label class="note"><input type="checkbox" name="add_to_kb" value="1" checked> append Q&amp;A to knowledge</label>
<select name="doc">{opts}</select>
<button class="btn">Send reply</button>
<a class="btn ghost" href="/review-done?session_id={_e(id)}">just mark reviewed</a>
</div></form></div>"""
    meta = (f"<p class='note'>session {_e(sess[0])} · {_e(sess[1])} · "
            f"ip {_e(sess[2])} · email {_e(sess[3]) or '—'} · "
            f"{sess[6]} messages</p>")
    return page(request, "Conversation", meta + review + bubbles)


@app.post("/review-reply")
async def review_reply(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    sid = str(form.get("session_id", ""))
    reply = str(form.get("reply", "")).strip()
    add_kb = form.get("add_to_kb") == "1"
    doc = str(form.get("doc", "troubleshooting"))
    if not reply:
        return RedirectResponse(f"/conversation?id={sid}", status_code=303)

    # last user question (for the KB entry)
    row = await _q("SELECT content FROM chat_messages WHERE session_id=%s "
                   "AND role='user' ORDER BY id DESC LIMIT 1",
                   (sid,), fetch="one")
    question = row[0] if row else ""

    # email the customer
    sess = await _q("SELECT email FROM chat_sessions WHERE id=%s",
                    (sid,), fetch="one")
    to = sess[0] if sess else None
    sent = False
    if to and RESEND_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.post(
                    "https://api.resend.com/emails",
                    json={"from": SUPPORT_EMAIL, "reply_to": to,
                          "to": [to],
                          "subject": "Your Gatelodge question — answered",
                          "text": (f"From your chat with the Gatekeeper:\n\n"
                                   f"Q: {question}\n\n{reply}\n\n"
                                   f"— The Gatelodge team")},
                    headers={"Authorization":
                             f"Bearer {RESEND_API_KEY}"})
                sent = r.status_code in (200, 201)
        except Exception:
            sent = False
        await _x("INSERT INTO email_log (kind, to_addr, serial, ok, detail,"
                 " subject, body) VALUES ('admin-reply', %s, %s, %s, %s,"
                 " %s, %s)",
                 (to, sid[:12], int(sent), "resend ok" if sent else "failed",
                  "Your Gatelodge question — answered",
                  f"From your chat with the Gatekeeper:\n\n"
                  f"Q: {question}\n\n{reply}\n\n— The Gatelodge team"))
        await _contact_upsert(to)

    # append Q&A to the KB override
    if add_kb and question:
        base = await _q("SELECT content FROM kb_overrides WHERE name=%s",
                        (doc,), fetch="one")
        if base:
            content = base[0] + f"\n\n## Q: {question}\nA: {reply}\n"
            await _x("UPDATE kb_overrides SET content=%s, "
                     "updated_at=now() WHERE name=%s", (content, doc))
        else:
            # seed the override from the gatekeeper's built-in doc
            import pathlib
            p = (pathlib.Path(__file__).resolve().parent
                 / "kb" / f"{doc}.md")
            seed = p.read_text(encoding="utf-8") if p.exists() else (
                f"title: {doc}\ndescription: (generated)\n")
            await _x("INSERT INTO kb_overrides (name, content) "
                     "VALUES (%s, %s)",
                     (doc, seed + f"\n\n## Q: {question}\nA: {reply}\n"))

    await _x("UPDATE chat_sessions SET needs_review=FALSE WHERE id=%s",
             (sid,))
    await _audit("review-reply",
                 f"{sid[:12]} kb={doc if add_kb else '-'} "
                 f"emailed={'yes' if sent else 'no'}")
    return RedirectResponse(f"/conversation?id={sid}&sent={int(sent)}",
                            status_code=303)


@app.get("/review-done")
async def review_done(request: Request, session_id: str = ""):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    await _x("UPDATE chat_sessions SET needs_review=FALSE WHERE id=%s",
             (session_id,))
    await _audit("review-done", session_id[:12])
    return RedirectResponse("/reviews", status_code=303)


@app.get("/reviews", response_class=HTMLResponse)
async def reviews(request: Request):
    if (r := _guard(request)):
        return r
    rows = await _q(
        "SELECT s.id, s.created_at, s.email, "
        "(SELECT content FROM chat_messages m WHERE m.session_id=s.id "
        "AND m.role='user' ORDER BY m.id DESC LIMIT 1) "
        "FROM chat_sessions s WHERE s.needs_review "
        "ORDER BY s.created_at DESC LIMIT 50")
    body = f"""
<table><tr><th>session</th><th>when</th><th>email</th><th>last question</th></tr>
{''.join(f'<tr><td><a href="/conversation?id={i}">{i[:10]}…</a></td><td class="note">{_e(c)}</td><td>{_e(e) or "—"}</td><td>{_e(q)[:110]}</td></tr>' for i, c, e, q in rows) or '<tr><td colspan=4 class="note">queue is clear — the gate stands</td></tr>'}
</table>"""
    return page(request, "Review queue", body)


@app.get("/leads", response_class=HTMLResponse)
async def leads(request: Request):
    if (r := _guard(request)):
        return r
    rows = await _q("SELECT email, business_name, ip, created_at "
                    "FROM leads ORDER BY id DESC LIMIT 200")
    body = f"""
<p style="margin-bottom:1rem"><a class="btn ghost" href="/leads.csv">export CSV</a></p>
<table><tr><th>email</th><th>company</th><th>ip</th><th>when</th></tr>
{''.join(f'<tr><td>{_e(e)}</td><td>{_e(b) or "—"}</td><td class="note">{_e(ip)}</td><td class="note">{_e(c)}</td></tr>' for e, b, ip, c in rows) or '<tr><td colspan=4 class="note">no leads yet</td></tr>'}
</table>"""
    return page(request, "Leads", body)


@app.get("/leads.csv")
async def leads_csv(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    rows = await _q("SELECT email, business_name, ip, created_at "
                    "FROM leads ORDER BY id")
    csv = "email,company,ip,created_at\n" + "".join(
        f'"{e}","{b}","{ip}","{c}"\n' for e, b, ip, c in rows)
    return Response(csv, media_type="text/csv",
                    headers={"Content-Disposition":
                             "attachment; filename=leads.csv"})


def _claims(license_json: str) -> dict:
    try:
        token = json.loads(license_json)["license"]
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception:
        return {}


@app.get("/licenses", response_class=HTMLResponse)
async def licenses(request: Request, flash: str = ""):
    if (r := _guard(request)):
        return r
    rows = await _q("SELECT serial, email, edition, kind, stripe_event, "
                    "created_at, license_json, revoked FROM licenses "
                    "ORDER BY id DESC LIMIT 200")
    trs = []
    for s, e, ed, k, ev, c, lj, rv in rows:
        cl = _claims(lj)
        exp = cl.get("exp")
        exp_s = (time.strftime("%Y-%m-%d", time.localtime(exp))
                 if exp else "—")
        provs = ", ".join(cl.get("providers", [])[:4]) or "—"
        trs.append(
            f"<tr><td class='pill'>{_e(s)}</td><td>{_e(e)}</td>"
            f"<td>{_e(ed)}</td><td>{_e(k)}</td>"
            f"<td class='note'>{_e(exp_s)}</td>"
            f"<td class='note' style='max-width:180px'>{_e(provs)}</td>"
            f"<td>{'<span class=tag bad>revoked</span>' if rv else '<span class=tag ok>active</span>'}</td>"
            f"<td><form method=post action=/revoke class=inline>"
            f"<input type=hidden name=serial value='{_e(s)}'>"
            f"<button class='btn ghost' style='padding:.2rem .6rem;"
            f"font-size:.7rem'>{'unrevoke' if rv else 'revoke'}</button>"
            f"</form></td></tr>")
    body = f"""
<div class="box"><p class="mono note" style="margin-bottom:.7rem">ISSUE A LICENSE MANUALLY</p>
<form method="post" action="/issue" class="inline">
<input type="email" name="email" placeholder="customer email" required>
<input type="text" name="business_name" placeholder="company (opt)">
<select name="edition"><option>community</option><option>enterprise</option></select>
<select name="duration"><option>perpetual</option><option>1y</option>
<option>2y</option><option>30d</option><option>60d</option><option>90d</option><option>3y</option></select>
<input type="text" name="providers" placeholder="providers (opt, comma-sep)" style="min-width:220px">
<button class="btn">Issue + email</button></form></div>
<table><tr><th>serial</th><th>email</th><th>edition</th><th>kind</th>
<th>expiry</th><th>providers</th><th>state</th><th></th></tr>
{''.join(trs) or '<tr><td colspan=8 class=note>no licenses yet</td></tr>'}
</table>"""
    return page(request, "Licenses", body, flash=flash)


@app.post("/revoke")
async def revoke(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    serial = str(form.get("serial", ""))
    await _x("UPDATE licenses SET revoked = NOT revoked WHERE serial=%s",
             (serial,))
    await _audit("revoke", serial)
    return RedirectResponse("/licenses", status_code=303)


@app.post("/issue")
async def issue(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    providers = [p.strip() for p in
                 str(form.get("providers", "")).split(",") if p.strip()]
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{LICENSING_URL}/api/admin/issue",
                json={"email": str(form.get("email", "")),
                      "edition": str(form.get("edition", "community")),
                      "business_name": str(form.get("business_name", "")),
                      "duration": str(form.get("duration", "perpetual")),
                      "providers": providers},
                headers={"Authorization": f"Bearer {ADMIN_KEY}"})
        if r.status_code == 200:
            serial = r.json().get("serial", "?")
            await _audit("issue", serial)
            return RedirectResponse(
                f"/licenses?flash=issued+{_e(serial)}+(emailed)",
                status_code=303)
        return RedirectResponse(
            f"/licenses?flash=issue+failed:+{_e(r.text[:80])}",
            status_code=303)
    except Exception as e:
        return RedirectResponse(
            f"/licenses?flash=issue+error:+{_e(repr(e)[:60])}",
            status_code=303)


# ── knowledge base (document CRUD + live overrides) ────────────────────────


def _kb_dir():
    import pathlib
    return pathlib.Path(__file__).resolve().parent / "kb"


def _parse_doc(content: str) -> tuple[str, str, str]:
    """Split a KB doc into (title, description, body).

    Docs carry a small header the Gatekeeper's triage stage routes on:
        # Title
        title: …
        description: …
        <markdown body>
    Tolerant: if the header lines are missing, everything is body.
    """
    lines = content.splitlines()
    title, desc, last = "", "", -1
    for i, ln in enumerate(lines[:8]):
        s = ln.strip()
        m = re.match(r"^title:\s*(.+)$", s)
        if m and not title:
            title, last = m.group(1).strip(), i
            continue
        m = re.match(r"^description:\s*(.+)$", s)
        if m and not desc:
            desc, last = m.group(1).strip(), i
    if last < 0:
        return "", "", content.strip("\n")
    body = "\n".join(lines[last + 1:]).strip("\n")
    if body.startswith("# "):          # drop the stale H1 — save re-adds it
        body = "\n".join(body.splitlines()[1:]).strip("\n")
    return title, desc, body


def _assemble_doc(title: str, description: str, body: str) -> str:
    return (f"# {title}\ntitle: {title}\ndescription: {description}\n\n"
            + body.strip("\n") + "\n")


# ── knowledge gaps (v2): cluster the questions that went to the team ─────

_GAP_STOP = frozenset("""a an and are as at be but by can could do does for
from has have how i if in into is it its may might must of on or our so than
that the their them then there these they this to too us was we what when
where which who why will with would you your""".split())


def _gap_tok(text: str) -> list[str]:
    return [t for t in re.findall(r"[a-z0-9']+", text.lower())
            if len(t) > 2 and t not in _GAP_STOP]


async def _gap_clusters(limit: int = 6) -> list[dict]:
    """Questions from review/escalated sessions, clustered by TF-IDF
    similarity — each cluster of >=2 is a candidate new document."""
    rows = await _q(
        "SELECT m.content FROM chat_messages m JOIN chat_sessions s "
        "ON s.id = m.session_id WHERE m.role = 'user' AND "
        "(s.needs_review OR s.escalated) ORDER BY m.id DESC LIMIT 200")
    qs = [r[0].strip()[:300] for r in rows if r[0] and r[0].strip()]
    if len(qs) < 2:
        return []
    tfs: list[dict[str, int]] = []
    for q in qs:
        tf: dict[str, int] = {}
        for t in _gap_tok(q):
            tf[t] = tf.get(t, 0) + 1
        tfs.append(tf)
    n = len(qs)
    df: dict[str, int] = {}
    for tf in tfs:
        for t in tf:
            df[t] = df.get(t, 0) + 1
    idf = {t: math.log((n + 1) / (d + 1)) + 1.0 for t, d in df.items()}
    vecs = []
    for tf in tfs:
        v = {t: (1.0 + math.log(c)) * idf.get(t, 1.0) for t, c in tf.items()}
        vecs.append((v, math.sqrt(sum(w * w for w in v.values())) or 1.0))

    def sim(i: int, j: int) -> float:
        a, an = vecs[i]
        b, bn = vecs[j]
        return sum(w * b.get(t, 0.0) for t, w in a.items()) / (an * bn)

    assigned = [False] * n
    clusters: list[dict] = []
    for i in range(n):
        if assigned[i]:
            continue
        members = [i]
        assigned[i] = True
        for j in range(i + 1, n):
            if not assigned[j] and sim(i, j) >= 0.22:
                members.append(j)
                assigned[j] = True
        if len(members) < 2:
            continue
        counts: dict[str, int] = {}
        for m in members:
            for t in tfs[m]:
                counts[t] = counts.get(t, 0) + 1
        shared = [t for t, c in counts.items()
                  if c >= (len(members) + 1) // 2]
        shared.sort(key=lambda t: -idf.get(t, 1.0))
        clusters.append({"count": len(members), "terms": shared[:5],
                         "examples": [qs[m] for m in members[:3]]})
    clusters.sort(key=lambda c: -c["count"])
    return clusters[:limit]


# ── FAQ site → KB sync (the site FAQ page is the source of truth) ────────

SITE_FAQ_URL = os.environ.get(
    "SITE_FAQ_URL", "https://datagatelodge.com/gatelodge.html")
_TAG = re.compile(r"<[^>]+>")


def _parse_faq_html(html: str) -> list[tuple[str, str]]:
    pairs = re.findall(
        r'<p class="faq-q">(.*?)</p>\s*<p class="faq-a">(.*?)</p>',
        html, re.S)
    out = []
    for q, a in pairs:
        q = _html.unescape(_TAG.sub("", q)).strip()
        a = _html.unescape(_TAG.sub("", a)).strip()
        if q and a:
            out.append((q, a))
    return out


@app.get("/kb", response_class=HTMLResponse)
async def kb_index(request: Request, flash: str = ""):
    if (r := _guard(request)):
        return r
    base = sorted(p.stem for p in _kb_dir().glob("*.md"))
    over = {n for n, in await _q("SELECT name FROM kb_overrides")}
    custom = sorted(over - set(base))

    rows = ""
    for n in base:
        state = ('<span class="tag warn">live-edited</span>' if n in over
                 else '<span class="tag ok">built-in</span>')
        rows += (f"<tr><td class='pill'>{_e(n)}</td><td>{state}</td>"
                 f"<td><a class='btn ghost' style='padding:.2rem .7rem;"
                 f"font-size:.72rem' href='/kb/edit?name={_e(n)}'>edit</a>"
                 f"</td></tr>")
    for n in custom:
        rows += (f"<tr><td class='pill'>{_e(n)}</td>"
                 f"<td><span class='tag'>custom</span></td>"
                 f"<td><a class='btn ghost' style='padding:.2rem .7rem;"
                 f"font-size:.72rem' href='/kb/edit?name={_e(n)}'>edit</a> "
                 f"<a class='btn ghost' style='padding:.2rem .7rem;"
                 f"font-size:.72rem' href='/kb/reset?name={_e(n)}'"
                 f" onclick=\"return confirm('remove {_e(n)}?')\""
                 f">remove</a></td></tr>")

    # knowledge gaps — questions that went to the team, clustered
    gaps_html = ""
    for g in await _gap_clusters():
        terms = " · ".join(g["terms"]) or "misc"
        guess = " ".join(g["terms"][:3]).strip().title() or "Visitor"
        prefill = urllib.parse.urlencode({
            "title": f"Questions about {guess}",
            "description": (f"Common visitor questions about {guess} — "
                            f"clustered from {g['count']} questions that "
                            f"went to the team"),
            "content": "\n\n".join(f"## {e}\n\n(answer pending)"
                                   for e in g["examples"]),
        })
        exs = "".join(f"<p class='note' style='margin:.15rem 0'>— "
                      f"{_e(e[:110])}</p>" for e in g["examples"])
        gaps_html += (
            f"<div class='box' style='margin-bottom:.8rem'>"
            f"<p><b>{_e(terms)}</b> &nbsp;"
            f"<span class='tag warn'>{g['count']} questions</span></p>"
            f"{exs}"
            f"<a class='btn ghost' style='padding:.2rem .7rem;font-size:"
            f".72rem;margin-top:.4rem;display:inline-block' "
            f"href='/kb/new?{prefill}'>draft document</a></div>")
    gaps_inner = gaps_html if gaps_html else (
        '<p class="note">No clusters yet — gaps appear once visitors ask '
        'questions the knowledge base did not confidently answer.</p>')
    gaps_block = """
<div class="box" style="margin-bottom:1.4rem">
<p class="note" style="letter-spacing:.14em;text-transform:uppercase;
color:var(--brass);margin-bottom:.7rem">Knowledge gaps — questions that
went to the team</p>
""" + gaps_inner + "</div>"

    actions = """
<div class="inline" style="margin-bottom:1.2rem">
<a class="btn" href="/kb/new">New document</a>
<form method="post" action="/kb/sync_faq" class="inline">
<button class="btn ghost">Sync FAQ from site</button></form>
<span class="note">edits and new documents go live on the next question</span>
</div>"""

    body = f"""
<p class="note" style="margin-bottom:1.2rem">The Gatekeeper's knowledge:
documents merged live over its built-ins. Title + description are what
its retrieval routes on — write them as the question a visitor would
ask.</p>
{actions}
{gaps_block}
<table><tr><th>doc</th><th>state</th><th></th></tr>{rows}</table>"""
    return page(request, "Knowledge base", body, flash=flash)


@app.get("/kb/new", response_class=HTMLResponse)
async def kb_new_form(request: Request, title: str = "",
                      description: str = "", content: str = ""):
    if (r := _guard(request)):
        return r
    body = f"""
<form method="post" action="/kb/new">
<div class="grid2">
<div>
<p class="note">name <span class="mono">(lowercase, e.g. channel-faq)</span></p>
<input type="text" name="name" placeholder="doc-name" required
 style="width:100%">
<p class="note" style="margin-top:.7rem">title
 <span class="mono">(what the retrieval matches on)</span></p>
<input type="text" name="title" value="{_e(title)}" required
 placeholder="Channel partners — FAQ" style="width:100%">
<p class="note" style="margin-top:.7rem">description
 <span class="mono">(when to pick this doc)</span></p>
<input type="text" name="description" value="{_e(description)}" required
 placeholder="Questions from inference partners about co-branded trials"
 style="width:100%">
</div>
<div>
<p class="note">content <span class="mono">(markdown — '##' sections
become retrieval passages)</span></p>
<textarea name="content">{_e(content)}</textarea>
</div></div>
<div class="inline" style="margin-top:.8rem">
<button class="btn">Create document (live)</button>
<a class="btn ghost" href="/kb">back</a></div></form>"""
    return page(request, "Knowledge · <em>new document</em>", body)


@app.post("/kb/sync_faq")
async def kb_sync_faq(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            r = await c.get(SITE_FAQ_URL,
                            headers={"user-agent": "lodge-office-sync"})
        items = _parse_faq_html(r.text)
    except Exception as e:
        return RedirectResponse(
            f"/kb?flash=sync+failed:+{_e(repr(e)[:60])}", status_code=303)
    if not items:
        return RedirectResponse(
            "/kb?flash=no+FAQ+items+found+on+the+site+page", status_code=303)
    ov = await _q("SELECT content FROM kb_overrides WHERE name='faq'",
                  (), fetch="one")
    if ov:
        title, desc, _ = _parse_doc(ov[0])
    else:
        p = _kb_dir() / "faq.md"
        if p.exists():
            title, desc, _ = _parse_doc(p.read_text(encoding="utf-8"))
        else:
            title, desc = "FAQ — frequently asked questions", ""
    content = "\n\n".join(f"## {q}\n\n{a}" for q, a in items)
    doc = _assemble_doc(title or "FAQ — frequently asked questions",
                        desc or "Direct answers to common questions",
                        content)
    await _x("INSERT INTO kb_overrides (name, content) VALUES ('faq', %s) "
             "ON CONFLICT (name) DO UPDATE SET content=%s, "
             "updated_at=now()", (doc, doc))
    await _audit("kb-sync-faq", f"{len(items)} items from {SITE_FAQ_URL}")
    return RedirectResponse(
        f"/kb?flash=synced+{len(items)}+FAQ+items+from+the+site",
        status_code=303)


@app.post("/kb/new")
async def kb_new(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    name = str(form.get("name", "")).strip().lower()
    title = str(form.get("title", "")).strip()
    description = str(form.get("description", "")).strip()
    content = str(form.get("content", "")).strip()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,40}", name):
        return RedirectResponse(
            "/kb?flash=name+must+be+lowercase+letters,+digits+and+dashes",
            status_code=303)
    if not (title and description and content):
        return RedirectResponse(
            "/kb?flash=title,+description+and+content+are+all+required",
            status_code=303)
    if (_kb_dir() / f"{name}.md").exists() or name in {
            n for n, in await _q("SELECT name FROM kb_overrides")}:
        return RedirectResponse(
            f"/kb?flash=a+document+named+{name}+already+exists"
            f"+—+edit+it+instead", status_code=303)
    doc = _assemble_doc(title, description, content)
    await _x("INSERT INTO kb_overrides (name, content) VALUES (%s, %s) "
             "ON CONFLICT (name) DO UPDATE SET content=%s, "
             "updated_at=now()", (name, doc, doc))
    await _audit("kb-new", f"{name} ({len(doc)} chars)")
    return RedirectResponse(
        f"/kb/edit?name={name}&flash=document+created+—+live+on+the"
        f"+next+question", status_code=303)


@app.get("/kb/edit", response_class=HTMLResponse)
async def kb_edit(request: Request, name: str = "", flash: str = ""):
    if (r := _guard(request)):
        return r
    ov = await _q("SELECT content FROM kb_overrides WHERE name=%s",
                  (name,), fetch="one")
    if ov:
        content = ov[0]
    else:
        p = _kb_dir() / f"{name}.md"
        if not p.exists():
            return RedirectResponse(
                "/kb?flash=document+not+found", status_code=303)
        content = p.read_text(encoding="utf-8")
    title, desc, body = _parse_doc(content)
    builtin = (_kb_dir() / f"{name}.md").exists()
    danger = ("<a class='btn ghost' href='/kb/reset?name="
              f"{_e(name)}'>reset to built-in</a>" if builtin else
              f"<a class='btn ghost' href='/kb/reset?name={_e(name)}'"
              f" onclick=\"return confirm('remove {name}?')\""
              f">remove document</a>")
    body_form = f"""
<form method="post" action="/kb/save">
<input type="hidden" name="name" value="{_e(name)}">
<div class="grid2">
<div>
<p class="note">title <span class="mono">(triage matches on this)</span></p>
<input type="text" name="title" value="{_e(title)}" required
 style="width:100%">
<p class="note" style="margin-top:.7rem">description
 <span class="mono">(when to pick this doc)</span></p>
<input type="text" name="description" value="{_e(desc)}" required
 style="width:100%">
</div>
<div>
<p class="note">content <span class="mono">(markdown)</span></p>
<textarea name="content">{_e(body)}</textarea>
</div></div>
<div class="inline" style="margin-top:.8rem">
<button class="btn">Save (live)</button>
{danger}
<a class="btn ghost" href="/kb">back</a></div></form>"""
    return page(request, f"Knowledge · <em>{_e(name)}</em>", body_form,
                flash=flash)


@app.post("/kb/save")
async def kb_save(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    name = str(form.get("name", ""))
    title = str(form.get("title", "")).strip()
    description = str(form.get("description", "")).strip()
    content = str(form.get("content", "")).strip()
    if not (title and description and content):
        return RedirectResponse(
            f"/kb/edit?name={_e(name)}&flash=title,+description+and"
            f"+content+are+all+required", status_code=303)
    doc = _assemble_doc(title, description, content)
    await _x("INSERT INTO kb_overrides (name, content) VALUES (%s, %s) "
             "ON CONFLICT (name) DO UPDATE SET content=%s, "
             "updated_at=now()", (name, doc, doc))
    await _audit("kb-save", f"{name} ({len(doc)} chars)")
    return RedirectResponse(
        f"/kb/edit?name={name}&flash=saved+—+live+on+the+next+question",
        status_code=303)


@app.get("/kb/reset")
async def kb_reset(request: Request, name: str = ""):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    await _x("DELETE FROM kb_overrides WHERE name=%s", (name,))
    await _audit("kb-reset", name)
    return RedirectResponse("/kb", status_code=303)


# ── mail room: contacts, compose, email viewing ──────────────────────────


async def _contact_upsert(email: str, name: str = "", company: str = "",
                          source: str = "") -> None:
    """Add/refresh a contact (auto-capture). Never overwrites a name or
    company the owner set; bumps counters."""
    email = email.strip().lower()[:200]
    if not email or "@" not in email:
        return
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(
        timespec="seconds")
    await _x(
        "INSERT INTO contacts (email, name, company, source, first_seen,"
        " last_seen, message_count) VALUES (%s, %s, %s, %s, %s, %s, 1) "
        "ON CONFLICT (email) DO UPDATE SET "
        "name = CASE WHEN EXCLUDED.name != '' THEN EXCLUDED.name "
        "ELSE contacts.name END, "
        "company = CASE WHEN EXCLUDED.company != '' THEN EXCLUDED.company "
        "ELSE contacts.company END, "
        "source = CASE WHEN contacts.source IN ('', 'manual') "
        "AND EXCLUDED.source != '' THEN EXCLUDED.source "
        "ELSE contacts.source END, "
        "last_seen = EXCLUDED.last_seen, "
        "message_count = contacts.message_count + 1",
        (email, name.strip()[:120], company.strip()[:160], source, now,
         now))


async def _send_mail(to: str, subject: str, body: str,
                     kind: str) -> tuple[bool, str]:
    """Send one email from SUPPORT_EMAIL via Resend + log with content."""
    sent, detail = False, "RESEND_API_KEY unset"
    if RESEND_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.post(
                    "https://api.resend.com/emails",
                    json={"from": SUPPORT_EMAIL, "reply_to": SUPPORT_EMAIL,
                          "to": [to], "subject": subject,
                          "text": body + "\n\n— The Gatelodge team"},
                    headers={"Authorization":
                             f"Bearer {RESEND_API_KEY}"})
                sent = r.status_code in (200, 201)
                detail = (r.json().get("id", "")
                          if sent else f"resend {r.status_code}")
        except Exception as e:
            sent, detail = False, f"send error: {e!r}"
    await _x("INSERT INTO email_log (kind, to_addr, serial, ok, detail, "
             "subject, body) VALUES (%s, %s, '', %s, %s, %s, %s)",
             (kind, to, int(sent), str(detail)[:300], subject[:300],
              body[:20000]))
    return sent, detail


# bulk send state (single-worker LAN admin — in-memory is fine)
BULK = {"running": False, "sent": 0, "total": 0, "errors": 0, "label": ""}


async def _bulk_worker(ids: list[int], subject: str, body: str) -> None:
    BULK.update(running=True, sent=0, total=len(ids), errors=0,
                label=subject[:60])
    for i, cid in enumerate(ids):
        row = await _q("SELECT email FROM contacts WHERE id=%s", (cid,),
                       fetch="one")
        if not row:
            BULK["errors"] += 1
            continue
        try:
            await _send_mail(row[0], subject, body, "bulk")
            BULK["sent"] += 1
            await _contact_upsert(row[0])      # refresh last_seen
        except Exception:
            BULK["errors"] += 1
        await asyncio.sleep(0.7)               # stay under rate limits
    BULK["running"] = False


@app.get("/contacts", response_class=HTMLResponse)
async def contacts(request: Request, flash: str = "", q: str = "",
                   tag: str = ""):
    if (r := _guard(request)):
        return r
    sql = ("SELECT id, email, name, company, tags, source, message_count,"
           " last_seen FROM contacts")
    args: list = []
    conds = []
    if q:
        conds.append("(email ILIKE %s OR name ILIKE %s OR company ILIKE %s)")
        args += [f"%{q}%"] * 3
    if tag:
        conds.append("tags ILIKE %s")
        args.append(f"%{tag}%")
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY last_seen DESC LIMIT 300"
    rows = await _q(sql, tuple(args))

    bulk_note = ""
    if BULK["running"] or BULK["total"]:
        bulk_note = (f'<div class="bulk-status">Bulk send: '
                     f'<b>{_e(BULK["label"])}</b> — {BULK["sent"]}/'
                     f'{BULK["total"]} sent'
                     + (f', {BULK["errors"]} errors' if BULK["errors"] else '')
                     + (' (running…)' if BULK["running"] else ' — done')
                     + '</div>')

    trs = "".join(
        f"<tr><td><input type=checkbox name=ids value={i}></td>"
        f"<td class='pill'>{_e(e)}</td><td>{_e(n or '—')}</td>"
        f"<td>{_e(co or '—')}</td><td class='note'>{_e(t)}</td>"
        f"<td><span class='tag'>{_e(s)}</span></td><td>{m}</td>"
        f"<td class='note'>{_e(ls[:16] if ls else '')}</td>"
        f"<td><a class='btn ghost' style='padding:.15rem .5rem;"
        f"font-size:.7rem' href='/contacts/edit?id={i}'>edit</a> "
        f"<a class='btn ghost' style='padding:.15rem .5rem;font-size:.7rem'"
        f" href='/compose?to={_e(e)}'>mail</a> "
        f"<a class='btn ghost' style='padding:.15rem .5rem;font-size:.7rem'"
        f" href='/contacts/delete?id={i}'"
        f" onclick=\"return confirm('delete {_e(e)}?')\">del</a></td></tr>"
        for i, e, n, co, t, s, m, ls in rows)

    body = f"""
{bulk_note}
<div class="inline" style="margin-bottom:1rem">
<form method="get" action="/contacts" class="inline">
<input type="text" name="q" value="{_e(q)}" placeholder="search email, name, company…"
 style="min-width:260px">
<button class="btn ghost">Search</button></form>
<a class="btn" href="/contacts/edit">New contact</a>
<a class="btn ghost" href="/compose">Compose</a>
</div>
<form method="post" action="/contacts/bulk" id="bulkform">
<div class="inline" style="margin-bottom:.8rem">
<button class="btn" type="button"
 onclick="var f=document.getElementById('bulkform');
  f.querySelectorAll('input[name=ids]').forEach(c=>c.checked=true)">
 select all</button>
<button class="btn ghost" type="submit"
 onclick="return confirm('Send this email to every selected contact?')">
 Email selected →</button>
<input type="hidden" name="subject" id="bulk-subject">
<input type="hidden" name="body" id="bulk-body">
<span class="note">compose below, then select recipients and send</span>
</div>
<div class="box" style="margin-bottom:1rem">
<p class="note">bulk message (sent individually to each selected contact)</p>
<input type="text" id="bs" placeholder="Subject" style="width:100%">
<textarea id="bb" style="min-height:110px" placeholder="Message…"
 oninput="document.getElementById('bulk-subject').value=
 document.getElementById('bs').value;
 document.getElementById('bulk-body').value=this.value"></textarea>
</div>
<table><tr><th></th><th>email</th><th>name</th><th>company</th><th>tags</th>
<th>source</th><th>msgs</th><th>last seen</th><th></th></tr>
{trs or '<tr><td colspan=9 class=note>no contacts yet — the book fills '
'automatically from inbound mail, trial downloads and purchases</td></tr>'}
</table></form>"""
    return page(request, "Contacts", body, flash=flash)


@app.get("/contacts/edit", response_class=HTMLResponse)
async def contacts_edit(request: Request, id: int = 0):
    if (r := _guard(request)):
        return r
    row = None
    if id:
        row = await _q("SELECT email, name, company, notes, tags "
                       "FROM contacts WHERE id=%s", (id,), fetch="one")
    e, n, co, no, t = row if row else ("", "", "", "", "")
    body = f"""
<form method="post" action="/contacts/save">
<input type="hidden" name="id" value="{id}">
{'<p class="note">email cannot be changed</p><input type=hidden name=email value="' + _e(e) + '"><p class=pill>' + _e(e) + '</p>' if row else '<p class="note">email</p><input type="email" name="email" value="' + _e(e) + '" required style="width:100%">'}
<div class="grid2" style="margin-top:.8rem">
<div><p class="note">name</p><input type="text" name="name" value="{_e(n)}" style="width:100%">
<p class="note" style="margin-top:.6rem">company</p><input type="text" name="company" value="{_e(co)}" style="width:100%"></div>
<div><p class="note">tags <span class="mono">(comma-separated)</span></p><input type="text" name="tags" value="{_e(t)}" style="width:100%">
<p class="note" style="margin-top:.6rem">notes</p><textarea name="notes" style="min-height:90px">{_e(no)}</textarea></div></div>
<div class="inline" style="margin-top:.8rem">
<button class="btn">Save</button>
<a class="btn ghost" href="/contacts">back</a></div></form>"""
    return page(request, "Contacts · " + ("edit" if row else "new"), body)


@app.post("/contacts/save")
async def contacts_save(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    cid = int(form.get("id", 0) or 0)
    email = str(form.get("email", "")).strip().lower()
    if cid:
        await _x("UPDATE contacts SET name=%s, company=%s, notes=%s, "
                 "tags=%s WHERE id=%s",
                 (str(form.get("name", ""))[:120],
                  str(form.get("company", ""))[:160],
                  str(form.get("notes", ""))[:2000],
                  str(form.get("tags", ""))[:200], cid))
        await _audit("contact-save", f"#{cid}")
        return RedirectResponse("/contacts?flash=saved", status_code=303)
    if not email or "@" not in email:
        return RedirectResponse("/contacts?flash=valid+email+required",
                                status_code=303)
    await _contact_upsert(email, str(form.get("name", "")),
                          str(form.get("company", "")), "manual")
    await _audit("contact-new", email)
    return RedirectResponse("/contacts?flash=added", status_code=303)


@app.get("/contacts/delete")
async def contacts_delete(request: Request, id: int = 0):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    await _x("DELETE FROM contacts WHERE id=%s", (id,))
    await _audit("contact-delete", f"#{id}")
    return RedirectResponse("/contacts?flash=deleted", status_code=303)


@app.post("/contacts/bulk")
async def contacts_bulk(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    ids = [int(i) for i in (form.getlist("ids") or [])
           if str(i).isdigit()][:200]
    subject = str(form.get("subject", "")).strip()[:300]
    body = str(form.get("body", "")).strip()
    if not ids or not subject or not body:
        return RedirectResponse(
            "/contacts?flash=select+recipients+and+write+the+message",
            status_code=303)
    if BULK["running"]:
        return RedirectResponse(
            "/contacts?flash=a+bulk+send+is+already+running", status_code=303)
    asyncio.create_task(_bulk_worker(ids, subject, body))
    await _audit("bulk-send", f"{len(ids)} recipients: {subject[:60]}")
    return RedirectResponse(
        f"/contacts?flash=bulk+send+started+({len(ids)}+recipients)",
        status_code=303)


@app.get("/compose", response_class=HTMLResponse)
async def compose(request: Request, to: str = "", ref: str = "",
                  flash: str = ""):
    if (r := _guard(request)):
        return r
    pref = f"Re: {ref}" if ref else ""
    body = f"""
<form method="post" action="/compose/send">
<div class="grid2">
<div>
<p class="note">to</p>
<input type="email" name="to" value="{_e(to)}" required style="width:100%">
<p class="note" style="margin-top:.6rem">subject</p>
<input type="text" name="subject" value="{_e(pref)}" required style="width:100%">
<p class="note" style="margin-top:.6rem">from</p>
<p class="pill">{_e(SUPPORT_EMAIL)}</p>
</div>
<div>
<p class="note">message</p>
<textarea name="body" style="min-height:260px"></textarea>
</div></div>
<div class="inline" style="margin-top:.8rem">
<button class="btn">Send</button>
<a class="btn ghost" href="/contacts">address book</a>
<a class="btn ghost" href="/inbox">inbox</a></div></form>"""
    return page(request, "Compose", body, flash=flash)


@app.post("/compose/send")
async def compose_send(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    to = str(form.get("to", "")).strip().lower()
    subject = str(form.get("subject", "")).strip()[:300]
    body = str(form.get("body", "")).strip()
    if not to or "@" not in to or not subject or not body:
        return RedirectResponse(
            "/compose?flash=to,+subject+and+message+required",
            status_code=303)
    sent, _ = await _send_mail(to, subject, body, "compose")
    await _contact_upsert(to)
    await _audit("compose", f"-> {to}: {subject[:60]}")
    return RedirectResponse(
        f"/compose?flash={'sent' if sent else 'send+failed+(logged)'}",
        status_code=303)


# ── inbox (inbound support mail handled by the agent) ─────────────────────


@app.get("/inbox", response_class=HTMLResponse)
async def inbox(request: Request, flash: str = ""):
    if (r := _guard(request)):
        return r
    pending = await _q(
        "SELECT id, from_addr, subject, body, topic, doc, created_at "
        "FROM inbound_emails WHERE status='review' "
        "ORDER BY created_at DESC LIMIT 50")
    recent = await _q(
        "SELECT id, from_addr, subject, status, doc, created_at "
        "FROM inbound_emails WHERE status != 'review' "
        "ORDER BY created_at DESC LIMIT 30")

    cards = ""
    for i, (mid, frm, subject, body, topic, doc, at) in enumerate(pending):
        cards += f"""
<div class="box">
<p><b>{_e(subject or '(no subject)')}</b> &nbsp;
<span class="tag warn">needs review</span>
<span class="mono note">{_e(frm)} · {str(at)[:16]} · {_e(topic or '-')}</span></p>
<p style="margin:.6rem 0; white-space:pre-wrap; max-width:760px">{_e(body[:500])}</p>
<form method="post" action="/inbox/reply" class="inline">
<input type="hidden" name="id" value="{_e(mid)}">
<textarea name="reply" style="min-height:110px; flex:1"
 placeholder="Type the reply this customer should receive…"></textarea>
<div style="display:flex; flex-direction:column; gap:.5rem">
<button class="btn">Send reply</button>
<a class="btn ghost" href="/inbox/ignore?id={_e(mid)}">ignore</a></div>
</form></div>"""
    if not pending:
        cards = ('<div class="box"><p class="note">Inbox clear — no '
                 'inbound mail waiting for review.</p></div>')

    rows = "".join(
        f"<tr><td class='pill'>{_e(frm)}</td><td>{_e(subject[:60])}</td>"
        f"<td>{'<span class=tag ok>' + _e(st) + '</span>' if st == 'answered' else '<span class=tag>' + _e(st) + '</span>'}</td>"
        f"<td class='pill'>{_e(doc or '-')}</td>"
        f"<td class='note'>{str(at)[:16]}</td>"
        f"<td><a class='btn ghost' style='padding:.15rem .5rem;"
        f"font-size:.7rem' href='/inbox/view?id={_e(mid)}'>view</a></td></tr>"
        for mid, frm, subject, st, doc, at in recent)

    body = f"""
<p class="note" style="margin-bottom:1.2rem">Inbound support mail. The
agent auto-replies to confident questions; everything else lands here.</p>
{cards}
<h1 style="font-size:1.2rem">Recently handled</h1>
<table><tr><th>from</th><th>subject</th><th>status</th><th>doc</th>
<th>at</th></tr>{rows or '<tr><td colspan=5 class=note>none yet</td></tr>'}</table>"""
    return page(request, "Inbox", body, flash=flash)


@app.post("/inbox/reply")
async def inbox_reply(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    mid = str(form.get("id", ""))
    reply = str(form.get("reply", "")).strip()
    if not reply:
        return RedirectResponse("/inbox", status_code=303)
    row = await _q("SELECT from_addr, subject FROM inbound_emails "
                   "WHERE id=%s", (mid,), fetch="one")
    if not row:
        return RedirectResponse("/inbox?flash=email+not+found",
                                status_code=303)
    to, subject = row
    sent = False
    if RESEND_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.post(
                    "https://api.resend.com/emails",
                    json={"from": SUPPORT_EMAIL, "reply_to": to, "to": [to],
                          "subject": f"Re: {subject}" if subject
                          else "Your Gatelodge question — answered",
                          "text": reply + "\n\n— The Gatelodge team"},
                    headers={"Authorization": f"Bearer {RESEND_API_KEY}"})
                sent = r.status_code in (200, 201)
        except Exception:
            sent = False
    await _x("INSERT INTO email_log (kind, to_addr, serial, ok, detail, "
             "subject, body) VALUES ('inbox-reply', %s, %s, %s, %s, %s, %s)",
             (to, mid[:12], int(sent), "ok" if sent else "failed",
              f"Re: {subject}" if subject else
              "Your Gatelodge question — answered",
              reply + "\n\n— The Gatelodge team"))
    await _x("UPDATE inbound_emails SET status='answered', reply=%s, "
             "handled_at=now() WHERE id=%s", (reply, mid))
    await _contact_upsert(to)
    await _audit("inbox-reply", f"{mid[:16]} -> {to}")
    return RedirectResponse(
        f"/inbox?flash=reply+{'sent' if sent else 'saved+(send+failed)'}",
        status_code=303)


@app.get("/inbox/ignore")
async def inbox_ignore(request: Request, id: str = ""):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    await _x("UPDATE inbound_emails SET status='ignored', "
             "handled_at=now() WHERE id=%s", (id,))
    await _audit("inbox-ignore", id[:16])
    return RedirectResponse("/inbox", status_code=303)


@app.get("/inbox/view", response_class=HTMLResponse)
async def inbox_view(request: Request, id: str = ""):
    if (r := _guard(request)):
        return r
    row = await _q(
        "SELECT id, from_addr, subject, body, status, confident, doc, "
        "topic, reply, created_at, handled_at FROM inbound_emails "
        "WHERE id=%s", (id,), fetch="one")
    if not row:
        return RedirectResponse("/inbox?flash=email+not+found",
                                status_code=303)
    (mid, frm, subject, body, st, conf, doc, topic, reply, at,
     handled) = row
    reply_html = (f"<h1 style=font-size:1.1rem>Reply sent</h1>"
                  f'<div class="view-body">{_e(reply)}</div>'
                  if reply else
                  '<p class="note">No reply sent yet — use the reply form '
                  'on the Inbox screen or compose a follow-up.</p>')
    body = f"""
<div class="inline" style="margin-bottom:1rem">
<span class="tag">{_e(st)}</span>
<span class="mono note">{_e(frm)} · {str(at)[:19]} · doc: {_e(doc or '-')} · topic: {_e(topic or '-')}</span></div>
<h1 style="font-size:1.2rem">{_e(subject or '(no subject)')}</h1>
<div class="view-body">{_e(body)}</div>
{reply_html}
<div class="inline" style="margin-top:1rem">
<a class="btn" href="/compose?to={_e(frm)}&ref={_e(subject)}">Compose follow-up</a>
<a class="btn ghost" href="/inbox">back to inbox</a></div>"""
    return page(request, "Inbox · message", body)


# ── analytics ─────────────────────────────────────────────────────────────


def _bar(label: str, value: int, maxv: int, dim: bool = False) -> str:
    pct = (100 * value / maxv) if maxv else 0
    return (f'<div class="bar-row"><span class="bar-label" '
            f'title="{_e(label)}">{_e(label)}</span>'
            f'<div class="bar-track"><div class="bar-fill'
            f'{" dim" if dim else ""}" style="width:{pct:.0f}%"></div>'
            f'</div><span class="bar-num">{value}</span></div>')


@app.get("/analytics", response_class=HTMLResponse)
async def analytics(request: Request):
    if (r := _guard(request)):
        return r

    # headline numbers
    tot = await _q("SELECT COUNT(*) FROM chat_sessions", fetch="one")
    conf = await _q(
        "SELECT COUNT(*) FILTER (WHERE confident)::float / "
        "NULLIF(COUNT(*), 0) FROM chat_messages WHERE role='assistant'",
        fetch="one")
    esc = await _q(
        "SELECT COUNT(*) FROM chat_sessions "
        "WHERE needs_review OR escalated", fetch="one")
    inb = await _q(
        "SELECT COUNT(*) FILTER (WHERE status='answered'), COUNT(*) "
        "FILTER (WHERE status='review'), COUNT(*) "
        "FROM inbound_emails", fetch="one") or (0, 0, 0)

    # daily volume (30d): sessions vs escalated
    days = await _q(
        "SELECT to_char(date_trunc('day', created_at), 'MM-DD') d, "
        "COUNT(*) total, COUNT(*) FILTER "
        "(WHERE needs_review OR escalated) esc "
        "FROM chat_sessions WHERE created_at > now() - interval '30 days' "
        "GROUP BY date_trunc('day', created_at) ORDER BY 1")
    maxd = max([r[1] for r in days], default=0)
    vol = "".join(_bar(r[0], r[1], maxd) + _bar("", r[2], maxd, dim=True)
                  for r in days) or '<p class="note">no sessions yet</p>'

    # doc hit rates
    docs = await _q(
        "SELECT doc, COUNT(*) FROM chat_messages "
        "WHERE role='assistant' AND doc IS NOT NULL "
        "GROUP BY doc ORDER BY 2 DESC LIMIT 12")
    maxdoc = max([r[1] for r in docs], default=0)
    dochits = "".join(_bar(r[0], r[1], maxdoc) for r in docs) \
        or '<p class="note">no answered questions yet</p>'

    # escalation topics
    topics = await _q(
        "SELECT COALESCE(NULLIF(last_topic, ''), '(unclassified)') t, "
        "COUNT(*) FROM chat_sessions WHERE needs_review OR escalated "
        "GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
    maxt = max([r[1] for r in topics], default=0)
    tophits = "".join(_bar(r[0], r[1], maxt, dim=True) for r in topics) \
        or '<p class="note">none yet</p>'

    # top unanswered questions (frequency across review/escalated sessions)
    unans = await _q(
        "SELECT m.content, COUNT(*) FROM chat_messages m "
        "JOIN chat_sessions s ON s.id = m.session_id "
        "WHERE m.role='user' AND (s.needs_review OR s.escalated) "
        "GROUP BY m.content ORDER BY 2 DESC LIMIT 8")
    maxu = max([r[1] for r in unans], default=0)
    unbars = "".join(_bar(r[0][:70], r[1], maxu, dim=True)
                     for r in unans) or '<p class="note">none yet</p>'

    rate = f"{(conf[0] or 0) * 100:.0f}%" if conf and conf[0] is not None \
        else "—"
    body = f"""
<div class="bigstats">
<div class="card"><div class="num">{tot[0] if tot else 0}</div>
<div class="lbl">chat sessions</div></div>
<div class="card"><div class="num">{rate}</div>
<div class="lbl">answer confidence</div></div>
<div class="card"><div class="num">{esc[0] if esc else 0}</div>
<div class="lbl">escalated / reviewed</div></div>
<div class="card"><div class="num">{inb[0]}</div>
<div class="lbl">inbound auto-replied</div></div>
<div class="card"><div class="num">{inb[1]}</div>
<div class="lbl">inbox pending</div></div>
</div>

<h1 style="font-size:1.2rem">Sessions per day — 30 days
<span class="note">(brass = sessions, grey = escalated)</span></h1>
<div class="bars">{vol}</div>

<h1 style="font-size:1.2rem">Knowledge-base hit rates</h1>
<div class="bars">{dochits}</div>

<h1 style="font-size:1.2rem">Escalation topics</h1>
<div class="bars">{tophits}</div>

<h1 style="font-size:1.2rem">Top unanswered questions</h1>
<div class="bars">{unbars}</div>"""
    return page(request, "Analytics", body)


@app.get("/emails", response_class=HTMLResponse)
async def emails(request: Request):
    if (r := _guard(request)):
        return r
    rows = await _q(
        "SELECT id, kind, to_addr, serial, ok, detail, subject, created_at "
        "FROM email_log ORDER BY id DESC LIMIT 150")
    trs = "".join(
        f"<tr><td><span class=tag>{_e(k)}</span></td><td>{_e(t)}</td>"
        f"<td>{_e(s or '(no subject)')}</td>"
        f"<td>{'<span class=tag ok>ok</span>' if o else '<span class=tag bad>fail</span>'}</td>"
        f"<td class=note>{_e(c)}</td>"
        f"<td><a class='btn ghost' style='padding:.15rem .5rem;"
        f"font-size:.7rem' href='/email/view?id={i}'>view</a></td></tr>"
        for i, k, t, s, o, d, c in
        [(r[0], r[1], r[2], r[6], r[4], r[5], r[7]) for r in rows])
    body = f"""
<table><tr><th>kind</th><th>to</th><th>subject</th><th>state</th>
<th>when</th><th></th></tr>
{trs or '<tr><td colspan=6 class=note>no emails yet</td></tr>'}
</table>"""
    return page(request, "Email log", body)


@app.get("/email/view", response_class=HTMLResponse)
async def email_view(request: Request, id: int = 0):
    if (r := _guard(request)):
        return r
    row = await _q(
        "SELECT kind, to_addr, serial, ok, detail, subject, body, "
        "created_at FROM email_log WHERE id=%s", (id,), fetch="one")
    if not row:
        return RedirectResponse("/emails?flash=email+not+found",
                                status_code=303)
    kind, to, serial, ok, detail, subject, body, at = row
    content = body or "(no content stored — sent before the mail-room update)"
    body = f"""
<div class="inline" style="margin-bottom:1rem">
<span class="tag">{_e(kind)}</span>
<span class="mono note">to: {_e(to)} · {str(at)[:19]}</span>
{'<span class=tag ok>delivered</span>' if ok else '<span class=tag bad>send failed</span>'}
</div>
<h1 style="font-size:1.2rem">{_e(subject or '(no subject)')}</h1>
<div class="view-body">{_e(content)}</div>
<div class="inline" style="margin-top:1rem">
<a class="btn" href="/compose?to={_e(to)}&ref={_e(subject)}">Compose follow-up</a>
<a class="btn ghost" href="/emails">back to log</a></div>"""
    return page(request, "Email · sent", body)


# ── tasks (the agent's jobs) ───────────────────────────────────────────────


@app.get("/tasks", response_class=HTMLResponse)
async def tasks(request: Request, flash: str = ""):
    if (r := _guard(request)):
        return r
    rows = await _q(
        "SELECT t.id, t.name, t.schedule, t.enabled, t.run_now, "
        "t.last_run_at, t.next_run_at, "
        "(SELECT classification FROM task_runs r WHERE r.task_id=t.id "
        "ORDER BY r.id DESC LIMIT 1) AS last_class "
        "FROM tasks t ORDER BY t.id")
    tagmap = {"achieved": "ok", "partial": "warn", "failed": "bad",
              "review": "", "running": "warn"}
    trs = "".join(
        f"<tr><td>{_e(n)}</td><td><span class='tag'>{_e(s)}</span></td>"
        f"<td>{'<span class=tag ok>on</span>' if en else '<span class=tag>off</span>'}</td>"
        f"<td>{'<span class=tag warn>queued</span>' if rn else ''}</td>"
        f"<td>{'<span class=tag ' + tagmap.get(lc or '', '') + '>' + _e(lc or '—') + '</span>' if lc else '—'}</td>"
        f"<td class='note'>{str(la)[:16] if la else '—'}</td>"
        f"<td class='note'>{str(nr)[:16] if nr else '—'}</td>"
        f"<td><a class='btn ghost' style='padding:.15rem .5rem;font-size:.7rem'"
        f" href='/tasks/run?id={i}'>runs</a> "
        f"<a class='btn ghost' style='padding:.15rem .5rem;font-size:.7rem'"
        f" href='/tasks/edit?id={i}'>edit</a> "
        f"<a class='btn' style='padding:.15rem .5rem;font-size:.7rem'"
        f" href='/tasks/now?id={i}'>run now</a></td></tr>"
        for i, n, s, en, rn, la, nr, lc in rows)

    proposals = await _q(
        "SELECT id, capability, context, status, created_at "
        "FROM tool_proposals ORDER BY status='open' DESC, id DESC LIMIT 20")
    props = "".join(
        f"<tr><td class='pill'>{_e(cap)}</td>"
        f"<td class='note' style='max-width:360px'>{_e(ctx[:140])}</td>"
        f"<td>{'<span class=tag warn>' + _e(st) + '</span>' if st == 'open' else '<span class=tag>' + _e(st) + '</span>'}</td>"
        f"<td class='note'>{str(at)[:16]}</td>"
        + (f"<td><a class='btn ghost' style='padding:.15rem .5rem;"
           f"font-size:.7rem' href='/tools/approve?id={pid}'>approve</a> "
           f"<a class='btn ghost' style='padding:.15rem .5rem;"
           f"font-size:.7rem' href='/tools/dismiss?id={pid}'>dismiss</a>"
           f"</td>" if st == "open" else "<td></td>")
        + "</tr>"
        for pid, cap, ctx, st, at in proposals)

    body = f"""
<p class="note" style="margin-bottom:1.2rem">Tasks the Gatekeeper runs on
its own: set a goal and success criteria, pick a schedule — the agent
plans, uses tools, and a separate review pass grades the outcome.</p>
<div class="inline" style="margin-bottom:1rem">
<a class="btn" href="/tasks/edit">New task</a>
<a class="btn ghost" href="/tools">Tool registry</a></div>
<table><tr><th>task</th><th>schedule</th><th>on</th><th>queued</th>
<th>last outcome</th><th>last run</th><th>next run</th><th></th></tr>
{trs or '<tr><td colspan=8 class=note>no tasks yet</td></tr>'}</table>
<h1 style="font-size:1.2rem">Tool proposals
<span class="note">— capabilities the agent wanted but doesn't have</span></h1>
<table><tr><th>capability</th><th>context</th><th>status</th><th>at</th>
<th></th></tr>
{props or '<tr><td colspan=5 class=note>none — the agent has not asked '
'for new tools yet</td></tr>'}</table>"""
    return page(request, "Tasks", body, flash=flash)


@app.get("/tasks/edit", response_class=HTMLResponse)
async def tasks_edit(request: Request, id: int = 0):
    if (r := _guard(request)):
        return r
    row = None
    if id:
        row = await _q("SELECT name, goal, success_criteria, inputs, "
                       "schedule, notify, enabled FROM tasks WHERE id=%s",
                       (id,), fetch="one")
    n, g, sc, inp, sch, nt, en = row if row else (
        "", "", "", "", "manual", True, True)
    opts = "".join(
        f'<option value="{v}"{" selected" if v == sch else ""}>{v}</option>'
        for v in ("manual", "hourly", "daily", "weekly"))
    body = f"""
<form method="post" action="/tasks/save">
<input type="hidden" name="id" value="{id}">
<div class="grid2">
<div>
<p class="note">name</p>
<input type="text" name="name" value="{_e(n)}" required style="width:100%">
<p class="note" style="margin-top:.6rem">schedule</p>
<select name="schedule" style="width:100%">{opts}</select>
<p class="note" style="margin-top:.6rem">
<label><input type="checkbox" name="notify" value="1"
{'checked' if nt else ''}> email me the report after each run</label></p>
<p class="note" style="margin-top:.4rem">
<label><input type="checkbox" name="enabled" value="1"
{'checked' if en else ''}> enabled</label></p>
</div>
<div>
<p class="note">goal / objective / outcome</p>
<textarea name="goal" style="min-height:90px" required>{_e(g)}</textarea>
<p class="note" style="margin-top:.6rem">success criteria
<span class="mono">(how the review judges it)</span></p>
<textarea name="success_criteria"
style="min-height:70px">{_e(sc)}</textarea>
<p class="note" style="margin-top:.6rem">inputs / context / sources</p>
<textarea name="inputs" style="min-height:70px">{_e(inp)}</textarea>
</div></div>
<div class="inline" style="margin-top:.8rem">
<button class="btn">Save</button>
<a class="btn ghost" href="/tasks">back</a>
{f'<a class="btn ghost" href="/tasks/delete?id={id}" onclick="return confirm(\'delete task?\')">delete</a>' if id else ''}
</div></form>"""
    return page(request, "Tasks · " + ("edit" if row else "new"), body)


@app.post("/tasks/save")
async def tasks_save(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    tid = int(form.get("id", 0) or 0)
    vals = (str(form.get("name", "")).strip()[:120],
            str(form.get("goal", "")).strip()[:4000],
            str(form.get("success_criteria", "")).strip()[:2000],
            str(form.get("inputs", "")).strip()[:2000],
            str(form.get("schedule", "manual")),
            form.get("notify") == "1", form.get("enabled") == "1")
    if tid:
        await _x("UPDATE tasks SET name=%s, goal=%s, success_criteria=%s, "
                 "inputs=%s, schedule=%s, notify=%s, enabled=%s WHERE id=%s",
                 vals + (tid,))
        await _audit("task-save", f"#{tid} {vals[0]}")
    else:
        await _x("INSERT INTO tasks (name, goal, success_criteria, inputs,"
                 " schedule, notify, enabled) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                 vals)
        await _audit("task-new", vals[0])
    return RedirectResponse("/tasks?flash=saved", status_code=303)


@app.get("/tasks/delete")
async def tasks_delete(request: Request, id: int = 0):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    await _x("DELETE FROM tasks WHERE id=%s", (id,))
    await _audit("task-delete", f"#{id}")
    return RedirectResponse("/tasks?flash=deleted", status_code=303)


@app.get("/tasks/now")
async def tasks_now(request: Request, id: int = 0):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    await _x("UPDATE tasks SET run_now=TRUE WHERE id=%s", (id,))
    await _audit("task-run-now", f"#{id}")
    return RedirectResponse(
        "/tasks?flash=queued+—+the+agent+picks+it+up+within+30+seconds",
        status_code=303)


@app.get("/tasks/run", response_class=HTMLResponse)
async def tasks_runs(request: Request, id: int = 0):
    if (r := _guard(request)):
        return r
    t = await _q("SELECT name FROM tasks WHERE id=%s", (id,), fetch="one")
    rows = await _q(
        "SELECT id, status, classification, summary, started_at, "
        "finished_at FROM task_runs WHERE task_id=%s ORDER BY id DESC "
        "LIMIT 50", (id,))
    tagmap = {"achieved": "ok", "partial": "warn", "failed": "bad",
              "running": "warn"}
    trs = "".join(
        f"<tr><td class='pill'>#{i}</td>"
        f"<td>{'<span class=tag ' + tagmap.get(cl or '', '') + '>' + _e(cl or st) + '</span>'}</td>"
        f"<td style='max-width:420px'>{_e(su[:180])}</td>"
        f"<td class='note'>{str(sa)[:16]}</td>"
        f"<td><a class='btn ghost' style='padding:.15rem .5rem;"
        f"font-size:.7rem' href='/tasks/runview?id={i}'>view</a></td></tr>"
        for i, st, cl, su, sa, fi in rows)
    body = f"""
<p class="note" style="margin-bottom:1rem">Task: <b>{_e(t[0] if t else '?')}</b></p>
<table><tr><th>run</th><th>outcome</th><th>summary</th><th>started</th>
<th></th></tr>
{trs or '<tr><td colspan=5 class=note>no runs yet</td></tr>'}</table>"""
    return page(request, "Task runs", body)


@app.get("/tasks/runview", response_class=HTMLResponse)
async def tasks_runview(request: Request, id: int = 0):
    if (r := _guard(request)):
        return r
    run = await _q(
        "SELECT r.task_id, t.name, r.status, r.classification, r.summary, "
        "r.report, r.started_at, r.finished_at FROM task_runs r "
        "JOIN tasks t ON t.id=r.task_id WHERE r.id=%s", (id,), fetch="one")
    if not run:
        return RedirectResponse("/tasks?flash=run+not+found",
                                status_code=303)
    tid, tname, st, cl, su, report, sa, fi = run
    steps = await _q(
        "SELECT seq, tool, description, input, output, status "
        "FROM task_steps WHERE run_id=%s ORDER BY seq", (id,))
    step_html = ""
    for seq, tool, de, inp, out, stat in steps:
        # drafts → one-click compose
        if tool == "email_draft" and out.startswith("DRAFT"):
            lines = out.split("\n")
            to = lines[1].replace("To: ", "") if len(lines) > 1 else ""
            subj = (lines[2].replace("Subject: ", "")
                    if len(lines) > 2 else "")
            out += (f'\n<p><a class="btn" style="padding:.15rem .5rem;'
                    f'font-size:.7rem" href="/compose?to={_e(to)}'
                    f'&ref={_e(subj)}">Send this draft →</a></p>')
        stat_tag = ("<span class=tag ok>ok</span>" if stat == "ok" else
                    f"<span class=tag bad>{_e(stat)}</span>")
        step_html += f"""
<div class="box">
<p><b>Step {seq + 1}</b> — <span class="pill">{_e(tool)}</span> {stat_tag}
<span class="note">{_e(de)}</span></p>
<p class="note" style="white-space:pre-wrap">{_e(inp[:400])}</p>
<div class="view-body" style="font-size:.8rem">{_e(out[:2500])}</div>
</div>"""
    body = f"""
<div class="inline" style="margin-bottom:1rem">
<span class="tag">{_e(cl or st)}</span>
<span class="mono note">{_e(tname)} · run #{id} · {str(sa)[:16]}</span></div>
<h1 style="font-size:1.2rem">Review</h1>
<div class="view-body">{_e(su or '(no summary)')}</div>
<h1 style="font-size:1.2rem;margin-top:1.2rem">Report</h1>
<div class="view-body">{_e(report or '(no report)')}</div>
<h1 style="font-size:1.2rem;margin-top:1.2rem">Steps</h1>
{step_html or '<p class="note">no steps recorded</p>'}
<div class="inline" style="margin-top:1rem">
<a class="btn ghost" href="/tasks/run?id={tid}">back to runs</a></div>"""
    return page(request, "Task run", body)


# ── tools (the agent's hands) ──────────────────────────────────────────────


@app.get("/tools", response_class=HTMLResponse)
async def tools(request: Request, flash: str = ""):
    if (r := _guard(request)):
        return r
    rows = await _q("SELECT name, description, kind, enabled FROM tools "
                    "ORDER BY kind, name")
    trs = "".join(
        f"<tr><td class='pill'>{_e(n)}</td>"
        f"<td style='max-width:460px'>{_e(d)}</td>"
        f"<td><span class='tag'>{_e(k)}</span></td>"
        f"<td>{'<span class=tag ok>on</span>' if en else '<span class=tag>off</span>'}</td></tr>"
        for n, d, k, en in rows)
    body = f"""
<p class="note" style="margin-bottom:1.2rem">The capabilities the task
agent can call. Builtin tools ship with the Gatekeeper; MCP tools can be
registered through the gateway. When the agent needs something missing,
it files a proposal on the Tasks screen.</p>
<table><tr><th>tool</th><th>description</th><th>kind</th><th>state</th>
</tr>
{trs or '<tr><td colspan=4 class=note>registry empty — the gatekeeper '
'syncs its builtin tools at startup</td></tr>'}</table>"""
    return page(request, "Tools", body, flash=flash)


@app.get("/tools/approve")
async def tools_approve(request: Request, id: int = 0):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    await _x("UPDATE tool_proposals SET status='approved' WHERE id=%s",
             (id,))
    await _audit("tool-approve", f"#{id}")
    return RedirectResponse("/tasks?flash=approved+—+backlog+for+the"
                            "+next+build", status_code=303)


@app.get("/tools/dismiss")
async def tools_dismiss(request: Request, id: int = 0):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    await _x("UPDATE tool_proposals SET status='dismissed' WHERE id=%s",
             (id,))
    await _audit("tool-dismiss", f"#{id}")
    return RedirectResponse("/tasks", status_code=303)


@app.get("/gateway", response_class=HTMLResponse)
async def gateway(request: Request):
    if (r := _guard(request)):
        return r
    hz, met = {}, "(gateway not configured)"
    if GATEWAY_URL and GATEWAY_KEY:
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(f"{GATEWAY_URL}/healthz",
                                headers={"Authorization":
                                         f"Bearer {GATEWAY_KEY}"})
                hz = r.json()
        except Exception as e:
            hz = {"error": repr(e)[:120]}
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(f"{GATEWAY_URL}/metrics",
                                headers={"Authorization":
                                         f"Bearer {GATEWAY_KEY}"})
                lines = [l for l in r.text.splitlines()
                         if any(k in l.lower() for k in
                                ("cost", "budget", "token", "request"))
                         and not l.startswith("#")][:40]
                met = "\n".join(lines)
        except Exception:
            met = "(metrics unavailable)"
    lic = hz.get("license", {})
    body = f"""
<div class="cards">
<div class="card"><div class="num">{_e(hz.get("version", "?"))}</div><div class="lbl">gateway version</div></div>
<div class="card"><div class="num">{_e(lic.get("mode", "?"))}</div><div class="lbl">license mode</div></div>
<div class="card"><div class="num">$25</div><div class="lbl">monthly budget cap</div></div>
<div class="card"><div class="num">{_e(str(hz.get("status", "?")))}</div><div class="lbl">status</div></div>
</div>
<div class="box"><p class="mono note" style="margin-bottom:.6rem">HEALTHZ (LIVE FROM THE GATEWAY — THE PRODUCT WATCHING ITSELF)</p>
<pre class="mono" style="font-size:.78rem;color:var(--muted);white-space:pre-wrap">{_e(json.dumps(hz, indent=1))}</pre></div>
<div class="box"><p class="mono note" style="margin-bottom:.6rem">COST / USAGE METRICS (FILTERED)</p>
<pre class="mono" style="font-size:.78rem;color:var(--muted);white-space:pre-wrap">{_e(met)}</pre></div>"""
    return page(request, "Gateway", body)


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "gatelodge-admin"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

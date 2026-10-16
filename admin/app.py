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
from fastapi.responses import (HTMLResponse, JSONResponse,
                               RedirectResponse, Response)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
SUPPORT_EMAIL = os.environ.get("SUPPORT_EMAIL",
                               "support@datagatelodge.com")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "").rstrip("/")
GATEWAY_KEY = os.environ.get("GATEWAY_KEY", "")
HELPDESK_URL = os.environ.get("HELPDESK_URL", "").rstrip("/")
REVIEW_KEY = os.environ.get("REVIEW_KEY", "")
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
            # the Gatekeeper records the triage rule that fired
            conn.execute("ALTER TABLE inbound_emails "
                         "ADD COLUMN IF NOT EXISTS rule TEXT DEFAULT ''")
            # intent map (written by the Gatekeeper, read here)
            conn.execute("""CREATE TABLE IF NOT EXISTS kb_intent (
                doc TEXT NOT NULL,
                section TEXT NOT NULL,
                questions TEXT NOT NULL,
                summary TEXT DEFAULT '',
                topics TEXT DEFAULT '',
                updated_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (doc, section)
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
            # KB provenance: every save/create/reset/restore, with the
            # content before and after — the audit trail for doc edits
            conn.execute("""CREATE TABLE IF NOT EXISTS kb_history (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                action TEXT NOT NULL,
                old_content TEXT,
                new_content TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )""")
            # system rulesets (task tool gate, email triage, chat
            # decisions) — same shape the Gatekeeper seeds
            conn.execute("""CREATE TABLE IF NOT EXISTS rulesets (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                kind TEXT DEFAULT 'general',
                description TEXT DEFAULT '',
                version INT DEFAULT 1,
                jdm TEXT NOT NULL,
                enabled BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT now()
            )""")
            # rerank-prompt escalation criteria + agent thresholds
            conn.execute("""CREATE TABLE IF NOT EXISTS escalation_topics (
                id BIGSERIAL PRIMARY KEY,
                topic TEXT UNIQUE NOT NULL,
                guidance TEXT NOT NULL,
                enabled BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT now()
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS agent_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT now()
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
       ("/policies", "Policies"),
       ("/analytics", "Analytics"), ("/contacts", "Contacts"),
       ("/clients", "Clients"),
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
/* policy decision-table grid (Lodge Office theme) */
.ptable { width:100%; border-collapse:collapse; font-size:.78rem; }
.ptable th, .ptable td { border:1px solid var(--line); padding:.2rem .3rem;
  vertical-align:top; }
.ptable th { background:var(--surface); }
.ptable input.f { display:block; width:92px; margin-bottom:.15rem;
  background:var(--ink); color:var(--paper); border:1px solid var(--line);
  border-radius:4px; padding:.15rem .3rem; }
.ptable input.n { display:block; width:92px;
  background:var(--ink); color:var(--paper); border:1px solid var(--line);
  border-radius:4px; padding:.15rem .3rem; }
.ptable input.cell, .ptable input.rname { width:100%; min-width:70px;
  background:var(--ink); color:var(--paper); border:1px solid var(--line);
  border-radius:4px; padding:.15rem .3rem; font-family:Consolas,
  'Courier New',monospace; font-size:.75rem; }
.ptable button.mini { background:none; border:none; color:var(--danger);
  cursor:pointer; font-size:.8rem; padding:0 .2rem; }
.ptable tr.drafted td { background:rgba(212,175,55,.07); }
.lint-item { margin:.15rem 0; }
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
            await _kb_history_add(doc, "save", base[0], content)
        else:
            # seed the override from the gatekeeper's built-in doc
            import pathlib
            p = (pathlib.Path(__file__).resolve().parent
                 / "kb" / f"{doc}.md")
            seed = p.read_text(encoding="utf-8") if p.exists() else (
                f"title: {doc}\ndescription: (generated)\n")
            content = seed + f"\n\n## Q: {question}\nA: {reply}\n"
            await _x("INSERT INTO kb_overrides (name, content) "
                     "VALUES (%s, %s)", (doc, content))
            await _kb_history_add(doc, "save", None, content)

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


def _kb_is_internal(content: str) -> bool:
    return bool(re.search(r"^internal:\s*true\s*$", content, re.M | re.I))


# ── security lint: docs + prompt templates ────────────────────────────────
# Runs on every doc listing/edit and over the Gatekeeper's prompt
# templates. Catches the failure mode that matters for a knowledge base:
# something internal leaking into something public.

_LINT_EMAIL_OK = ("@datagatelodge.com", "@xs.technology")
_LINT_EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_LINT_PATTERNS = [
    ("secret", "high", re.compile(r"\b(?:sk|rk)-[A-Za-z0-9_-]{16,}\b")),
    ("secret", "high", re.compile(r"\bghp_[A-Za-z0-9]{20,}\b")),
    ("secret", "high", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("secret", "high", re.compile(r"\b[0-9a-f]{40,}\b|\b[0-9a-f]{32}\b")),
    ("secret", "high", re.compile(
        r"(?i)\b(?:api[_-]?key|secret|password|passwd)\b\s*[:=]\s*\S{8,}")),
    # our infrastructure pattern — customers never see RFC1918 in docs
    ("private-net", "high", re.compile(
        r"\b(?:10\.\d{1,3}|192\.168\.\d{1,3}|"
        r"172\.(?:1[6-9]|2\d|3[01])\.\d{1,3})\.\d{1,3}\b")),
    # localhost:8010 is the product's documented console default (the
    # customer's own deployment) — other ports are worth a look
    ("local-port", "warn", re.compile(
        r"(?i)\blocalhost:(?!8010\b)\d{2,5}\b")),
    ("phone", "warn", re.compile(r"\b\d{3}[-.]\d{3}[-.]\d{4}\b")),
    ("todo", "warn", re.compile(
        r"(?i)\b(?:TODO|FIXME|lorem ipsum|placeholder text)\b")),
]
# phrases that only make sense in staff docs — flagged in PUBLIC docs
# ("admin key" is deliberately absent: the customer's own console uses
# admin keys, so the phrase is public product vocabulary)
_LINT_PUBLIC_ONLY = [
    ("internal-mention", "high", re.compile(
        r"(?i)\b(?:lodge office|support playbook|competitive playbook|"
        r"review key|inbound key|tier[- ]2)\b")),
]


def _lint_text(text: str, internal: bool = True) -> list[dict]:
    """Regex findings: {rule, severity, line, excerpt}. internal docs
    skip the internal-mention rule (it is their native habitat)."""
    out = []
    pats = list(_LINT_PATTERNS) + ([] if internal else _LINT_PUBLIC_ONLY)
    lines = text.split("\n")
    for i, line in enumerate(lines, 1):
        for rule, sev, rx in pats:
            for m in rx.finditer(line):
                out.append({"rule": rule, "severity": sev, "line": i,
                            "excerpt": m.group(0)[:60]})
        if rule_email := _LINT_EMAIL_RE.search(line):
            addr = rule_email.group(0).lower()
            if not any(addr.endswith(d) for d in _LINT_EMAIL_OK):
                out.append({"rule": "email", "severity": "warn",
                            "line": i, "excerpt": addr[:60]})
    # dedupe (a line can hit a rule repeatedly)
    seen, uniq = set(), []
    for f in out:
        k = (f["rule"], f["line"], f["excerpt"])
        if k not in seen:
            seen.add(k)
            uniq.append(f)
    return uniq


def _lint_badge(findings: list[dict]) -> str:
    if not findings:
        return "<span class='tag ok' title='no findings'>clean</span>"
    tip = _e("; ".join(f"{f['rule']}@{f['line']}"
                       for f in findings[:6]))
    high = sum(1 for f in findings if f["severity"] == "high")
    if high:
        return f"<span class='tag bad' title='{tip}'>{high} high</span>"
    return (f"<span class='tag warn' title='{tip}'>"
            f"{len(findings)} warn</span>")


async def _kb_history_add(name: str, action: str, old, new) -> None:
    """Record one provenance entry. old/new: str | None."""
    await _x(
        "INSERT INTO kb_history (name, action, old_content, new_content) "
        "VALUES (%s, %s, %s, %s)",
        (name, action, old, new))


async def _fetch_prompts() -> list[dict]:
    """The Gatekeeper's prompt templates (name + role + text), for
    linting. Fail-soft: an unreachable Gatekeeper is not a broken
    Knowledge screen."""
    if not (HELPDESK_URL and REVIEW_KEY):
        return []
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(
                f"{HELPDESK_URL}/api/kb/prompts",
                headers={"Authorization": f"Bearer {REVIEW_KEY}"})
            if r.status_code == 200:
                return r.json().get("prompts", [])
    except Exception as e:
        print(f"[kb] prompt fetch failed: {e!r}")
    return []


def _parse_doc(content: str) -> tuple[str, str, str]:
    """Split a KB doc into (title, description, body).

    Docs carry a small header the Gatekeeper's triage stage routes on:
        # Title
        title: …
        description: …
        [internal: true]      <- staff-only, never served to customers
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
            continue
        if re.match(r"^internal:\s*true\s*$", s, re.I):
            last = i
    if last < 0:
        return "", "", content.strip("\n")
    body = "\n".join(lines[last + 1:]).strip("\n")
    if body.startswith("# "):          # drop the stale H1 — save re-adds it
        body = "\n".join(body.splitlines()[1:]).strip("\n")
    return title, desc, body


def _assemble_doc(title: str, description: str, body: str,
                  internal: bool = False) -> str:
    return (f"# {title}\ntitle: {title}\ndescription: {description}\n"
            + ("internal: true\n" if internal else "")
            + "\n" + body.strip("\n") + "\n")


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
    ovc = {n: c for n, c in await _q(
        "SELECT name, content FROM kb_overrides")}
    over = set(ovc)
    custom = sorted(over - set(base))
    iq = {n: q for n, q in await _q(
        "SELECT doc, SUM(jsonb_array_length(questions::jsonb)) "
        "FROM kb_intent GROUP BY doc")}

    rows = ""
    for n in base:
        state = ('<span class="tag warn">live-edited</span>' if n in over
                 else '<span class="tag ok">built-in</span>')
        content = ovc.get(n) or (_kb_dir() / f"{n}.md").read_text(
            encoding="utf-8")
        iv = _kb_is_internal(content)
        if iv:
            state += ' <span class="tag bad" title="staff-only — the ' \
                     'Gatekeeper never serves this">internal</span>'
        nq = iq.get(n)
        intent = (f"<span class='tag' title='pre-analyzed questions'>"
                  f"answers {nq} questions</span>" if nq
                  else "<span class='tag'>not analyzed</span>")
        rows += (f"<tr><td class='pill'>{_e(n)}</td><td>{state}</td>"
                 f"<td>{_lint_badge(_lint_text(content, internal=iv))}</td>"
                 f"<td>{intent}</td>"
                 f"<td><a class='btn ghost' style='padding:.2rem .7rem;"
                 f"font-size:.72rem' href='/kb/edit?name={_e(n)}'>edit</a>"
                 f"</td></tr>")
    for n in custom:
        nq = iq.get(n)
        intent = (f"<span class='tag' title='pre-analyzed questions'>"
                  f"answers {nq} questions</span>" if nq
                  else "<span class='tag'>not analyzed</span>")
        rows += (f"<tr><td class='pill'>{_e(n)}</td>"
                 f"<td><span class='tag'>custom</span></td>"
                 f"<td>{_lint_badge(_lint_text(ovc[n]))}</td><td>{intent}</td>"
                 f"<td><a class='btn ghost' style='padding:.2rem .7rem;"
                 f"font-size:.72rem' href='/kb/edit?name={_e(n)}'>edit</a> "
                 f"<a class='btn ghost' style='padding:.2rem .7rem;"
                 f"font-size:.72rem' href='/kb/reset?name={_e(n)}'"
                 f" onclick=\"return confirm('remove {_e(n)}?')\""
                 f">remove</a></td></tr>")

    # security lint over the Gatekeeper's prompt templates
    prompts = await _fetch_prompts()
    prow = ""
    for p in prompts:
        f = _lint_text(p["text"])
        prow += (f"<tr><td class='pill'>{_e(p['name'])}</td>"
                 f"<td class='note'>{_e(p.get('role', ''))}</td>"
                 f"<td>{_lint_badge(f)}</td></tr>")
    prompts_block = f"""
<div class="box" style="margin-bottom:1.4rem">
<p class="note" style="letter-spacing:.14em;text-transform:uppercase;
color:var(--brass);margin-bottom:.7rem">Security lint — the Gatekeeper's
prompt templates</p>
<table><tr><th>prompt</th><th>role</th><th>lint</th></tr>
{prow or '<tr><td colspan=3 class=note>Gatekeeper not reachable — '
'prompt templates shown when it is.</td></tr>'}</table>
<p class="note" style="margin-top:.5rem">Same checks as the documents:
secrets, private addresses, stray emails. Prompts are code — fix
findings in the repo, not here.</p></div>""" if prompts else ""

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
{prompts_block}
<table><tr><th>doc</th><th>state</th><th>security</th><th>intent map</th>
<th></th></tr>{rows}</table>"""
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
    await _kb_history_add("faq", "sync", ov[0] if ov else None, doc)
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
    await _kb_history_add(name, "create", None, doc)
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
    internal = _kb_is_internal(content)
    internal_badge = ('<p style="margin:.2rem 0 .6rem"><span class="tag bad"'
                      ' title="staff-only — the Gatekeeper never serves '
                      'this to customers">internal — staff only, never '
                      'served to customers</span></p>') if internal else ""
    # pre-analyzed intent map: what the Gatekeeper believes this doc
    # answers — reviewable, refreshable
    imap = await _q("SELECT section, questions, summary FROM kb_intent "
                    "WHERE doc=%s ORDER BY section", (name,))
    imap_html = ""
    for section, qjson, summary in imap:
        try:
            qs = json.loads(qjson)
        except Exception:
            qs = []
        qhtml = "".join(f"<p class='note' style='margin:.15rem 0'>— "
                        f"{_e(q)}</p>" for q in qs)
        imap_html += (f"<div style='margin-bottom:.7rem'>"
                      f"<b>{_e(section)}</b>"
                      f"<p class='note' style='margin:.1rem 0'>"
                      f"{_e(summary)}</p>{qhtml}</div>")
    intent_block = f"""
<div class="box" style="margin-top:1.2rem">
<p class="note" style="letter-spacing:.14em;text-transform:uppercase;
color:var(--brass);margin-bottom:.6rem">Intent map — what the AI says
this document answers</p>
{imap_html or '<p class="note">Not analyzed yet — the Gatekeeper '
'analyzes new documents automatically, or press Re-analyze.</p>'}
<form method="post" action="/kb/reanalyze" class="inline">
<input type="hidden" name="name" value="{_e(name)}">
<button class="btn ghost" style="padding:.3rem .8rem;font-size:.75rem">
Re-analyze</button></form>
<span class="note">the map drives question routing — save first, then
re-analyze to refresh it</span></div>"""
    # security lint of the content being edited
    findings = _lint_text(content, internal=internal)
    frows = "".join(
        f"<p class='lint-item'><span class='tag "
        f"{'bad' if f['severity'] == 'high' else 'warn'}'>{_e(f['rule'])}"
        f"</span> line {f['line']} — <span class='mono'>"
        f"{_e(f['excerpt'])}</span></p>" for f in findings)
    lint_block = f"""
<div class="box" style="margin-top:1.2rem">
<p class="note" style="letter-spacing:.14em;text-transform:uppercase;
color:var(--brass);margin-bottom:.6rem">Security lint</p>
{frows or '<p class="note">No findings — no secrets, private addresses, '
'stray emails, or internal phrases in a public doc.</p>'}
<p class="note" style="margin-top:.4rem">high findings should block a
save; warns are worth a look.</p></div>"""

    # provenance: every version this doc has been through
    hist = await _q(
        "SELECT id, action, old_content, new_content, created_at "
        "FROM kb_history WHERE name=%s ORDER BY id DESC LIMIT 12",
        (name,), fetch="all")
    hrows = ""
    for hid, act, oldc, newc, at in hist:
        sz = (f"{len(oldc) if oldc else 0} → "
              f"{len(newc) if newc else 0} chars")
        hrows += (f"<tr><td class='note'>{str(at)[:16]}</td>"
                  f"<td><span class='tag'>{_e(act)}</span></td>"
                  f"<td class='note'>{sz}</td>"
                  f"<td><a class='btn ghost' style='padding:.15rem .5rem;"
                  f"font-size:.7rem' href='/kb/version?id={hid}'>view</a>"
                  + (f" <form method='post' action='/kb/restore' "
                     f"class='inline'><input type='hidden' name='id' "
                     f"value='{hid}'><button class='btn ghost' "
                     f"style='padding:.15rem .5rem;font-size:.7rem' "
                     f"onclick=\"return confirm('restore this version?"
                     f"')\">restore</button></form>" if newc else "")
                  + "</td></tr>")
    history_block = f"""
<div class="box" style="margin-top:1.2rem">
<p class="note" style="letter-spacing:.14em;text-transform:uppercase;
color:var(--brass);margin-bottom:.6rem">Provenance — version history</p>
<table><tr><th>when</th><th>action</th><th>size</th><th></th></tr>
{hrows or '<tr><td colspan=4 class=note>No history yet — saves, resets '
'and restores are recorded here.</td></tr>'}</table></div>"""

    danger = ("<a class='btn ghost' href='/kb/reset?name="
              f"{_e(name)}'>reset to built-in</a>" if builtin else
              f"<a class='btn ghost' href='/kb/reset?name={_e(name)}'"
              f" onclick=\"return confirm('remove {name}?')\""
              f">remove document</a>")
    body_form = f"""
{internal_badge}
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
<a class="btn ghost" href="/kb">back</a></div></form>
{intent_block}
{lint_block}
{history_block}"""
    return page(request, f"Knowledge · <em>{_e(name)}</em>", body_form,
                flash=flash)


async def _request_reanalyze(doc: str) -> None:
    """Ask the Gatekeeper to re-analyze a doc's intent map (background)."""
    if not (HELPDESK_URL and REVIEW_KEY):
        return
    try:
        async with httpx.AsyncClient(timeout=120) as c:
            r = await c.post(
                f"{HELPDESK_URL}/api/kb/reanalyze",
                json={"doc": doc},
                headers={"Authorization": f"Bearer {REVIEW_KEY}"})
            print(f"[kb] reanalyze {doc}: {r.status_code} {r.text[:120]}")
    except Exception as e:
        print(f"[kb] reanalyze {doc} failed: {e!r}")


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
    # internal docs stay internal across edits — publishing one is a
    # deliberate git-level change, never a form slip
    cur = await _q("SELECT content FROM kb_overrides WHERE name=%s",
                  (name,), fetch="one")
    if not cur:
        p = _kb_dir() / f"{name}.md"
        cur = (p.read_text(encoding="utf-8"),) if p.exists() else None
    internal = bool(cur and _kb_is_internal(cur[0]))
    doc = _assemble_doc(title, description, content, internal=internal)
    await _x("INSERT INTO kb_overrides (name, content) VALUES (%s, %s) "
             "ON CONFLICT (name) DO UPDATE SET content=%s, "
             "updated_at=now()", (name, doc, doc))
    await _kb_history_add(name, "save",
                          cur[0] if cur else None, doc)
    await _audit("kb-save", f"{name} ({len(doc)} chars)"
                 + (" [internal]" if internal else ""))
    # the Gatekeeper re-analyzes what this doc answers (background)
    asyncio.create_task(_request_reanalyze(name))
    return RedirectResponse(
        f"/kb/edit?name={name}&flash=saved+—+live+on+the+next+question",
        status_code=303)


@app.post("/kb/reanalyze")
async def kb_reanalyze_route(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    name = str(form.get("name", "")).strip()
    if not (HELPDESK_URL and REVIEW_KEY):
        return RedirectResponse(
            f"/kb/edit?name={name}&flash=intent+engine+not+configured",
            status_code=303)
    try:
        async with httpx.AsyncClient(timeout=180) as c:
            r = await c.post(
                f"{HELPDESK_URL}/api/kb/reanalyze",
                json={"doc": name},
                headers={"Authorization": f"Bearer {REVIEW_KEY}"})
        j = r.json() if r.status_code == 200 else {}
        n = sum(j.get("analyzed", {}).values())
        await _audit("kb-reanalyze", name)
        return RedirectResponse(
            f"/kb/edit?name={name}&flash=re-analyzed+{n}+sections",
            status_code=303)
    except Exception as e:
        return RedirectResponse(
            f"/kb/edit?name={name}&flash=re-analysis+failed:+"
            f"{_e(repr(e)[:60])}", status_code=303)


@app.get("/kb/reset")
async def kb_reset(request: Request, name: str = ""):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    row = await _q("SELECT content FROM kb_overrides WHERE name=%s",
                   (name,), fetch="one")
    p = _kb_dir() / f"{name}.md"
    base_content = (p.read_text(encoding="utf-8")
                    if p.exists() else None)
    await _x("DELETE FROM kb_overrides WHERE name=%s", (name,))
    await _kb_history_add(name, "reset",
                          row[0] if row else None, base_content)
    await _audit("kb-reset", name)
    return RedirectResponse("/kb", status_code=303)


@app.get("/kb/version", response_class=HTMLResponse)
async def kb_version(request: Request, id: int = 0):
    if (r := _guard(request)):
        return r
    row = await _q(
        "SELECT id, name, action, old_content, new_content, created_at "
        "FROM kb_history WHERE id=%s", (id,), fetch="one")
    if not row:
        return RedirectResponse("/kb?flash=version+not+found",
                                status_code=303)
    hid, name, act, oldc, newc, at = row
    shown = newc if newc is not None else oldc
    label = ("state after this action" if newc is not None
             else "content discarded by this action")
    body = f"""
<div class="inline" style="margin-bottom:1rem">
<a class="btn ghost" href="/kb/edit?name={_e(name)}">← back to
{_e(name)}</a></div>
<p><b>{_e(name)}</b> · <span class="tag">{_e(act)}</span> ·
<span class="note">{str(at)[:16]}</span> — {label}</p>
<div class="view-body mono" style="margin-top:.8rem">{_e(shown or '')}</div>"""
    return page(request, f"Knowledge · <em>version {hid}</em>", body)


@app.post("/kb/restore")
async def kb_restore(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    hid = int(form.get("id") or 0)
    row = await _q(
        "SELECT name, new_content FROM kb_history WHERE id=%s",
        (hid,), fetch="one")
    if not row or row[1] is None:
        return RedirectResponse("/kb?flash=version+not+restorable",
                                status_code=303)
    name, content = row
    cur = await _q("SELECT content FROM kb_overrides WHERE name=%s",
                   (name,), fetch="one")
    await _x("INSERT INTO kb_overrides (name, content) VALUES (%s, %s) "
             "ON CONFLICT (name) DO UPDATE SET content=%s, "
             "updated_at=now()", (name, content, content))
    await _kb_history_add(name, "restore",
                          cur[0] if cur else None, content)
    await _audit("kb-restore", f"{name} from version {hid}")
    asyncio.create_task(_request_reanalyze(name))
    return RedirectResponse(
        f"/kb/edit?name={name}&flash=restored+from+version+{hid}",
        status_code=303)


# ── system rulesets: deterministic policies as reviewable data ───────────
# The Gatekeeper loads these per request: task-tool-gate runs before
# every task-agent tool call, email-triage routes every inbound message,
# chat-decision decides visitor rulings. Editing here changes live agent
# behavior on the next request — that is the point (policies as data,
# not code), and why every save versions up and audits.

_KIND_LABELS = {
    "task-tool-gate": "pre-tool gate — runs before every task-agent "
                      "tool call (fail-open when disabled)",
    "email-triage": "inbound email triage — routes every support "
                    "message (auto-reply vs inbox)",
    "chat-decision": "visitor decisions — refund/eligibility rulings "
                     "in chat and email",
}


def _policy_model(jdm_json: str) -> dict | None:
    """Parse a single-table JDM graph into an editable model."""
    try:
        g = json.loads(jdm_json)
        for n in g.get("nodes", []):
            if n.get("type") != "decisionTableNode":
                continue
            ct = n.get("content", {})
            ins = [(i.get("field", ""), i.get("name", ""),
                    i.get("id", "")) for i in ct.get("inputs", [])]
            outs = [(o.get("field", ""), o.get("name", ""),
                     o.get("id", "")) for o in ct.get("outputs", [])]
            rules = []
            for r in ct.get("rules", []):
                cells = [str(r.get(cid, "")) for _, _, cid in ins + outs]
                rules.append({"name": str(r.get("_id", "")),
                              "cells": cells})
            return {"hitPolicy": ct.get("hitPolicy", "first"),
                    "inputs": ins, "outputs": outs, "rules": rules}
    except Exception:
        pass
    return None


_COND_RE = re.compile(
    r'^(?:(==|!=|<=|>=|<|>)\s*("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?'
    r'|true|false)?)?$')
_VAL_RE = re.compile(
    r'^(true|false|"(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?)?$')


def _cells_valid(in_fields: list[str], out_fields: list[str],
                 cells: dict) -> bool:
    """zen accepts any cell text and silently never matches bad
    expressions — the grammar is enforced here instead."""
    return all(_COND_RE.match(str(cells.get(f, ""))) for f in in_fields) \
        and all(_VAL_RE.match(str(cells.get(f, ""))) for f in out_fields)


def _jdm_valid(jdm_json: str) -> bool:
    try:
        from zen import ZenEngine
        ZenEngine().create_decision(json.loads(jdm_json))
        model = _policy_model(jdm_json)
        if not model:
            return False
        in_f = [f for f, _, _ in model["inputs"]]
        out_f = [f for f, _, _ in model["outputs"]]
        return all(_cells_valid(in_f, out_f, dict(zip(in_f + out_f,
                                                      r["cells"])))
                   for r in model["rules"])
    except Exception:
        return False


POLICY_JS = """
function addRule() {
  const grid = document.getElementById('grid');
  const nIn = grid.querySelectorAll('thead th[data-kind=in]').length;
  const nOut = grid.querySelectorAll('thead th[data-kind=out]').length;
  const tr = document.createElement('tr');
  let h = '<td><input class="rname" placeholder="rule id"></td>';
  for (let i = 0; i < nIn + nOut; i++) {
    const kind = i < nIn ? 'in' : 'out';
    h += `<td><input class="cell" data-kind="${kind}" data-col="${kind==='in'?i:i-nIn}"></td>`;
  }
  h += '<td><button type="button" class="mini" onclick="this.closest(\\'tr\\').remove()">✕</button></td>';
  tr.innerHTML = h;
  grid.querySelector('tbody').appendChild(tr);
}
function addCol(kind) {
  const grid = document.getElementById('grid');
  const idx = grid.querySelectorAll(`thead th[data-kind=${kind}]`).length;
  const th = document.createElement('th');
  th.dataset.kind = kind;
  th.innerHTML = `<input class="f mono" placeholder="field" style="font-size:.72rem"><input class="n" placeholder="label" style="font-size:.72rem"><button type="button" class="mini" onclick="delCol(this)">✕</button>`;
  const anchor = kind === 'in' ? grid.querySelector('thead th[data-kind=out]')
                               : grid.querySelector('thead th.mini-col');
  anchor.parentNode.insertBefore(th, anchor);
  grid.querySelectorAll('tbody tr').forEach(tr => {
    const td = document.createElement('td');
    td.innerHTML = `<input class="cell" data-kind="${kind}" data-col="${idx}">`;
    const cellIdx = kind === 'in' ? idx + 1 : tr.children.length - 1;
    tr.insertBefore(td, tr.children[cellIdx]);
  });
}
function delCol(btn) {
  const th = btn.closest('th');
  const kind = th.dataset.kind;
  if (document.querySelectorAll(`#grid thead th[data-kind=${kind}]`).length <= 1) return;
  const idx = [...th.parentNode.children].indexOf(th);
  th.remove();
  document.querySelectorAll('#grid tbody tr').forEach(tr =>
    tr.children[idx] && tr.children[idx].remove());
}
function buildJdm() {
  const grid = document.getElementById('grid');
  const ins = [...grid.querySelectorAll('thead th[data-kind=in]')].map((th, i) => ({
    field: th.querySelector('.f').value || ('f' + (i + 1)),
    id: 'c' + (i + 1),
    name: th.querySelector('.n').value || th.querySelector('.f').value || ('in' + (i + 1)),
    type: 'expression'
  }));
  const outs = [...grid.querySelectorAll('thead th[data-kind=out]')].map((th, i) => ({
    field: th.querySelector('.f').value || ('out' + (i + 1)),
    id: 'o' + (i + 1),
    name: th.querySelector('.n').value || th.querySelector('.f').value || ('out' + (i + 1)),
    type: 'expression'
  }));
  const rules = [...grid.querySelectorAll('tbody tr')].map((tr, i) => {
    const r = { _id: tr.querySelector('.rname').value || ('r' + (i + 1)) };
    tr.querySelectorAll('input.cell').forEach(inp => {
      const k = inp.dataset.kind, ci = +inp.dataset.col;
      r[k === 'in' ? ins[ci].id : outs[ci].id] = inp.value;
    });
    return r;
  });
  const name = document.getElementById('pname').value || 'Decision';
  return {
    edges: [{ id: 'e1', sourceId: 'n-input', targetId: 'n-table' },
            { id: 'e2', sourceId: 'n-table', targetId: 'n-output' }],
    nodes: [
      { id: 'n-input', type: 'inputNode', name: 'Facts' },
      { id: 'n-output', type: 'outputNode', name: 'Decision' },
      { id: 'n-table', type: 'decisionTableNode', name: name,
        content: { hitPolicy: document.getElementById('hitpolicy').value,
                   inputs: ins, outputs: outs, rules: rules } }
    ]
  };
}
document.getElementById('pform').addEventListener('submit', () => {
  document.getElementById('jdm').value = JSON.stringify(buildJdm());
});
async function testPolicy() {
  const pid = document.getElementById('pid').value;
  const out = document.getElementById('testout');
  if (!pid) { out.textContent = 'No policy id.'; return; }
  let facts;
  try { facts = JSON.parse(document.getElementById('facts').value || '{}'); }
  catch (e) { out.textContent = 'Facts must be valid JSON: ' + e.message; return; }
  out.textContent = 'evaluating…';
  try {
    const r = await fetch('/policy/' + pid + '/test', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ facts: facts })
    });
    const j = await r.json();
    out.textContent = JSON.stringify(j, null, 2);
  } catch (e) { out.textContent = 'ERROR: ' + e; }
}
function fillFacts() {
  const grid = document.getElementById('grid');
  const f = {};
  grid.querySelectorAll('thead th[data-kind=in] .f').forEach(i => { if (i.value) f[i.value] = ''; });
  document.getElementById('facts').value = JSON.stringify(f, null, 2);
}
"""


@app.get("/policies", response_class=HTMLResponse)
async def policies_index(request: Request, flash: str = ""):
    if (r := _guard(request)):
        return r
    rows = await _q(
        "SELECT id, name, kind, description, version, enabled, jdm "
        "FROM rulesets ORDER BY id")
    trs = ""
    for rid, name, kind, desc, ver, en, jdm in rows:
        model = _policy_model(jdm)
        nrules = len(model["rules"]) if model else 0
        what = _KIND_LABELS.get(kind, kind)
        state = ('<span class=tag ok>on</span>' if en
                 else '<span class=tag bad>off</span>')
        toggle = (f"<form method='post' action='/policy/toggle' "
                  f"class='inline'><input type='hidden' name='id' "
                  f"value='{rid}'><button class='btn ghost' "
                  f"style='padding:.15rem .5rem;font-size:.7rem'>"
                  f"{'disable' if en else 'enable'}</button></form>")
        trs += (f"<tr><td>{_e(name)}</td>"
                f"<td class='pill'>{_e(kind)}</td>"
                f"<td class='note'>{_e(what)}</td>"
                f"<td class='mono note'>v{ver}</td>"
                f"<td>{nrules} rules</td><td>{state}</td>"
                f"<td><a class='btn ghost' style='padding:.15rem .5rem;"
                f"font-size:.7rem' href='/policy/{rid}'>edit</a> "
                f"<a class='btn ghost' style='padding:.15rem .5rem;"
                f"font-size:.7rem' href='/policy/{rid}#test'>test</a> "
                f"{toggle}</td></tr>")

    # escalation topics — the rerank prompt's legal list, as data
    erows = await _q(
        "SELECT id, topic, guidance, enabled FROM escalation_topics "
        "ORDER BY id")
    etrs = ""
    for eid, topic, guidance, en in erows:
        state = ('<span class=tag ok>on</span>' if en
                 else '<span class=tag bad>off</span>')
        etrs += (f"<tr><td>{_e(topic)}</td>"
                 f"<td class='note'>{_e(guidance[:110])}</td>"
                 f"<td>{state}</td>"
                 f"<td><a class='btn ghost' style='padding:.15rem .5rem;"
                 f"font-size:.7rem' href='/escalation/{eid}'>edit</a> "
                 f"<form method='post' action='/escalation/toggle' "
                 f"class='inline'><input type='hidden' name='id' "
                 f"value='{eid}'><button class='btn ghost' "
                 f"style='padding:.15rem .5rem;font-size:.7rem'>"
                 f"{'disable' if en else 'enable'}</button></form>"
                 f"</td></tr>")
    escalation_block = f"""
<div class="box" style="margin-top:1.8rem">
<p class="note" style="letter-spacing:.14em;text-transform:uppercase;
color:var(--brass);margin-bottom:.7rem">Escalation topics — what the
AI always sends to the team</p>
<p class="note" style="margin-bottom:.7rem">These feed the Gatekeeper's
question triage: a question matching a topic is escalated (legal=true)
instead of answered. Seeded from the repo; edits go live on the next
question.</p>
<table><tr><th>topic</th><th>matches</th><th>state</th><th></th></tr>
{etrs or '<tr><td colspan=4 class=note>None — the Gatekeeper falls '
'back to its built-in list.</td></tr>'}</table></div>"""

    # agent settings — retrieval + reply thresholds, as data
    srows = await _q(
        "SELECT key, value, updated_at FROM agent_settings "
        "ORDER BY key")
    SETTING_HELP = {
        "retrieval_top_k": "candidates the keyword stage sends to the "
                           "AI triage (1-50)",
        "rerank_pick_max": "passages an answer may cite (1-5)",
        "inbound_daily_cap": "auto-reply emails per sender per day "
                             "(1-100)",
    }
    str_ = ""
    for k, v, at in srows:
        str_ += (f"<tr><td class='pill'>{_e(k)}</td>"
                 f"<td class='mono'>{_e(v)}</td>"
                 f"<td class='note'>{_e(SETTING_HELP.get(k, ''))}</td>"
                 f"<td><form method='post' action='/settings/save' "
                 f"class='inline'><input type='hidden' name='key' "
                 f"value='{_e(k)}'><input type='text' name='value' "
                 f"value='{_e(v)}' style='width:70px'> "
                 f"<button class='btn ghost' style='padding:.15rem "
                 f".5rem;font-size:.7rem'>save</button></form></td></tr>")
    settings_block = f"""
<div class="box" style="margin-top:1.2rem">
<p class="note" style="letter-spacing:.14em;text-transform:uppercase;
color:var(--brass);margin-bottom:.7rem">Agent settings — routing
thresholds</p>
<table><tr><th>setting</th><th>value</th><th>controls</th><th></th></tr>
{str_ or '<tr><td colspan=4 class=note>Defaults in effect — the '
'Gatekeeper seeds these at startup.</td></tr>'}</table>
<p class="note" style="margin-top:.5rem">Read per question (15s cache):
changes apply to the next request, no restart.</p></div>"""
    body = f"""
<p class="note" style="margin-bottom:1.2rem">Deterministic system
policies. The Gatekeeper loads these on every request — the tool gate
before each task-agent tool call, email triage on every inbound
message, the decision policy behind visitor rulings. Edits take effect
immediately and version up; every change is audited. Disabling a gate
fail-opens the behavior it guards.</p>
<table><tr><th>policy</th><th>kind</th><th>gates</th><th>ver</th>
<th>rules</th><th>state</th><th></th></tr>
{trs or '<tr><td colspan=7 class=note>No rulesets — the Gatekeeper '
'seeds these at startup.</td></tr>'}</table>
<p class="note" style="margin-top:1rem">Policies are data, not code:
the model extracts facts, this table decides, the run trace names the
rule that fired. New kinds are wired in the repo — these screens edit
what exists.</p>
{escalation_block}
{settings_block}"""
    return page(request, "Policies", body, flash=flash)


@app.get("/policy/{rid}", response_class=HTMLResponse)
async def policy_edit(request: Request, rid: int, flash: str = ""):
    if (r := _guard(request)):
        return r
    row = await _q(
        "SELECT id, name, kind, description, version, enabled, jdm "
        "FROM rulesets WHERE id=%s", (rid,), fetch="one")
    if not row:
        return RedirectResponse("/policies?flash=not+found",
                                status_code=303)
    pid, name, kind, desc, ver, en, jdm = row
    model = _policy_model(jdm) or {
        "hitPolicy": "first", "inputs": [], "outputs": [], "rules": []}

    def th(col, kind_):
        f, lbl, _ = col
        return (f'<th data-kind="{kind_}">'
                f'<input class="f mono" value="{_e(f)}" '
                f'placeholder="field" style="font-size:.72rem">'
                f'<input class="n" value="{_e(lbl)}" '
                f'placeholder="label" style="font-size:.72rem">'
                f'<button type="button" class="mini" '
                f'onclick="delCol(this)">✕</button></th>')

    head = ('<th style="min-width:110px">rule</th>'
            + "".join(th(c, "in") for c in model["inputs"])
            + "".join(th(c, "out") for c in model["outputs"])
            + '<th class="mini-col"></th>')
    rows_html = ""
    for rl in model["rules"]:
        cells = ""
        for i, val in enumerate(rl["cells"]):
            k = "in" if i < len(model["inputs"]) else "out"
            ci = i if k == "in" else i - len(model["inputs"])
            cells += (f'<td><input class="cell" data-kind="{k}" '
                      f'data-col="{ci}" value="{_e(val)}"></td>')
        rows_html += (f'<tr><td><input class="rname" '
                      f'value="{_e(rl["name"])}"></td>{cells}'
                      f'<td><button type="button" class="mini" '
                      f'onclick="this.closest(\'tr\').remove()">✕'
                      f'</button></td></tr>')
    hp = model["hitPolicy"]
    body = f"""
<div class="inline" style="margin-bottom:1rem">
<a class="btn ghost" href="/policies">← policies</a>
<span class="mono note">v{ver} · {_e(kind)}</span>
<span class="tag {'ok' if en else 'bad'}">{'enabled' if en else 'disabled'}</span></div>
<form method="post" action="/policy/save" id="pform">
<input type="hidden" name="id" value="{pid}">
<input type="hidden" name="jdm" id="jdm" value="">
<label>policy name</label>
<input id="pname" name="name" value="{_e(name)}" required>
<label>description</label>
<input name="description" value="{_e(desc)}">
<label class="inline" style="gap:.5rem">
<input type="checkbox" name="enabled" value="1"
{' checked' if en else ''} style="width:auto"> enabled — unchecking
fail-opens what this policy guards</label>
<label>hit policy</label>
<select id="hitpolicy" name="hitpolicy">
<option value="first"{' selected' if hp == 'first' else ''}>first match — first rule that fits decides</option>
<option value="collect"{' selected' if hp == 'collect' else ''}>collect — all matching rules combine</option>
</select>
<div class="inline" style="margin:.6rem 0">
<button type="button" class="btn ghost" style="padding:.3rem .8rem;
font-size:.75rem" onclick="addRule()">+ rule</button>
<button type="button" class="btn ghost" style="padding:.3rem .8rem;
font-size:.75rem" onclick="addCol('in')">+ condition column</button>
<button type="button" class="btn ghost" style="padding:.3rem .8rem;
font-size:.75rem" onclick="addCol('out')">+ output column</button></div>
<div style="overflow-x:auto"><table id="grid" class="ptable">
<thead><tr>{head}</tr></thead><tbody>{rows_html}</tbody></table></div>
<div class="inline" style="margin-top:1rem">
<button class="btn">Save policy (versions up, live immediately)</button>
</div></form>
<p class="note">Condition cells are ZEN expressions relative to the
column field — e.g. <span class="mono">&lt;= 30</span>,
<span class="mono">== "enterprise"</span>, empty means any. Output
cells are values or expressions — quote strings:
<span class="mono">"approved"</span>. Invalid cells are rejected on
save (zen would silently never match them).</p>
<div class="box" id="test" style="margin-top:1.6rem">
<h2 style="font-family:Georgia,serif">Test this policy</h2>
<p class="note">Evaluate facts against the SAVED ruleset — instant,
deterministic, no agent.</p>
<input type="hidden" id="pid" value="{pid}">
<div class="inline" style="margin:.6rem 0">
<button type="button" class="btn ghost" style="padding:.3rem .8rem;
font-size:.75rem" onclick="fillFacts()">skeleton from fields</button></div>
<label>facts (JSON)</label>
<textarea id="facts" class="mono" style="min-height:90px">{{}}</textarea>
<div class="inline" style="margin-top:.6rem">
<button type="button" class="btn" onclick="testPolicy()">Evaluate</button></div>
<pre id="testout" class="mono note" style="margin-top:.8rem;
white-space:pre-wrap"></pre></div>
<script>{POLICY_JS}</script>"""
    return page(request, f"Policy · <em>{_e(name)}</em>", body, flash=flash)


@app.post("/policy/save")
async def policy_save(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    pid = int(form.get("id") or 0)
    name = str(form.get("name", "")).strip()[:120]
    desc = str(form.get("description", "")).strip()[:400]
    jdm = str(form.get("jdm", "")).strip()
    enabled = bool(form.get("enabled"))
    if not (name and jdm and _jdm_valid(jdm)):
        return RedirectResponse(
            f"/policy/{pid}?flash=invalid+or+unparsable+policy",
            status_code=303)
    cur = await _q("SELECT version FROM rulesets WHERE id=%s",
                   (pid,), fetch="one")
    await _x(
        "UPDATE rulesets SET name=%s, description=%s, jdm=%s, "
        "enabled=%s, version=version+1 WHERE id=%s",
        (name, desc, jdm, enabled, pid))
    await _audit("policy-save",
                 f"#{pid} {name} -> v{(cur[0] + 1) if cur else '?'}"
                 + ("" if enabled else " [disabled]"))
    return RedirectResponse(f"/policy/{pid}?flash=policy+saved",
                            status_code=303)


@app.post("/policy/toggle")
async def policy_toggle(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    pid = int(form.get("id") or 0)
    row = await _q("SELECT name, enabled, kind FROM rulesets WHERE id=%s",
                   (pid,), fetch="one")
    if not row:
        return RedirectResponse("/policies?flash=not+found",
                                status_code=303)
    await _x("UPDATE rulesets SET enabled=NOT enabled WHERE id=%s", (pid,))
    await _audit("policy-toggle",
                 f"#{pid} {row[0]} -> {'off' if row[1] else 'on'}")
    return RedirectResponse("/policies", status_code=303)


@app.post("/policy/{rid}/test")
async def policy_test(request: Request, rid: int):
    if not _authed(request):
        return JSONResponse({"error": "auth"}, status_code=403)
    try:
        body = await request.json()
        facts = body.get("facts") or {}
        if not isinstance(facts, dict):
            raise ValueError
    except Exception:
        return JSONResponse({"error": "facts must be a JSON object"},
                            status_code=400)
    row = await _q("SELECT name, version, jdm, enabled FROM rulesets "
                   "WHERE id=%s", (rid,), fetch="one")
    if not row or not row[3]:
        return JSONResponse({"error": "policy not found or disabled"},
                            status_code=404)
    try:
        from zen import ZenEngine
        out = ZenEngine().create_decision(json.loads(row[2])).evaluate(
            facts)
        return {"policy": f"{row[0]} v{row[1]}",
                "result": out.get("result", {})
                if isinstance(out, dict) else {},
                "performance": out.get("performance", "")
                if isinstance(out, dict) else ""}
    except Exception as e:
        return JSONResponse({"error": f"evaluation failed: {e!r}"},
                            status_code=400)


# ── escalation topics + agent settings (prompt data, editable) ────────────

@app.get("/escalation/{eid}", response_class=HTMLResponse)
async def escalation_edit(request: Request, eid: int, flash: str = ""):
    if (r := _guard(request)):
        return r
    row = await _q(
        "SELECT id, topic, guidance, enabled FROM escalation_topics "
        "WHERE id=%s", (eid,), fetch="one")
    if not row:
        return RedirectResponse("/policies?flash=not+found",
                                status_code=303)
    _, topic, guidance, en = row
    body = f"""
<div class="inline" style="margin-bottom:1rem">
<a class="btn ghost" href="/policies">← policies</a>
<span class="tag {'ok' if en else 'bad'}">{'enabled' if en else
'disabled'}</span></div>
<form method="post" action="/escalation/save">
<input type="hidden" name="id" value="{eid}">
<label>topic name</label>
<input name="topic" value="{_e(topic)}" required>
<label>matches — the guidance the triage AI reads (one phrase, precise;
this text goes into the prompt verbatim)</label>
<textarea name="guidance" required
 style="min-height:110px">{_e(guidance)}</textarea>
<label class="inline" style="gap:.5rem;margin-top:.6rem">
<input type="checkbox" name="enabled" value="1"
{' checked' if en else ''} style="width:auto"> enabled</label>
<div class="inline" style="margin-top:.8rem">
<button class="btn">Save (live on the next question)</button></div></form>
<p class="note" style="margin-top:1rem">This is prompt data: write it
as the shape of question to escalate, not as instructions.</p>"""
    return page(request, f"Escalation · <em>{_e(topic)}</em>", body,
                flash=flash)


@app.post("/escalation/save")
async def escalation_save(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    eid = int(form.get("id") or 0)
    topic = str(form.get("topic", "")).strip()[:120]
    guidance = str(form.get("guidance", "")).strip()[:400]
    enabled = bool(form.get("enabled"))
    if not (topic and guidance):
        return RedirectResponse(
            f"/escalation/{eid}?flash=topic+and+guidance+required",
            status_code=303)
    await _x("UPDATE escalation_topics SET topic=%s, guidance=%s, "
             "enabled=%s WHERE id=%s", (topic, guidance, enabled, eid))
    await _audit("escalation-save", f"#{eid} {topic}")
    return RedirectResponse("/policies?flash=escalation+topic+saved",
                            status_code=303)


@app.post("/escalation/toggle")
async def escalation_toggle(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    eid = int(form.get("id") or 0)
    row = await _q("SELECT topic, enabled FROM escalation_topics "
                   "WHERE id=%s", (eid,), fetch="one")
    if not row:
        return RedirectResponse("/policies?flash=not+found",
                                status_code=303)
    await _x("UPDATE escalation_topics SET enabled=NOT enabled "
             "WHERE id=%s", (eid,))
    await _audit("escalation-toggle",
                 f"#{eid} {row[0]} -> {'off' if row[1] else 'on'}")
    return RedirectResponse("/policies", status_code=303)


# known settings + their valid ranges — the Gatekeeper reads these keys
_SETTING_RANGES = {
    "retrieval_top_k": (1, 50),
    "rerank_pick_max": (1, 5),
    "inbound_daily_cap": (1, 100),
}


@app.post("/settings/save")
async def settings_save(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    key = str(form.get("key", "")).strip()
    value = str(form.get("value", "")).strip()
    rng = _SETTING_RANGES.get(key)
    if not rng:
        return RedirectResponse("/policies?flash=unknown+setting",
                                status_code=303)
    try:
        v = int(value)
        if not (rng[0] <= v <= rng[1]):
            raise ValueError
    except ValueError:
        return RedirectResponse(
            f"/policies?flash={key}+must+be+{rng[0]}-{rng[1]}",
            status_code=303)
    await _x("INSERT INTO agent_settings (key, value) VALUES (%s, %s) "
             "ON CONFLICT (key) DO UPDATE SET value=%s, "
             "updated_at=now()", (key, value, value))
    await _audit("setting-save", f"{key}={value}")
    return RedirectResponse(f"/policies?flash={key}+saved",
                            status_code=303)


# ── portal clients: provisioning for the client task portal ──────────────

def _new_portal_key() -> str:
    import secrets as _secrets
    return "portal-" + _secrets.token_urlsafe(18)


@app.get("/clients", response_class=HTMLResponse)
async def clients(request: Request, flash: str = ""):
    if (r := _guard(request)):
        return r
    rows = await _q(
        "SELECT c.id, c.name, c.company, c.email, c.brand_name, c.enabled,"
        " (SELECT count(*) FROM portal_tasks t WHERE t.client_id=c.id)"
        " FROM portal_clients c ORDER BY c.id")
    trs = ""
    for cid, name, company, email, brand, en, ntasks in rows:
        state = ('<span class="tag ok">active</span>' if en
                 else '<span class="tag">paused</span>')
        act = (f'<form method="post" action="/clients/toggle" '
               f'class="inline"><input type="hidden" name="id" '
               f'value="{cid}"><button class="btn ghost" style="padding:'
               f'.2rem .7rem;font-size:.72rem">'
               f'{"pause" if en else "activate"}</button></form> '
               f'<form method="post" action="/clients/regen" class="inline"'
               f' onsubmit="return confirm(\'regenerate the access key? '
               f'the old one stops working immediately\')">'
               f'<input type="hidden" name="id" value="{cid}">'
               f'<button class="btn ghost" style="padding:.2rem .7rem;'
               f'font-size:.72rem">new key</button></form>')
        trs += (f"<tr><td>{_e(name)}</td><td class='note'>{_e(company)}"
                f"</td><td class='note'>{_e(email)}</td>"
                f"<td>{_e(brand or company or name)}</td><td>{state}</td>"
                f"<td class='mono note'>{ntasks}</td><td>{act}</td></tr>")

    body = f"""
<p class="note" style="margin-bottom:1.2rem">Clients of the task portal
(portal.datagatelodge.com): each signs in with an access key and sees
only their own tasks, policies and runs. The key is shown once when
created or regenerated.</p>
<form method="post" action="/clients/new" class="box">
<div class="grid2">
<div>
<p class="note">name</p>
<input type="text" name="name" required style="width:100%">
<p class="note" style="margin-top:.6rem">email (key delivery)</p>
<input type="email" name="email" required style="width:100%">
</div>
<div>
<p class="note">company</p>
<input type="text" name="company" required style="width:100%">
<p class="note" style="margin-top:.6rem">brand shown in their portal</p>
<input type="text" name="brand" style="width:100%"
 placeholder="defaults to company">
</div></div>
<div class="inline" style="margin-top:.8rem">
<button class="btn">Create client + key</button>
<label style="display:flex;gap:.4rem;align-items:center;margin:0">
<input type="checkbox" name="email_key" checked
 style="width:auto">email the key</label>
</div></form>
<table><tr><th>client</th><th>company</th><th>email</th><th>brand</th>
<th>state</th><th>tasks</th><th></th></tr>
{trs or '<tr><td colspan=7 class=note>No clients yet</td></tr>'}</table>"""
    return page(request, "Portal clients", body, flash=flash)


@app.post("/clients/new")
async def client_new(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    name = str(form.get("name", "")).strip()[:120]
    company = str(form.get("company", "")).strip()[:120]
    email = str(form.get("email", "")).strip().lower()[:200]
    brand = str(form.get("brand", "")).strip()[:120] or company or name
    if not (name and company and email and "@" in email):
        return RedirectResponse("/clients?flash=name,+company+and+a+valid"
                                "+email+are+required", status_code=303)
    key = _new_portal_key()
    await _x(
        "INSERT INTO portal_clients (name, company, email, access_key,"
        " brand_name) VALUES (%s, %s, %s, %s, %s)",
        (name, company, email, key, brand))
    await _audit("client-new", f"{name} <{email}>")
    mailed = ""
    if form.get("email_key"):
        ok, _ = await _send_mail(
            email, "Your Gatelodge Task Portal access",
            f"Hello {name},\n\nYour task portal is ready.\n\n"
            f"Sign in at: https://portal.datagatelodge.com\n"
            f"Your access key: {key}\n\nThe key is personal — keep it to "
            f"your team. You can define tasks, decision policies and see "
            f"every governed run from your portal.",
            kind="portal-key")
        mailed = "+key+emailed" if ok else "+(email+failed+—+key+below)"
    return RedirectResponse(f"/clients?flash=client+created+{mailed}"
                            f"+—+key:+{key}", status_code=303)


@app.post("/clients/toggle")
async def client_toggle(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    cid = str(form.get("id", "")).strip()
    await _x("UPDATE portal_clients SET enabled = NOT enabled WHERE id=%s",
             (cid,))
    await _audit("client-toggle", cid)
    return RedirectResponse("/clients?flash=state+updated", status_code=303)


@app.post("/clients/regen")
async def client_regen(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    cid = str(form.get("id", "")).strip()
    key = _new_portal_key()
    row = await _q("SELECT email FROM portal_clients WHERE id=%s",
                   (cid,), fetch="one")
    updated = await _q(
        "UPDATE portal_clients SET access_key=%s, enabled=TRUE "
        "WHERE id=%s RETURNING id", (key, cid), fetch="one")
    if not updated:
        return RedirectResponse("/clients?flash=client+not+found",
                                status_code=303)
    await _audit("client-regen", cid)
    ok, _ = await _send_mail(
        row[0], "Your new Gatelodge Task Portal key",
        f"Your access key was regenerated.\n\nNew key: {key}\n\n"
        f"Sign in at: https://portal.datagatelodge.com — the previous "
        f"key no longer works.", kind="portal-key") if row else (False, "")
    return RedirectResponse(f"/clients?flash=new+key+{key}", status_code=303)


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
        "SELECT id, from_addr, subject, body, topic, doc, rule, "
        "created_at FROM inbound_emails WHERE status='review' "
        "ORDER BY created_at DESC LIMIT 50")
    recent = await _q(
        "SELECT id, from_addr, subject, status, doc, rule, created_at "
        "FROM inbound_emails WHERE status != 'review' "
        "ORDER BY created_at DESC LIMIT 30")

    cards = ""
    for i, (mid, frm, subject, body, topic, doc, rule, at) in \
            enumerate(pending):
        rule_tag = (f" <span class='tag' title='the triage rule that "
                    f"fired — see the Policies screen'>"
                    f"{_e(rule)}</span>" if rule else "")
        cards += f"""
<div class="box">
<p><b>{_e(subject or '(no subject)')}</b> &nbsp;
<span class="tag warn">needs review</span>{rule_tag}
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
        f"<td><span class='tag'>{_e(rl or '-')}</span></td>"
        f"<td class='note'>{str(at)[:16]}</td>"
        f"<td><a class='btn ghost' style='padding:.15rem .5rem;"
        f"font-size:.7rem' href='/inbox/view?id={_e(mid)}'>view</a></td></tr>"
        for mid, frm, subject, st, doc, rl, at in recent)

    body = f"""
<p class="note" style="margin-bottom:1.2rem">Inbound support mail. The
agent auto-replies to confident questions; everything else lands here.
The rule tag is the deterministic triage rule that queued it.</p>
{cards}
<h1 style="font-size:1.2rem">Recently handled</h1>
<table><tr><th>from</th><th>subject</th><th>status</th><th>doc</th>
<th>rule</th><th>at</th></tr>{rows or '<tr><td colspan=6 class=note>none yet</td></tr>'}</table>"""
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

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
import os
import re
import time

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
       ("/reviews", "Reviews"), ("/leads", "Leads"),
       ("/licenses", "Licenses"), ("/kb", "Knowledge"),
       ("/emails", "Emails"), ("/gateway", "Gateway")]

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
        await _x("INSERT INTO email_log (kind, to_addr, serial, ok, detail) "
                 "VALUES ('admin-reply', %s, %s, %s, %s)",
                 (to, sid[:12], int(sent), "resent ok" if sent else "failed"))

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

    new_form = """
<div class="box" style="margin-bottom:1.4rem">
<p class="note" style="letter-spacing:.14em;text-transform:uppercase;
color:var(--brass);margin-bottom:.7rem">New document</p>
<form method="post" action="/kb/new" class="grid2">
<div>
<p class="note">name <span class="mono">(lowercase, e.g. channel-faq)</span></p>
<input type="text" name="name" placeholder="doc-name" required
 style="width:100%">
<p class="note" style="margin-top:.7rem">title
 <span class="mono">(what the triage model matches on)</span></p>
<input type="text" name="title" placeholder="Channel partners — FAQ"
 required style="width:100%">
<p class="note" style="margin-top:.7rem">description
 <span class="mono">(when to pick this doc)</span></p>
<input type="text" name="description"
 placeholder="Questions from inference partners about co-branded trials"
 required style="width:100%">
</div>
<div>
<p class="note">content <span class="mono">(markdown)</span></p>
<textarea name="content" style="min-height:200px"
 placeholder="## Question visitors ask…&#10;&#10;The answer, in plain terms."
 ></textarea>
</div></div>
<div class="inline" style="margin-top:.4rem">
<button class="btn">Create document (live)</button>
<span class="note">custom docs join the agent's catalog on the next
question — no redeploy</span></div>
</form></div>"""

    body = f"""
<p class="note" style="margin-bottom:1.2rem">Edits and new documents
apply LIVE — the Gatekeeper merges these over its built-in docs on the
next question. Title + description are what its triage model routes on,
so write them as the question a visitor would ask.</p>
{new_form}
<table><tr><th>doc</th><th>state</th><th></th></tr>{rows}</table>"""
    return page(request, "Knowledge base", body, flash=flash)


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


@app.get("/emails", response_class=HTMLResponse)
async def emails(request: Request):
    if (r := _guard(request)):
        return r
    rows = await _q("SELECT kind, to_addr, serial, ok, detail, created_at "
                    "FROM email_log ORDER BY id DESC LIMIT 150")
    body = f"""
<table><tr><th>kind</th><th>to</th><th>serial/sess</th><th>state</th>
<th>detail</th><th>when</th></tr>
{''.join(f'<tr><td><span class=tag>{_e(k)}</span></td><td>{_e(t)}</td><td class=pill>{_e(s) or "—"}</td><td>{"<span class=tag ok>ok</span>" if o else "<span class=tag bad>fail</span>"}</td><td class=note style=max-width:280px>{_e(d)[:120]}</td><td class=note>{_e(c)}</td></tr>' for k, t, s, o, d, c in rows) or '<tr><td colspan=6 class=note>no emails yet</td></tr>'}
</table>"""
    return page(request, "Email log", body)


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

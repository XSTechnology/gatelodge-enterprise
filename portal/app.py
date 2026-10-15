"""Gatelodge Task Portal — the client-facing task product.

A standalone service, fully separate from the Lodge Office admin task
engine. Clients log in with an access key, launch tasks from templates
(or write their own), and watch the agent work — plan, execute, review,
report. Every LLM call routes through the Gatelodge gateway.

Tables are portal_* namespaced: nothing is shared with the admin's
tasks/task_runs. Client-safe tools only (no database access, no send).

Env:
  DATABASE_URL   — shared Postgres (portal_* tables only)
  GATEWAY_URL / GATEWAY_KEY — the gateway (dogfood: budgets + audit)
  PORT           — default 8106
  RUN_TASKS=0    — disable the background runner (tests)
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import time

import httpx
import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse

DATABASE_URL = os.environ.get("DATABASE_URL", "")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "").rstrip("/")
GATEWAY_KEY = os.environ.get("GATEWAY_KEY", "")
MODEL = os.environ.get("PORTAL_MODEL", "triage")
PORT = int(os.environ.get("PORT", "8106"))
MAX_STEPS = 8

app = FastAPI(title="Gatelodge Task Portal", docs_url=None, redoc_url=None)

COOKIE = "portal_session"


def _token(key: str) -> str:
    return hmac.new(key.encode(), b"portal-v1",
                    hashlib.sha256).hexdigest()


# ── schema (portal_* only) ────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS portal_clients (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  company TEXT DEFAULT '',
  email TEXT DEFAULT '',
  access_key TEXT UNIQUE NOT NULL,
  brand_name TEXT DEFAULT '',
  enabled BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS portal_templates (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT DEFAULT '',
  icon TEXT DEFAULT '◆',
  goal_template TEXT NOT NULL,
  criteria_template TEXT DEFAULT '',
  parameters TEXT DEFAULT '[]',
  schedule TEXT DEFAULT 'manual',
  enabled BOOLEAN DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS portal_tasks (
  id BIGSERIAL PRIMARY KEY,
  client_id BIGINT NOT NULL,
  name TEXT NOT NULL,
  goal TEXT NOT NULL DEFAULT '',
  success_criteria TEXT DEFAULT '',
  inputs TEXT DEFAULT '',
  schedule TEXT DEFAULT 'manual',
  enabled BOOLEAN DEFAULT TRUE,
  next_run_at TIMESTAMPTZ,
  last_run_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS portal_runs (
  id BIGSERIAL PRIMARY KEY,
  task_id BIGINT NOT NULL,
  status TEXT DEFAULT 'running',
  classification TEXT DEFAULT '',
  summary TEXT DEFAULT '',
  report TEXT DEFAULT '',
  started_at TIMESTAMPTZ DEFAULT now(),
  finished_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS portal_steps (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  seq INT DEFAULT 0,
  tool TEXT DEFAULT '',
  input TEXT DEFAULT '',
  output TEXT DEFAULT '',
  status TEXT DEFAULT 'ok',
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS portal_rulesets (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT DEFAULT '',
  version INT DEFAULT 1,
  jdm TEXT NOT NULL,
  enabled BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE portal_templates ADD COLUMN IF NOT EXISTS ruleset_id BIGINT;
ALTER TABLE portal_tasks ADD COLUMN IF NOT EXISTS ruleset_id BIGINT;
ALTER TABLE portal_runs ADD COLUMN IF NOT EXISTS facts TEXT DEFAULT '';
ALTER TABLE portal_runs ADD COLUMN IF NOT EXISTS decision TEXT DEFAULT '';
"""

_schema_ok = False


def _db():
    return psycopg.connect(DATABASE_URL)


def _init_schema() -> None:
    with _db() as c:
        c.execute(_SCHEMA)


async def _run(fn):
    return await asyncio.to_thread(fn)


@app.on_event("startup")
async def init_db() -> None:
    global _schema_ok
    if not DATABASE_URL:
        print("FATAL: DATABASE_URL unset")
        return
    try:
        await _run(_init_schema)
        _schema_ok = True
    except Exception as e:
        print(f"schema init deferred ({e!r})")
    if os.environ.get("RUN_TASKS") == "0":
        return
    asyncio.create_task(_runner_loop())


# ── auth ──────────────────────────────────────────────────────────────────


def _authed(request: Request) -> dict | None:
    tok = request.cookies.get(COOKIE, "")
    if not tok:
        return None
    try:
        with _db() as c:
            row = c.execute(
                "SELECT id, name, company, brand_name, access_key "
                "FROM portal_clients WHERE enabled").fetchall()
    except Exception:
        return None
    for cid, name, company, brand, key in row:
        if hmac.compare_digest(_token(key), tok):
            return {"id": cid, "name": name, "company": company,
                    "brand": brand or company or name}
    return None


def _guard(request: Request):
    if not _authed(request):
        return RedirectResponse("/login", status_code=303)
    return None


# ── client-safe tools ─────────────────────────────────────────────────────

TOOLS: dict[str, dict] = {}


def tool(name: str, description: str):
    def deco(fn):
        TOOLS[name] = {"description": description, "fn": fn}
        return fn
    return deco


@tool("web_fetch",
      "Fetch a URL over HTTPS and return the HTTP status plus a text "
      "excerpt. Input: the full URL.")
async def t_web_fetch(url: str) -> str:
    url = (url or "").strip()
    if not url.startswith(("http://", "https://")):
        return "ERROR: provide a full http(s) URL"
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
            r = await c.get(url, headers={"user-agent": "gatelodge-portal"})
        text = " ".join(r.text.split())[:2000]
        return f"HTTP {r.status_code}\n{text}"
    except Exception as e:
        return f"ERROR: {e!r}"


@tool("kb_search",
      "Search the Gatelodge public knowledge base for product "
      "documentation. Input: a question or keywords.")
async def t_kb_search(query: str) -> str:
    try:
        r = await _gw_json(
            "You are a documentation search. Reply with the 2-3 most "
            "relevant passages from the Gatelodge docs context below. "
            "If nothing matches, say so.\n\n"
            "DOCS INDEX: pricing community $99 one-time, enterprise "
            "$39/user/mo; 10 backend types (embedded GGUF, llama-server, "
            "RunPod, OpenAI, Anthropic, Gemini, Responses, Azure, Vertex, "
            "Bedrock); governance (RBAC, audit chain, budgets, keys); "
            "self-hosted single binary; 30-day trial; MCP gateway.",
            [{"role": "user", "content": query or ""}])
        return r[:2000]
    except Exception as e:
        return f"ERROR: {e!r}"


@tool("email_draft",
      "Record an email DRAFT in the task report (never sends — you "
      "review and send from the portal). Input JSON: "
      '{"to": "...", "subject": "...", "body": "..."}.')
async def t_email_draft(spec: str) -> str:
    try:
        d = json.loads(spec)
        to = str(d.get("to", "")).strip().lower()
        subject = str(d.get("subject", "")).strip()
        body = str(d.get("body", "")).strip()
    except Exception:
        return "ERROR: input must be JSON with to/subject/body"
    if not to or "@" not in to or not subject or not body:
        return "ERROR: to, subject and body required"
    return ("DRAFT RECORDED (review before sending):\n"
            f"To: {to}\nSubject: {subject}\n\n{body[:4000]}")


@tool("report",
      "Record the final report for this task (markdown). Use when "
      "complete. Input: the report text.")
async def t_report(text: str) -> str:
    return f"REPORT RECORDED:\n{text[:8000]}"


@tool("submit_facts",
      "Submit the structured FACTS you extracted as ONE JSON object "
      "(values as proper JSON types — numbers unquoted, categories "
      "quoted). A deterministic decision policy will be evaluated "
      "against them; do NOT decide yourself. Input: the JSON object.")
async def t_submit_facts(spec: str) -> str:
    try:
        d = json.loads(spec)
        if not isinstance(d, dict) or not d:
            raise ValueError("not an object")
    except Exception:
        return ("ERROR: input must be a single JSON object of facts, "
                'e.g. {"days_since_purchase": 12, "reason": "defective"}')
    _RUN_STATE["facts"] = d
    return "FACTS RECORDED: " + json.dumps(d, sort_keys=True)


# ── decision policies: deterministic, versioned, auditable ───────────────
# The agent EXTRACTS facts; the rules engine DECIDES. The decision never
# comes from the LLM — that is the whole point of the hybrid pattern.

_RUN_STATE: dict = {}   # facts for the run in flight (runner is single-task)


def _evaluate_ruleset(jdm_json: str, facts: dict) -> dict:
    """Evaluate a JDM decision graph against extracted facts (µs-scale)."""
    from zen import ZenEngine  # deferred: not needed for UI-only paths
    graph = json.loads(jdm_json)
    decision = ZenEngine().create_decision(graph)
    return decision.evaluate(facts)


def _missing_fields(jdm_json: str, facts: dict) -> list[str]:
    """Input columns the ruleset expects but the agent did not supply."""
    try:
        g = json.loads(jdm_json)
        need = [i.get("field", "") for n in g.get("nodes", [])
                if n.get("type") == "decisionTableNode"
                for i in n.get("content", {}).get("inputs", [])]
        return [f for f in need if f and f not in facts]
    except Exception:
        return []


def _apply_policy(run_id: int, ruleset_id: int, report: str,
                  transcript: list[str]) -> tuple[str, str]:
    """Evaluate the task's ruleset against submitted facts. Returns
    (report, decision_json) — decision appended to the report verbatim."""
    facts = _RUN_STATE.get("facts") or {}
    if not facts:
        return report, ""
    try:
        with _db() as c:
            rs = c.execute(
                "SELECT name, version, jdm FROM portal_rulesets "
                "WHERE id=%s AND enabled", (ruleset_id,)).fetchone()
        if not rs:
            return report, ""
        rs_name, rs_ver, jdm = rs
        r = _evaluate_ruleset(jdm, facts)
        res = r.get("result", {}) if isinstance(r, dict) else {}
        perf = r.get("performance", "") if isinstance(r, dict) else ""
        decision_json = json.dumps(res, sort_keys=True)
        missing = _missing_fields(jdm, facts)
        note = (f"\nMISSING FIELDS (decision may be unreliable): "
                f"{', '.join(missing)}") if missing else ""
        detail = (f"DECISION: {decision_json}\n"
                  f"RULESET: {rs_name} v{rs_ver} · evaluated in {perf}"
                  f"{note}")
        with _db() as c:
            c.execute("INSERT INTO portal_steps (run_id, seq, tool, input,"
                      " output, status) VALUES (%s,%s,%s,%s,%s,%s)",
                      (run_id, 900, "policy_check",
                       json.dumps(facts, sort_keys=True)[:4000],
                       detail[:6000], "ok"))
        report += (f"\n\n---\n**Policy decision** — `{rs_name}` v{rs_ver}"
                   f" (deterministic rules engine, not the agent's "
                   f"judgment):\n\n`{decision_json}`")
        transcript.append(f"policy_check (ok): {rs_name} v{rs_ver} → "
                          f"{decision_json}")
        return report, decision_json
    except Exception as e:
        with _db() as c:
            c.execute("INSERT INTO portal_steps (run_id, seq, tool, input,"
                      " output, status) VALUES (%s,%s,%s,%s,%s,%s)",
                      (run_id, 900, "policy_check",
                       json.dumps(facts, sort_keys=True)[:4000],
                       f"ERROR: {e!r}", "error"))
        return report, ""


async def _gw_json(system: str, messages: list[dict]) -> str:
    async with httpx.AsyncClient(timeout=90) as c:
        r = await c.post(
            f"{GATEWAY_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
            json={"model": MODEL,
                  "messages": [{"role": "system", "content": system}]
                  + messages,
                  "max_tokens": 1200, "temperature": 0.1})
        data = r.json()
        if "choices" not in data:
            raise RuntimeError(f"gateway {r.status_code}: {str(data)[:300]}")
        return (data["choices"][0]["message"]["content"] or "").strip()


def _tools_spec() -> list[dict]:
    return [{"type": "function", "function": {
        "name": n, "description": d["description"][:600],
        "parameters": {"type": "object",
                       "properties": {"input": {
                           "type": "string",
                           "description": "text input"}},
                       "required": ["input"]}}}
        for n, d in sorted(TOOLS.items())]


# ── the engine (client tasks: plan → execute → review) ────────────────────

TASK_SYSTEM = """You are the Gatelodge task agent. Complete the user's \
task by calling the provided function capabilities, one at a time.

When complete, reply as plain text content (NOT a tool call) with ONLY:
{{"done": true, "report": "<final report in markdown>"}}

Rules: at most {max_steps} calls; chain results; use email_draft for any \
email (never assume sending); if something is impossible, say so in the \
report."""

REVIEW_SYSTEM = """You review a completed agent task. Given the GOAL, \
SUCCESS CRITERIA and TRANSCRIPT, classify the outcome.

Reply ONLY JSON:
{{"classification": "achieved" | "partial" | "failed" | "review", \
"summary": "<2-3 sentences for the client>"}"""


async def _plan_step(messages: list[dict]) -> dict:
    async with httpx.AsyncClient(timeout=90) as c:
        r = await c.post(
            f"{GATEWAY_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
            json={"model": MODEL,
                  "messages": [{"role": "system", "content":
                                TASK_SYSTEM.format(max_steps=MAX_STEPS)}]
                  + messages,
                  "tools": _tools_spec(), "tool_choice": "auto",
                  "max_tokens": 1200, "temperature": 0.1})
        data = r.json()
        if "choices" not in data:
            raise RuntimeError(f"gateway {r.status_code}: {str(data)[:300]}")
    msg = data["choices"][0]["message"]
    for tc in (msg.get("tool_calls") or []):
        fn = tc.get("function", {})
        try:
            args = json.loads(fn.get("arguments") or "{}")
        except Exception:
            args = {}
        inp = args.get("input") or args.get("url") or ""
        clean = {"role": "assistant", "content": msg.get("content") or "",
                 "tool_calls": [{"id": tc.get("id", ""),
                                 "type": "function",
                                 "function": {
                                     "name": fn.get("name", ""),
                                     "arguments": fn.get("arguments")
                                     or "{}"}}]}
        return {"tool": fn.get("name", ""), "input": str(inp),
                "call_id": tc.get("id", ""), "assistant": clean}
    content = (msg.get("content") or "").strip()
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        raise RuntimeError(f"unparseable: {content[:200]}")
    d = json.loads(m.group(0))
    if d.get("done"):
        return {"done": True, "report": str(d.get("report", "")).strip(),
                "assistant": {"role": "assistant", "content": content}}
    return {"tool": str(d.get("tool", "")).strip(),
            "input": str(d.get("input", "")).strip(),
            "call_id": "", "assistant": {"role": "assistant",
                                         "content": content}}


def _run_task(task_id: int) -> None:
    """Synchronous core (runs in a thread); the portal runner calls it."""
    with _db() as c:
        row = c.execute(
            "SELECT name, goal, success_criteria, inputs, ruleset_id "
            "FROM portal_tasks WHERE id=%s", (task_id,)).fetchone()
    if not row:
        return
    name, goal, criteria, inputs, ruleset_id = row
    with _db() as c:
        run_id = c.execute(
            "INSERT INTO portal_runs (task_id) VALUES (%s) RETURNING id",
            (task_id,)).fetchone()[0]
    _RUN_STATE.clear()
    _RUN_STATE["run_id"] = run_id
    print(f"[portal] task '{name}' run {run_id}")

    user = f"TASK: {goal}"
    if criteria:
        user += f"\n\nSUCCESS CRITERIA: {criteria}"
    if inputs:
        user += f"\n\nINPUTS: {inputs}"
    messages: list[dict] = [{"role": "user", "content": user}]
    transcript: list[str] = []
    report = ""

    for step_no in range(MAX_STEPS):
        try:
            d = asyncio.run(_plan_step(messages))
        except Exception as e:
            print(f"[portal] plan error: {e!r}")
            report = f"Agent error: {e!r}"
            break
        if d.get("done"):
            report = d["report"]
            break
        messages.append(d["assistant"])
        tn, ti, cid = d["tool"], d["input"], d.get("call_id", "")
        if tn not in TOOLS:
            output = f"ERROR: no capability '{tn}'"
            status = "blocked"
        else:
            try:
                output = str(asyncio.run(TOOLS[tn]["fn"](ti)))[:6000]
                status = "ok"
            except Exception as e:
                output = f"ERROR: {e!r}"
                status = "error"
        if tn == "report":
            report = ti
        transcript.append(f"{tn} ({status}): {ti[:150]} → {output[:400]}")
        with _db() as c:
            c.execute("INSERT INTO portal_steps (run_id, seq, tool, input,"
                      " output, status) VALUES (%s,%s,%s,%s,%s,%s)",
                      (run_id, step_no, tn, ti[:4000], output[:6000], status))
        if cid:
            messages.append({"role": "tool", "tool_call_id": cid,
                             "content": output[:4000]})
        else:
            messages.append({"role": "user",
                             "content": f"TOOL OUTPUT ({tn}):\n{output}"})

    if not report:
        report = "(no report produced)"

    decision_json = ""
    if ruleset_id:
        report, decision_json = _apply_policy(
            run_id, ruleset_id, report, transcript)

    classification, summary = "review", ""
    try:
        rv = asyncio.run(_gw_json(
            REVIEW_SYSTEM,
            [{"role": "user",
              "content": f"GOAL: {goal}\n\nCRITERIA: {criteria or '(none)'}"
                         f"\n\nTRANSCRIPT:\n" + "\n\n".join(transcript)
                         + f"\n\nREPORT:\n{report}"}]))
        m = re.search(r"\{.*\}", rv, re.S)
        if m:
            rd = json.loads(m.group(0))
            classification = str(rd.get("classification", "review"))
            summary = str(rd.get("summary", ""))[:1000]
    except Exception as e:
        summary = f"Review error: {e!r}"

    submitted = _RUN_STATE.get("facts") or {}
    with _db() as c:
        c.execute("UPDATE portal_runs SET status=%s, classification=%s, "
                  "summary=%s, report=%s, facts=%s, decision=%s, "
                  "finished_at=now() WHERE id=%s",
                  (classification, classification, summary, report,
                   json.dumps(submitted, sort_keys=True)
                   if submitted else "",
                   decision_json, run_id))
        interval = {"hourly": "1 hour", "daily": "1 day",
                    "weekly": "7 days"}.get(
                    _schedule(task_id), None)
        c.execute("UPDATE portal_tasks SET last_run_at=now(), next_run_at="
                  + ("now() + interval '" + interval + "'"
                     if interval else "NULL") + " WHERE id=%s", (task_id,))
    print(f"[portal] run {run_id}: {classification}")


def _schedule(task_id: int) -> str:
    with _db() as c:
        row = c.execute("SELECT schedule FROM portal_tasks WHERE id=%s",
                        (task_id,)).fetchone()
    return row[0] if row else "manual"


async def _runner_loop() -> None:
    while True:
        try:

            def due():
                with _db() as c:
                    return [r[0] for r in c.execute(
                        "SELECT id FROM portal_tasks WHERE enabled AND "
                        "next_run_at IS NOT NULL AND next_run_at <= now()"
                        " ORDER BY id").fetchall()]
            for tid in await _run(due):
                try:
                    await _run(lambda t=tid: _run_task(t))
                except Exception as e:
                    print(f"[portal] task {tid} crashed: {e!r}")
        except Exception as e:
            print(f"[portal] loop error: {e!r}")
        await asyncio.sleep(30)


# ── UI (gatehouse-styled, branded) ────────────────────────────────────────

CSS = """
:root { --ink:#0e1411; --ink2:#121a15; --surface:#1a241d; --line:#2c3a30;
  --brass:#d4af37; --brassb:#e8ca6a; --paper:#ece7dd; --muted:#9aa79d;
  --faint:#6d7a70; --ok:#57b481; --danger:#c4705f; }
* { margin:0; padding:0; box-sizing:border-box; }
body { background:radial-gradient(1100px 500px at 85% -5%,
  rgba(212,175,55,.06), transparent 60%), var(--ink); color:var(--paper);
  font-family:'Segoe UI',system-ui,sans-serif; font-size:15.5px;
  line-height:1.6; }
.mono { font-family:Consolas,monospace; }
a { color:var(--brassb); text-decoration:none; }
header { display:flex; align-items:center; gap:1.2rem; padding:.9rem 1.6rem;
  border-bottom:2px solid var(--line); background:var(--ink2);
  position:sticky; top:0; z-index:10; }
.brand { font-family:Georgia,serif; font-size:1.1rem; color:var(--brass);
  font-weight:600; }
.brand-sub { font-size:.62rem; letter-spacing:.18em; text-transform:uppercase;
  color:var(--faint); }
nav { margin-left:auto; display:flex; gap:1.1rem; }
nav a { color:var(--muted); font-size:.8rem; text-transform:uppercase;
  letter-spacing:.08em; }
nav a:hover { color:var(--brassb); }
main { max-width:1060px; margin:0 auto; padding:2rem 1.6rem 4rem; }
h1 { font-family:Georgia,serif; font-weight:500; font-size:1.7rem;
  margin-bottom:1.2rem; }
h2 { font-family:Georgia,serif; font-weight:500; font-size:1.2rem;
  margin:1.6rem 0 .8rem; }
.note { color:var(--faint); font-size:.78rem; }
.tag { display:inline-block; font-size:.62rem; letter-spacing:.1em;
  padding:.15rem .5rem; border:1px solid var(--line); border-radius:6px;
  color:var(--muted); text-transform:uppercase; }
.tag.ok { color:var(--ok); border-color:var(--ok); }
.tag.warn { color:var(--brassb); border-color:var(--brass); }
.tag.bad { color:var(--danger); border-color:var(--danger); }
table { width:100%; border-collapse:collapse; font-size:.86rem;
  margin-bottom:1.4rem; }
th { text-align:left; font-size:.66rem; letter-spacing:.14em;
  text-transform:uppercase; color:var(--faint); padding:.5rem .6rem;
  border-bottom:1px solid var(--line); }
td { padding:.55rem .6rem; border-bottom:1px solid var(--line); }
.btn { display:inline-block; background:var(--brass); color:var(--ink);
  border:none; border-radius:8px; padding:.55rem 1.2rem; font-weight:600;
  font-size:.88rem; cursor:pointer; font-family:inherit; }
.btn:hover { background:var(--brassb); }
.btn.ghost { background:transparent; color:var(--paper);
  border:1px solid var(--line); }
.cards { display:grid; grid-template-columns:repeat(auto-fill,
  minmax(220px,1fr)); gap:1rem; margin-bottom:1.6rem; }
.card { background:var(--surface); border:1px solid var(--line);
  border-radius:12px; padding:1.1rem 1.2rem; }
.card .icon { font-size:1.4rem; color:var(--brass); margin-bottom:.5rem; }
.card h3 { font-family:Georgia,serif; font-size:1.05rem;
  margin-bottom:.4rem; }
.card p { color:var(--muted); font-size:.82rem; }
.view-body { background:var(--ink2); border:1px solid var(--line);
  border-radius:10px; padding:1.1rem 1.3rem; white-space:pre-wrap;
  font-size:.86rem; line-height:1.6; }
input, textarea, select { background:var(--ink2); color:var(--paper);
  border:1px solid var(--line); border-radius:8px; padding:.55rem .8rem;
  font-size:.9rem; font-family:inherit; width:100%; }
input:focus, textarea:focus { border-color:var(--brass); }
textarea { min-height:100px; font-family:Consolas,monospace; }
label { display:block; font-size:.7rem; letter-spacing:.12em;
  text-transform:uppercase; color:var(--brass); margin:.8rem 0 .3rem; }
.box { background:var(--surface); border:1px solid var(--line);
  border-radius:12px; padding:1.2rem 1.3rem; margin-bottom:1rem; }
.inline { display:flex; gap:.7rem; align-items:center; flex-wrap:wrap; }
.flash { background:rgba(87,180,129,.12); border:1px solid var(--ok);
  border-radius:8px; padding:.6rem .9rem; margin-bottom:1.2rem;
  font-size:.85rem; }
.login-wrap { max-width:400px; margin:5rem auto; }
"""

SHELL = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} — Gatelodge Task Portal</title>
<meta name="robots" content="noindex,nofollow">
<style>{css}</style></head><body>
<header><div>
<div class="brand">{brand}</div>
<div class="brand-sub">powered by Gatelodge</div></div>
<nav><a href="/">My tasks</a><a href="/templates">Templates</a>
<a href="/logout">Sign out</a></nav></header>
<main>{body}</main></body></html>"""


def _e(s) -> str:
    import html
    return html.escape(str(s), quote=True)


def _page(request: Request, client: dict, title: str, body: str,
          flash: str = "") -> HTMLResponse:
    fl = f'<div class="flash">{_e(flash)}</div>' if flash else ""
    html = SHELL.format(brand=_e(client["brand"]), title=_e(title),
                        css=CSS, body=fl + body)
    return HTMLResponse(html)


@app.get("/login", response_class=HTMLResponse)
async def login(request: Request, flash: str = ""):
    body = f"""
<div class="login-wrap box">
<p class="brand" style="margin-bottom:.3rem">Gatelodge Task Portal</p>
<p class="note" style="margin-bottom:1rem">Sign in with your access key</p>
<form method="post" action="/login">
<input type="password" name="key" placeholder="your access key" autofocus>
<div style="margin-top:.8rem"><button class="btn" style="width:100%">
Enter</button></div></form></div>"""
    fl = f'<div class="flash">{_e(flash)}</div>' if flash else ""
    return HTMLResponse(
        f'<!doctype html><html><head><meta charset="utf-8">'
        f'<title>Sign in — Task Portal</title><style>{CSS}</style></head>'
        f'<body><main style="max-width:400px">{fl}{body}</main></body></html>')


@app.post("/login")
async def do_login(request: Request):
    form = await request.form()
    key = str(form.get("key", "")).strip()
    try:
        with _db() as c:
            row = c.execute(
                "SELECT access_key FROM portal_clients WHERE access_key=%s"
                " AND enabled", (key,)).fetchone()
    except Exception:
        row = None
    if not row:
        return RedirectResponse("/login?flash=invalid+key", status_code=303)
    r = RedirectResponse("/", status_code=303)
    r.set_cookie(COOKIE, _token(key), httponly=True, samesite="lax")
    return r


@app.get("/logout")
async def logout():
    r = RedirectResponse("/login", status_code=303)
    r.delete_cookie(COOKIE)
    return r


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, flash: str = ""):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)

    def q():
        with _db() as c:
            tasks = c.execute(
                "SELECT t.id, t.name, t.schedule, t.enabled, t.last_run_at,"
                " (SELECT classification FROM portal_runs r "
                " WHERE r.task_id=t.id ORDER BY r.id DESC LIMIT 1)"
                " FROM portal_tasks t WHERE t.client_id=%s ORDER BY t.id",
                (client["id"],)).fetchall()
            templates = c.execute(
                "SELECT id, name, description, icon FROM portal_templates"
                " WHERE enabled ORDER BY id").fetchall()
        return tasks, templates
    tasks, templates = await _run(q)

    tagmap = {"achieved": "ok", "partial": "warn", "failed": "bad",
              "review": "", "running": "warn"}
    trs = "".join(
        f"<tr><td>{_e(n)}</td><td><span class='tag'>{_e(s)}</span></td>"
        f"<td>{'<span class=tag ok>on</span>' if en else '<span class=tag>paused</span>'}</td>"
        f"<td>{'<span class=tag ' + tagmap.get(lc or '', '') + '>' + _e(lc or '—') + '</span>' if lc else '—'}</td>"
        f"<td class='note'>{str(la)[:16] if la else '—'}</td>"
        f"<td><a href='/task/{i}'>view</a></td></tr>"
        for i, n, s, en, la, lc in tasks)

    cards = "".join(
        f'<div class="card"><div class="icon">{_e(ic)}</div>'
        f'<h3>{_e(n)}</h3><p>{_e(d)}</p>'
        f'<p style="margin-top:.6rem"><a class="btn ghost" '
        f'style="padding:.35rem .8rem;font-size:.78rem" '
        f'href="/launch/{tid}">Use this →</a></p></div>'
        for tid, n, d, ic in templates)

    body = f"""
<h1>Your tasks</h1>
<table><tr><th>task</th><th>schedule</th><th>state</th><th>last outcome</th>
<th>last run</th><th></th></tr>
{trs or '<tr><td colspan=6 class=note>No tasks yet — start from a '
'template below</td></tr>'}</table>
<h2>Start from a template</h2>
<div class="cards">{cards or '<p class=note>no templates available</p>'}</div>
<p class="note">Tasks run on the Gatelodge agent platform — every call
governed, budgeted, and audited. Results are reviewed by a separate
quality pass before they reach you.</p>"""
    return _page(request, client, "My tasks", body, flash)


@app.get("/templates", response_class=HTMLResponse)
async def templates(request: Request):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)

    def q():
        with _db() as c:
            return c.execute(
                "SELECT id, name, description, icon, schedule "
                "FROM portal_templates WHERE enabled ORDER BY id").fetchall()
    rows = await _run(q)

    cards = "".join(
        f'<div class="card"><div class="icon">{_e(ic)}</div>'
        f'<p class="tag" style="margin:.4rem 0">{_e(s)}</p>'
        f'<h3>{_e(n)}</h3><p>{_e(d)}</p>'
        f'<p style="margin-top:.8rem"><a class="btn" '
        f'style="padding:.4rem .9rem;font-size:.8rem" '
        f'href="/launch/{tid}">Use this template →</a></p></div>'
        for tid, n, d, ic, s in rows)

    body = f"""
<h1>Template gallery</h1>
<p class="note" style="margin-bottom:1.2rem">Pre-built agent tasks —
pick one, fill in the specifics, and launch. Every run is governed,
budgeted, and audited through the Gatelodge gateway.</p>
<div class="cards">{cards or '<p class=note>No templates available yet.</p>'}</div>"""
    return _page(request, client, "Templates", body)


@app.get("/launch/{tid}", response_class=HTMLResponse)
async def launch_form(request: Request, tid: int, flash: str = ""):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)

    def q():
        with _db() as c:
            return c.execute(
                "SELECT name, description, goal_template, "
                "criteria_template, parameters, schedule "
                "FROM portal_templates WHERE id=%s AND enabled",
                (tid,)).fetchone()
    t = await _run(q)
    if not t:
        return RedirectResponse("/", status_code=303)
    name, desc, goal_t, crit_t, params_json, sched = t
    try:
        params = json.loads(params_json)
    except Exception:
        params = []

    fields = ""
    for p in params:
        kind = p.get("type", "text")
        fields += (f'<label>{_e(p.get("label", p["name"]))}</label>'
                   + (f'<textarea name="{_e(p["name"])}" '
                      f'placeholder="{_e(p.get("placeholder", ""))}">'
                      f'</textarea>' if kind == "textarea" else
                      f'<input type="text" name="{_e(p["name"])}" '
                      f'placeholder="{_e(p.get("placeholder", ""))}">'))
    opts = "".join(
        f'<option value="{v}"{" selected" if v == sched else ""}>{v}</option>'
        for v in ("manual", "daily", "weekly"))

    body = f"""
<div class="box">
<p class="tag">{_e(sched)}</p>
<h2 style="margin-top:.5rem">{_e(name)}</h2>
<p class="note" style="margin-bottom:1rem">{_e(desc)}</p>
<form method="post" action="/launch/{tid}">
{fields}
<label>schedule</label>
<select name="schedule">{opts}</select>
<label>task name</label>
<input type="text" name="task_name" value="{_e(name)}" required>
<div style="margin-top:1.2rem"><button class="btn">Launch task</button>
<a class="btn ghost" href="/">cancel</a></div></form></div>
<p class="note">The agent will plan, execute with governed tools, and a
separate review pass grades the outcome before you see it.</p>"""
    return _page(request, client, f"Launch · {name}", body, flash)


@app.post("/launch/{tid}")
async def launch(request: Request, tid: int):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)
    form = await request.form()

    def q():
        with _db() as c:
            return c.execute(
                "SELECT goal_template, criteria_template, parameters, "
                "ruleset_id FROM portal_templates WHERE id=%s AND enabled",
                (tid,)).fetchone()
    t = await _run(q)
    if not t:
        return RedirectResponse("/", status_code=303)
    goal_t, crit_t, params_json, ruleset_id = t
    try:
        params = json.loads(params_json)
    except Exception:
        params = []

    values = {p["name"]: str(form.get(p["name"], "")).strip()
              for p in params}
    goal = goal_t
    crit = crit_t or ""
    for k, v in values.items():
        goal = goal.replace("{" + k + "}", v)
        crit = crit.replace("{" + k + "}", v)
    task_name = str(form.get("task_name", "Task")).strip()[:120]
    schedule = str(form.get("schedule", "manual"))
    if schedule not in ("manual", "daily", "weekly"):
        schedule = "manual"

    def save():
        with _db() as c:
            c.execute(
                "INSERT INTO portal_tasks (client_id, name, goal, "
                "success_criteria, inputs, schedule, next_run_at, "
                "ruleset_id) VALUES (%s,%s,%s,%s,%s,%s, now(), %s)",
                (client["id"], task_name, goal, crit,
                 "\n".join(f"{k}: {v}" for k, v in values.items()
                           if v), schedule, ruleset_id))
    await _run(save)
    return RedirectResponse("/?flash=task+launched", status_code=303)


@app.post("/task/{tid}/run")
async def task_run_again(request: Request, tid: int):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)

    def q():
        with _db() as c:
            c.execute("UPDATE portal_tasks SET next_run_at=now() "
                      "WHERE id=%s AND client_id=%s",
                      (tid, client["id"]))
    await _run(q)
    return RedirectResponse(f"/task/{tid}?flash=run+queued", status_code=303)


@app.get("/task/{tid}", response_class=HTMLResponse)
async def task_detail(request: Request, tid: int, flash: str = ""):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)

    def q():
        with _db() as c:
            task = c.execute(
                "SELECT name, goal, schedule, enabled FROM portal_tasks "
                "WHERE id=%s AND client_id=%s",
                (tid, client["id"])).fetchone()
            runs = c.execute(
                "SELECT id, classification, summary, started_at "
                "FROM portal_runs WHERE task_id=%s ORDER BY id DESC "
                "LIMIT 30", (tid,)).fetchall() if task else []
        return task, runs
    task, runs = await _run(q)
    if not task:
        return RedirectResponse("/", status_code=303)
    name, goal, sched, en = task

    tagmap = {"achieved": "ok", "partial": "warn", "failed": "bad",
              "running": "warn"}
    rtrs = "".join(
        f"<tr><td class='mono'>#{i}</td>"
        f"<td><span class='tag {tagmap.get(cl or '', '')}'>"
        f"{_e(cl or 'running')}</span></td>"
        f"<td style='max-width:420px'>{_e(su[:160])}</td>"
        f"<td class='note'>{str(sa)[:16]}</td>"
        f"<td><a href='/run/{i}'>view</a></td></tr>"
        for i, cl, su, sa in runs)

    body = f"""
<div class="inline" style="margin-bottom:1rem">
<span class="tag">{_e(sched)}</span>
{'<span class=tag ok>active</span>' if en else '<span class=tag>paused</span>'}
</div>
<h1>{_e(name)}</h1>
<div class="view-body" style="margin-bottom:1.4rem">{_e(goal)}</div>
<h2>Runs</h2>
<table><tr><th>run</th><th>outcome</th><th>summary</th><th>started</th>
<th></th></tr>
{rtrs or '<tr><td colspan=5 class=note>No runs yet — the first run '
'starts within a minute of launch</td></tr>'}</table>
<div class="inline">
<form method="post" action="/task/{tid}/run" style="display:inline">
<button class="btn">Run again now</button></form>
<a class="btn ghost" href="/">back</a></div>"""
    return _page(request, client, name, body, flash)


@app.get("/run/{rid}", response_class=HTMLResponse)
async def run_detail(request: Request, rid: int):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)

    def q():
        with _db() as c:
            run = c.execute(
                "SELECT r.task_id, t.name, t.client_id, r.classification,"
                " r.summary, r.report, r.started_at, r.facts, r.decision"
                " FROM portal_runs r "
                "JOIN portal_tasks t ON t.id=r.task_id WHERE r.id=%s",
                (rid,)).fetchone()
            steps = c.execute(
                "SELECT seq, tool, input, output, status FROM portal_steps"
                " WHERE run_id=%s ORDER BY seq", (rid,)).fetchall() \
                if run else []
        return run, steps
    run, steps = await _run(q)
    if not run or run[2] != client["id"]:
        return RedirectResponse("/", status_code=303)
    tid, tname, _, cl, su, report, sa, facts_txt, dec_txt = run

    decision_html = ""
    if dec_txt:
        try:
            dj = json.loads(dec_txt)
        except Exception:
            dj = {}
        dv = str(dj.get("decision", dec_txt))
        dcolor = {"approved": "ok", "denied": "bad"}.get(dv, "warn")
        rule = dj.get("rule", "")
        decision_html = f"""
<h2>Policy decision</h2>
<div class="box" style="border-left:3px solid var(--brass)">
<p style="font-size:1.05rem"><b>{_e(dv)}</b>
<span class="tag {dcolor}">deterministic</span></p>
<p class="note mono">{_e(f'rule: {rule}' if rule else dec_txt)}</p>
<p class="note">Produced by a versioned rules engine against the
extracted facts — not the agent's judgment.</p></div>"""

    facts_html = ""
    if facts_txt and facts_txt not in ("", "{}"):
        facts_html = f"""
<h2>Extracted facts</h2>
<div class="view-body mono" style="font-size:.78rem">{_e(facts_txt)}</div>"""

    step_html = ""
    for seq, tool, inp, out, stat in steps:
        draft_link = ""
        if tool == "email_draft" and out.startswith("DRAFT"):
            lines = out.split("\n")
            if len(lines) > 2:
                draft_link = (f'<p class="note" style="margin-top:.5rem">'
                              f'Draft recorded — send it from your own '
                              f'email client using the details above.</p>')
        label = ("Policy evaluation" if tool == "policy_check"
                 else f"Step {seq + 1}")
        step_html += f"""
<div class="box"><p><b>{_e(label)}</b> — <span class="tag">
{'ok' if stat == 'ok' else stat}</span> <span class="mono note">
{_e(tool)}</span></p>
<div class="view-body" style="font-size:.78rem;margin-top:.5rem">
{_e(out[:2000])}</div>{draft_link}</div>"""

    body = f"""
<div class="inline" style="margin-bottom:1rem">
<span class="tag">{_e(cl or 'running')}</span>
<span class="mono note">{_e(tname)} · run #{rid} · {str(sa)[:16]}</span></div>
{decision_html}
{facts_html}
<h2>Review</h2>
<div class="view-body">{_e(su or '(pending)')}</div>
<h2>Report</h2>
<div class="view-body">{_e(report or '(pending)')}</div>
<h2>Steps</h2>
{step_html or '<p class="note">no steps recorded</p>'}
<div class="inline" style="margin-top:1rem">
<a class="btn ghost" href="/task/{tid}">back to task</a></div>"""
    return _page(request, client, f"Run #{rid}", body)


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "task-portal",
            "gateway": bool(GATEWAY_URL and GATEWAY_KEY),
            "db": bool(DATABASE_URL)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

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
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

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
ALTER TABLE portal_rulesets ADD COLUMN IF NOT EXISTS client_id BIGINT;
ALTER TABLE portal_rulesets ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
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
    # fact gate — the policy says which facts it needs; if any are
    # missing the agent is told exactly what to go get and resubmit
    policy = _RUN_STATE.get("policy")
    if policy:
        missing = _missing_fields(policy[2], d)
        if missing:
            return ("FACTS INCOMPLETE — the decision policy still needs: "
                    + ", ".join(missing)
                    + ". Re-read the request text and RESUBMIT all facts "
                    "as one JSON object including these. If the text does "
                    "not state a value, infer the most reasonable one "
                    "from context (e.g. a count of prior events not "
                    "mentioned = 0) and include it.")
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
    (report, decision_json) — decision appended to the report verbatim.
    If required facts are missing the policy is NOT evaluated — the
    outcome is insufficient_facts with the missing list (the fact-gate
    pattern: an incomplete fact set must never produce a decision)."""
    facts = _RUN_STATE.get("facts") or {}
    if not facts:
        return report, ""
    try:
        rs = _RUN_STATE.get("policy")
        if not rs:
            with _db() as c:
                rs = c.execute(
                    "SELECT name, version, jdm FROM portal_rulesets "
                    "WHERE id=%s AND enabled",
                    (ruleset_id,)).fetchone()
        if not rs:
            return report, ""
        rs_name, rs_ver, jdm = rs

        missing = _missing_fields(jdm, facts)
        if missing:
            decision_json = json.dumps(
                {"decision": "insufficient_facts",
                 "missing_fields": missing}, sort_keys=True)
            detail = (f"INSUFFICIENT FACTS: {', '.join(missing)} — "
                      f"the policy was NOT evaluated.")
            with _db() as c:
                c.execute("INSERT INTO portal_steps (run_id, seq, tool,"
                          " input, output, status) VALUES (%s,%s,%s,%s,"
                          "%s,%s)",
                          (run_id, 900, "policy_check",
                           json.dumps(facts, sort_keys=True)[:4000],
                           detail[:6000], "blocked"))
            report += (f"\n\n---\n**Policy decision** — `{rs_name}` "
                       f"v{rs_ver}: **insufficient facts** "
                       f"({', '.join(missing)}). The rules engine did "
                       f"not evaluate — a decision requires every fact "
                       f"the policy defines.")
            transcript.append(f"policy_check (blocked): {rs_name} "
                              f"v{rs_ver} → insufficient_facts "
                              f"({', '.join(missing)})")
            return report, decision_json

        r = _evaluate_ruleset(jdm, facts)
        res = r.get("result", {}) if isinstance(r, dict) else {}
        perf = r.get("performance", "") if isinstance(r, dict) else ""
        decision_json = json.dumps(res, sort_keys=True)
        detail = (f"DECISION: {decision_json}\n"
                  f"RULESET: {rs_name} v{rs_ver} · evaluated in {perf}")
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


async def _gw_json(system: str, messages: list[dict],
                   max_tokens: int = 1200) -> str:
    async with httpx.AsyncClient(timeout=90) as c:
        r = await c.post(
            f"{GATEWAY_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
            json={"model": MODEL,
                  "messages": [{"role": "system", "content": system}]
                  + messages,
                  "max_tokens": max_tokens, "temperature": 0.1})
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
    if ruleset_id:
        with _db() as c:
            rs = c.execute(
                "SELECT name, version, jdm FROM portal_rulesets "
                "WHERE id=%s AND enabled", (ruleset_id,)).fetchone()
        if rs:
            _RUN_STATE["policy"] = rs
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
.ptable { font-size:.8rem; }
.ptable th { vertical-align:top; min-width:120px; }
.ptable th .f, .ptable th .n { padding:.25rem .4rem; margin-bottom:.2rem; }
.ptable td { padding:.2rem .25rem; }
.ptable input.cell, .ptable input.rname {
  padding:.3rem .4rem; font-family:Consolas,monospace; font-size:.76rem; }
.ptable tr.drafted td { background:rgba(212,175,55,.08); }
.mini { background:transparent; color:var(--faint); border:1px solid
  var(--line); border-radius:5px; padding:.05rem .35rem; font-size:.7rem;
  cursor:pointer; }
.mini:hover { color:var(--danger); border-color:var(--danger); }
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
<a href="/policies">Policies</a>
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


# ── decision policies: per-client rulesets + editor ──────────────────────

def _policy_model(jdm_json: str) -> dict | None:
    """Parse a single-table JDM graph into an editable model."""
    try:
        g = json.loads(jdm_json)
        for n in g.get("nodes", []):
            if n.get("type") != "decisionTableNode":
                continue
            ct = n.get("content", {})
            ins = [(i.get("field", ""), i.get("name", ""), i.get("id", ""))
                   for i in ct.get("inputs", [])]
            outs = [(o.get("field", ""), o.get("name", ""), o.get("id", ""))
                    for o in ct.get("outputs", [])]
            rules = []
            for r in ct.get("rules", []):
                cells = []
                for _, _, cid in ins + outs:
                    cells.append(str(r.get(cid, "")))
                rules.append({"name": str(r.get("_id", "")), "cells": cells})
            return {"hitPolicy": ct.get("hitPolicy", "first"),
                    "inputs": ins, "outputs": outs, "rules": rules}
    except Exception:
        pass
    return None


def _jdm_valid(jdm_json: str) -> bool:
    """Structural + cell-syntax validation. zen itself accepts any cell
    text and simply never matches bad expressions (fail-silent), so the
    cell grammar is enforced here instead."""
    try:
        from zen import ZenEngine
        ZenEngine().create_decision(json.loads(jdm_json))
        model = _policy_model(jdm_json)
        if not model:
            return False
        in_fields = [f for f, _, _ in model["inputs"]]
        out_fields = [f for f, _, _ in model["outputs"]]
        return all(
            _cells_valid(in_fields, out_fields, cells)
            for cells in [dict(zip(in_fields + out_fields, r["cells"]))
                          for r in model["rules"]])
    except Exception:
        return False


_COND_RE = re.compile(
    r'^(?:(==|!=|<=|>=|<|>)\s*("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?)?)?$')
_VAL_RE = re.compile(r'^("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?)?$')


_BARE_RE = re.compile(r'^("(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?)$')


def _normalize_cells(in_fields: list[str], out_fields: list[str],
                     cells: dict) -> dict:
    """LLM drafts are messy in two predictable ways: bare values for
    conditions ("premium" instead of == "premium" — a truthy always-match
    in zen, silently wrong) and unquoted output words (approved instead
    of "approved"). Both are normalized before grammar validation."""
    for f in in_fields:
        v = str(cells.get(f, "")).strip()
        if v and _BARE_RE.match(v):
            cells[f] = "== " + v
    for f in out_fields:
        v = str(cells.get(f, "")).strip()
        if v and not _VAL_RE.match(v):
            q = '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
            if _VAL_RE.match(q):
                cells[f] = q
    return cells


def _cells_valid(in_fields: list[str], out_fields: list[str],
                 cells: dict) -> bool:
    """Condition cells: operator + quoted string or bare number (or
    empty = any). Output cells: a value (or empty). This is the subset
    the editor documents; anything else silently never matches in zen,
    so it is rejected up front."""
    for f in in_fields:
        if not _COND_RE.match(str(cells.get(f, ""))):
            return False
    for f in out_fields:
        if not _VAL_RE.match(str(cells.get(f, ""))):
            return False
    return True


@app.get("/policies", response_class=HTMLResponse)
async def policies(request: Request, flash: str = ""):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)

    def q():
        with _db() as c:
            return c.execute(
                "SELECT id, name, description, version, enabled, "
                "(client_id IS NULL) AS is_global FROM portal_rulesets "
                "WHERE client_id IS NULL OR client_id=%s "
                "ORDER BY is_global DESC, id", (client["id"],)).fetchall()
    rows = await _run(q)

    trs = ""
    for pid, name, desc, ver, en, is_global in rows:
        scope = ("<span class='tag'>global</span>" if is_global
                 else "<span class='tag ok'>mine</span>")
        state = ('<span class=tag ok>on</span>' if en
                 else '<span class=tag>off</span>')
        act = (f"<a href='/policy/{pid}'>edit</a> · "
               f"<a href='/policy/{pid}#test'>test</a>"
               if not is_global else f"<a href='/policy/{pid}'>view</a> · "
               f"<a href='/policy/{pid}#test'>test</a>")
        trs += (f"<tr><td>{_e(name)}</td><td>{scope}</td><td>{state}</td>"
                f"<td class='mono note'>v{ver}</td>"
                f"<td class='note'>{_e(desc[:90])}</td><td>{act}</td></tr>")

    body = f"""
<h1>Decision policies</h1>
<p class="note" style="margin-bottom:1.2rem">Deterministic rulesets —
the agent extracts facts, the policy decides, the trace shows the rule
that fired. Global policies are maintained by us; yours are yours to
edit and version.</p>
<table><tr><th>policy</th><th>scope</th><th>state</th><th>ver</th>
<th>description</th><th></th></tr>
{trs or '<tr><td colspan=6 class=note>No policies yet — create one '
'below</td></tr>'}</table>
<div class="inline">
<a class="btn" href="/policy/0">New policy</a>
<a class="btn ghost" href="/">back</a></div>"""
    return _page(request, client, "Policies", body)


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
  if (!pid || pid === '0') { out.textContent = 'Save the policy first, then test.'; return; }
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
const esc = s => String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
function colFields() {
  const inF = [...document.querySelectorAll('#grid thead th[data-kind=in] .f')].map(i => i.value);
  const outF = [...document.querySelectorAll('#grid thead th[data-kind=out] .f')].map(i => i.value);
  return {inF, outF};
}
function addRuleFromDraft(row) {
  const grid = document.getElementById('grid');
  const {inF, outF} = colFields();
  const tr = document.createElement('tr');
  tr.classList.add('drafted');
  let h = '<td><input class="rname" value="' + esc(row.name) + '"></td>';
  for (const f of [...inF, ...outF]) {
    const kind = inF.includes(f) ? 'in' : 'out';
    const ci = kind === 'in' ? inF.indexOf(f) : outF.indexOf(f);
    const val = (row.cells && row.cells[f] !== undefined) ? row.cells[f] : '';
    h += '<td><input class="cell" data-kind="' + kind + '" data-col="' + ci + '" value="' + esc(val) + '"></td>';
  }
  h += '<td><button type="button" class="mini" onclick="this.closest(\\'tr\\').remove()">✕</button></td>';
  tr.innerHTML = h;
  grid.querySelector('tbody').appendChild(tr);
}
function makeTh(kind, field, name) {
  const th = document.createElement('th');
  th.dataset.kind = kind;
  th.innerHTML = '<input class="f mono" value="' + esc(field) + '" placeholder="field" style="font-size:.72rem">' +
    '<input class="n" value="' + esc(name) + '" placeholder="label" style="font-size:.72rem">' +
    '<button type="button" class="mini" onclick="delCol(this)">✕</button>';
  return th;
}
function rebuildGrid(j) {
  const grid = document.getElementById('grid');
  const thead = grid.querySelector('thead');
  const tbody = grid.querySelector('tbody');
  thead.innerHTML = '';
  const trh = document.createElement('tr');
  trh.innerHTML = '<th style="min-width:110px">rule</th>';
  j.inputs.forEach(c => trh.appendChild(makeTh('in', c.field, c.name)));
  j.outputs.forEach(c => trh.appendChild(makeTh('out', c.field, c.name)));
  const miniTh = document.createElement('th');
  miniTh.className = 'mini-col';
  trh.appendChild(miniTh);
  thead.appendChild(trh);
  tbody.innerHTML = '';
  j.rows.forEach(row => addRuleFromDraft(row));
  if (j.name) document.getElementById('pname').value = j.name;
  const desc = document.querySelector('input[name=description]');
  if (desc && j.description) desc.value = j.description;
}
async function draftRules() {
  const pid = document.getElementById('pid').value;
  const text = document.getElementById('nldesc').value.trim();
  const out = document.getElementById('nlout');
  if (!text) { out.textContent = 'Describe the rules first.'; return; }
  out.textContent = 'drafting…';
  try {
    const r = await fetch('/policy/compile', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({pid: +pid || 0, text})
    });
    const j = await r.json();
    if (j.error) {
      out.textContent = 'ERROR: ' + j.error +
        (j.dropped && j.dropped.length ? ' — ' + j.dropped.map(d => d.name).join(', ') + ' could not compile' : '');
      return;
    }
    if (j.mode === 'new') rebuildGrid(j);
    else j.rows.forEach(row => addRuleFromDraft(row));
    out.textContent = 'Drafted ' + j.rows.length + ' rule(s)' +
      (j.dropped && j.dropped.length ? ' · dropped ' + j.dropped.length + ' invalid draft(s)' : '') +
      ' — review every cell below, then Save.';
  } catch (e) { out.textContent = 'ERROR: ' + e; }
}
"""


@app.get("/policy/{pid}", response_class=HTMLResponse)
async def policy_editor(request: Request, pid: int, flash: str = ""):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)

    def q():
        with _db() as c:
            row = c.execute(
                "SELECT id, name, description, version, jdm, client_id "
                "FROM portal_rulesets WHERE id=%s", (pid,)).fetchone()
            return row
    row = await _run(q) if pid else None
    editable = (pid == 0) or bool(row and row[5] == client["id"])
    if pid and not row:
        return RedirectResponse("/policies", status_code=303)
    if pid and row[5] is not None and row[5] != client["id"]:
        return RedirectResponse("/policies", status_code=303)

    if row:
        name, desc, ver = row[1], row[2], row[3]
        model = _policy_model(row[4]) or {
            "hitPolicy": "first", "inputs": [("fact", "Fact", "c1")],
            "outputs": [("decision", "Decision", "o1")], "rules": []}
    else:
        name, desc, ver = "", "", 1
        model = {"hitPolicy": "first",
                 "inputs": [("customer_tier", "Tier", "c1"),
                            ("days_since_purchase", "Days", "c2")],
                 "outputs": [("decision", "Decision", "o1"),
                             ("rule", "Rule fired", "o2")],
                 "rules": [{"name": "example", "cells": [
                     '== "premium"', "<= 30", '"approved"', '"example"']}]}

    def th(col, kind):
        f, lbl, _ = col
        return (f'<th data-kind="{kind}">'
                f'<input class="f mono" value="{_e(f)}" placeholder="field" '
                f'style="font-size:.72rem"{" disabled" if not editable else ""}>'
                f'<input class="n" value="{_e(lbl)}" placeholder="label" '
                f'style="font-size:.72rem"{" disabled" if not editable else ""}>'
                + (f'<button type="button" class="mini" '
                   f'onclick="delCol(this)">✕</button>' if editable else "")
                + "</th>")

    head = ('<th style="min-width:110px">rule</th>'
            + "".join(th(c, "in") for c in model["inputs"])
            + "".join(th(c, "out") for c in model["outputs"])
            + '<th class="mini-col"></th>')
    rows_html = ""
    for r in model["rules"]:
        cells = ""
        for i, val in enumerate(r["cells"]):
            kind = "in" if i < len(model["inputs"]) else "out"
            ci = i if kind == "in" else i - len(model["inputs"])
            cells += (f'<td><input class="cell" data-kind="{kind}" '
                      f'data-col="{ci}" value="{_e(val)}"'
                      + ("" if editable else " disabled") + "></td>")
        rows_html += (f'<tr><td><input class="rname" value="{_e(r["name"])}"'
                      + ("" if editable else " disabled") + "></td>"
                      + cells
                      + ("<td><button type='button' class='mini' "
                         "onclick='this.closest(\"tr\").remove()'>✕</button>"
                         "</td>" if editable else "<td></td>")
                      + "</tr>")

    hp = model["hitPolicy"]
    ro = "" if editable else " disabled"
    save_btn = (f'<button class="btn">Save policy</button>' if editable
                else '<span class="tag">read-only · global policy</span>')
    new_btns = (f'<div class="inline" style="margin:.4rem 0 1rem">'
                f'<button type="button" class="btn ghost" '
                f'style="padding:.3rem .8rem;font-size:.75rem" '
                f'onclick="addRule()">+ rule</button>'
                f'<button type="button" class="btn ghost" '
                f'style="padding:.3rem .8rem;font-size:.75rem" '
                f'onclick="addCol(\'in\')">+ condition column</button>'
                f'<button type="button" class="btn ghost" '
                f'style="padding:.3rem .8rem;font-size:.75rem" '
                f'onclick="addCol(\'out\')">+ output column</button></div>'
                if editable else "")
    nl_label = ("Draft policy" if not pid else "Draft rules")
    nl_box = (f"""
<div class="box" style="margin-bottom:1.2rem;border-left:3px solid
  var(--brass)">
<label>describe your {"policy" if not pid else "new rules"} in plain
english</label>
<textarea id="nldesc" style="min-height:70px" placeholder="e.g.
Premium customers can change their mind within 30 days for orders under
$300. Anyone with 3 or more refunds in 90 days goes to manual review.
Everything else is denied."></textarea>
<div class="inline" style="margin-top:.6rem">
<button type="button" class="btn" onclick="draftRules()">
{nl_label}</button>
<span class="note" id="nlout"></span></div>
<p class="note" style="margin-top:.5rem">The AI drafts into the grid
below — every cell is yours to review and edit before saving. Saved
policies always run deterministically.</p></div>""" if editable else "")

    body = f"""
<form method="post" action="/policy/save" id="pform">
<input type="hidden" name="id" value="{pid}">
<input type="hidden" name="jdm" id="jdm" value="">
<div class="inline" style="margin-bottom:1rem">
<a class="btn ghost" href="/policies">← policies</a>
<span class="mono note">{"v" + str(ver) if pid else "new"}</span></div>
<label>policy name</label>
<input id="pname" name="name" value="{_e(name)}" required{ro}>
<label>description</label>
<input name="description" value="{_e(desc)}"{ro}>
<label>hit policy</label>
<select id="hitpolicy" name="hitpolicy"{ro}>
<option value="first"{' selected' if hp == 'first' else ''}>first match — first rule that fits decides</option>
<option value="collect"{' selected' if hp == 'collect' else ''}>collect — all matching rules combine</option>
</select>
{nl_box}
{new_btns}
<div style="overflow-x:auto"><table id="grid" class="ptable">
<thead><tr>{head}</tr></thead><tbody>{rows_html}</tbody></table></div>
<div class="inline" style="margin-top:1rem">{save_btn}
</div></form>
<p class="note">Condition cells are ZEN expressions relative to the column
field — e.g. <span class="mono">&lt;= 30</span>,
<span class="mono">== "enterprise"</span>, empty means any. Output cells
are values or expressions — quote strings:
<span class="mono">"approved"</span>.</p>
<div class="box" id="test" style="margin-top:1.6rem">
<h2 style="font-family:Georgia,serif">Test this policy</h2>
<p class="note">Evaluate saved facts against the ruleset — no agent,
no cost, instant.</p>
<input type="hidden" id="pid" value="{pid}">
<div class="inline" style="margin:.6rem 0">
<button type="button" class="btn ghost" style="padding:.3rem .8rem;
font-size:.75rem" onclick="fillFacts()">skeleton from fields</button></div>
<label>facts (JSON)</label>
<textarea id="facts" style="min-height:90px">{{}}</textarea>
<div class="inline" style="margin-top:.8rem">
<button type="button" class="btn" onclick="testPolicy()">Run test</button>
<span class="note" id="testnote"></span></div>
<div class="view-body mono" id="testout" style="margin-top:.8rem;
min-height:50px;font-size:.78rem">(result)</div></div>
<script>{POLICY_JS}</script>"""
    return _page(request, client, name or "New policy", body, flash)


@app.post("/policy/save")
async def policy_save(request: Request):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    pid = int(form.get("id") or 0)
    name = str(form.get("name", "")).strip()[:120]
    desc = str(form.get("description", "")).strip()[:400]
    jdm = str(form.get("jdm", "")).strip()

    if not name or not jdm or not _jdm_valid(jdm):
        return RedirectResponse(
            f"/policy/{pid}?flash=invalid+or+unparsable+policy", status_code=303)

    def save():
        with _db() as c:
            if pid:
                cur = c.execute(
                    "UPDATE portal_rulesets SET name=%s, description=%s, "
                    "jdm=%s, version=version+1, updated_at=now() "
                    "WHERE id=%s AND client_id=%s RETURNING id",
                    (name, desc, jdm, pid, client["id"])).fetchone()
                return cur[0] if cur else None
            return c.execute(
                "INSERT INTO portal_rulesets (name, description, version,"
                " jdm, client_id) VALUES (%s,%s,1,%s,%s) RETURNING id",
                (name, desc, jdm, client["id"])).fetchone()[0]
    new_id = await _run(save)
    if not new_id:
        return RedirectResponse("/policies?flash=not+yours+to+edit",
                                status_code=303)
    return RedirectResponse(f"/policy/{new_id}?flash=policy+saved",
                            status_code=303)


@app.post("/policy/{pid}/delete")
async def policy_delete(request: Request, pid: int):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)

    def q():
        with _db() as c:
            c.execute("DELETE FROM portal_rulesets WHERE id=%s AND "
                      "client_id=%s", (pid, client["id"]))
    await _run(q)
    return RedirectResponse("/policies?flash=policy+deleted", status_code=303)


@app.post("/policy/{pid}/test")
async def policy_test(request: Request, pid: int):
    client = _authed(request)
    if not client:
        return JSONResponse({"error": "auth"}, status_code=403)
    try:
        body = await request.json()
        facts = body.get("facts") or {}
        if not isinstance(facts, dict):
            raise ValueError
    except Exception:
        return JSONResponse({"error": "facts must be a JSON object"},
                            status_code=400)

    def q():
        with _db() as c:
            return c.execute(
                "SELECT name, version, jdm, enabled FROM portal_rulesets "
                "WHERE id=%s AND (client_id IS NULL OR client_id=%s)",
                (pid, client["id"])).fetchone()
    row = await _run(q)
    if not row or not row[3]:
        return JSONResponse({"error": "policy not found or disabled"},
                            status_code=404)
    try:
        r = _evaluate_ruleset(row[2], facts)
        return {"policy": f"{row[0]} v{row[1]}",
                "result": r.get("result", {}),
                "performance": r.get("performance", ""),
                "missing_fields": _missing_fields(row[2], facts)}
    except Exception as e:
        return JSONResponse({"error": f"evaluation failed: {e!r}"},
                            status_code=400)


# ── natural-language rule authoring ───────────────────────────────────────
# The LLM DRAFTS (rows into the editor grid); the human REVIEWS every
# cell; zen-engine VALIDATES on save. Nothing the model produces becomes
# a live policy without both.

COMPILE_ROWS_SYSTEM = """You compile plain-English business rules into \
decision-table rows for the ZEN rules engine. The policy's columns are \
given. Output ONLY JSON, no prose:

{"rows": [{"name": "kebab-case-id", "cells": {"<field>": "<cell>", ...}}]}

Cell conventions (STRICT):
- Condition cells are expressions RELATIVE to the column's field:
  == "premium"   != "gold"   <= 30   >= 3   > 100   < 5
  Empty string "" means no condition (any value matches).
- Quote string literals: == "enterprise". Leave numbers unquoted: <= 90.
- Output cells are the values the rule produces: quoted strings
  "approved" or plain numbers 8.
- EVERY input AND output column field must appear as a key in cells.
- One row per distinct rule in the description; name = short kebab id.
- Translate thresholds and conditions EXACTLY as stated.
- Never invent columns or fields that are not in the given list.

Example — columns inputs: [tier, days_since, amount], outputs: \
[decision, rule]:
{"rows": [
  {"name": "premium-under-200", "cells": {"tier": "== \\"premium\\"", \
"days_since": "<= 30", "amount": "<= 200", "decision": "\\"approved\\"", \
"rule": "\\"premium-under-200\\""}},
  {"name": "deny-rest", "cells": {"tier": "", "days_since": "", \
"amount": "", "decision": "\\"denied\\"", "rule": "\\"deny-rest\\""}}
]}"""

COMPILE_NEW_SYSTEM = """You design a decision policy from a plain-English \
description, for the ZEN rules engine. Output ONLY JSON, no prose:

{"name": "Policy name", "description": "one sentence",
 "inputs": [{"field": "snake_case", "name": "Human label"}],
 "outputs": [{"field": "snake_case", "name": "Human label"}],
 "rows": [{"name": "kebab-case-id", "cells": {"<field>": "<cell>", ...}}]}

Requirements (STRICT):
- inputs = 3-6 FACT columns the policy needs to decide (snake_case
  fields like customer_tier, days_since_purchase, order_amount_usd).
- One output column carries the decision/outcome value.
- One output column has field "rule": its cell value is the row's own
  name as a quoted string, so the trace shows which rule fired.
- Condition cells are expressions relative to the field:
  == "premium", != "gold", <= 30, >= 3; "" means any. Quote strings,
  keep numbers bare. Output cells are values: "approved" (quoted) or 8.
- EVERY input and output field must appear as a key in every row's cells.
- Order rules most-specific-first. The LAST row must be a fallback named
  "fallback" with every condition cell empty.
- 3-8 rules total. Translate the description exactly; do not add
  conditions it does not state."""


def _trial_jdm(ins: list[dict], outs: list[dict], rule: dict) -> dict:
    """Single-rule JDM used to validate a drafted row compiles + runs.
    Column ids are normalized to the field names so drafted cells (keyed
    by field) map straight onto the columns."""
    norm = lambda cols: [{"field": c["field"], "id": c["field"],
                          "name": c.get("name", c["field"]),
                          "type": "expression"} for c in cols]
    return {
        "edges": [{"id": "e1", "sourceId": "i", "targetId": "t"},
                  {"id": "e2", "sourceId": "t", "targetId": "o"}],
        "nodes": [{"id": "i", "type": "inputNode", "name": "Facts"},
                  {"id": "o", "type": "outputNode", "name": "Decision"},
                  {"id": "t", "type": "decisionTableNode",
                   "name": "trial",
                   "content": {"hitPolicy": "first", "inputs": norm(ins),
                               "outputs": norm(outs),
                               "rules": [rule]}}]}


def _draft_rows_valid(ins: list[dict], outs: list[dict],
                      rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Keep only rows that pass the cell grammar and compile; return
    (valid, dropped). zen accepts any cell text (fail-silent), so the
    grammar check is what actually rejects malformed drafts."""
    from zen import ZenEngine
    engine = ZenEngine()
    dummy = {c["field"]: 0 for c in ins}
    in_fields = [c["field"] for c in ins]
    out_fields = [c["field"] for c in outs]
    valid, dropped = [], []
    for r in rows:
        name = str(r.get("name", "")).strip()[:60] or "draft"
        cells = _normalize_cells(
            in_fields, out_fields,
            {k: str(v) for k, v in (r.get("cells") or {}).items()})
        reason = None
        if not _cells_valid(in_fields, out_fields, cells):
            reason = "cell does not match the expression grammar"
        else:
            rule = {"_id": name, **cells}
            try:
                d = engine.create_decision(_trial_jdm(ins, outs, rule))
                d.evaluate(dummy)
            except Exception as e:
                reason = str(e)[:200]
        if reason:
            dropped.append({"name": name, "error": reason})
        else:
            valid.append({"name": name, "cells": cells})
    return valid, dropped


@app.post("/policy/compile")
async def policy_compile(request: Request):
    """Draft rules (or a whole policy) from a plain-English description.
    pid=0 → design a new policy (columns + rules). pid>0 → draft rows
    against the existing policy's columns. The client reviews everything
    in the editor before anything is saved."""
    client = _authed(request)
    if not client:
        return JSONResponse({"error": "auth"}, status_code=403)
    try:
        body = await request.json()
        pid = int(body.get("pid") or 0)
        text = str(body.get("text", "")).strip()[:2000]
    except Exception:
        return JSONResponse({"error": "bad request body"},
                            status_code=400)
    if not text:
        return JSONResponse({"error": "describe the rules first"},
                            status_code=400)

    try:
        if pid:
            def q():
                with _db() as c:
                    return c.execute(
                        "SELECT jdm FROM portal_rulesets WHERE id=%s "
                        "AND (client_id IS NULL OR client_id=%s)",
                        (pid, client["id"])).fetchone()
            row = await _run(q)
            if not row:
                return JSONResponse({"error": "policy not found"},
                                    status_code=404)
            model = _policy_model(row[0])
            if not model:
                return JSONResponse(
                    {"error": "policy is not a single decision table"},
                    status_code=400)
            ins = [{"field": f, "name": n, "type": "expression"}
                   for f, n, _ in model["inputs"]]
            outs = [{"field": f, "name": n, "type": "expression"}
                    for f, n, _ in model["outputs"]]
            col_spec = ("COLUMNS\ninputs (conditions): "
                        + "; ".join(f"{c['field']} — {c['name']}"
                                    for c in ins)
                        + "\noutputs (values): "
                        + "; ".join(f"{c['field']} — {c['name']}"
                                    for c in outs))
            rv = await _gw_json(
                COMPILE_ROWS_SYSTEM,
                [{"role": "user", "content": f"{col_spec}\n\n"
                                             f"DESCRIPTION:\n{text}\n\n"
                                             f"Output ONLY the JSON."}],
                max_tokens=3000)
            m = re.search(r"\{.*\}", rv, re.S)
            if not m:
                return JSONResponse(
                    {"error": "could not draft from that description"},
                    status_code=400)
            rows = json.loads(m.group(0)).get("rows") or []
            valid, dropped = _draft_rows_valid(ins, outs, rows)
            if not valid:
                return JSONResponse(
                    {"error": "no valid rules drafted",
                     "dropped": dropped}, status_code=400)
            return {"mode": "rows", "rows": valid, "dropped": dropped}

        # new-policy mode: the LLM designs columns + rules together
        rv = await _gw_json(
            COMPILE_NEW_SYSTEM,
            [{"role": "user",
              "content": f"DESCRIPTION:\n{text}\n\nOutput ONLY the JSON."}],
            max_tokens=3000)
        m = re.search(r"\{.*\}", rv, re.S)
        if not m:
            return JSONResponse(
                {"error": "could not design a policy from that "
                          "description"}, status_code=400)
        d = json.loads(m.group(0))
        ins = [{"field": str(c.get("field", "")).strip(),
                "name": str(c.get("name", c.get("field", "")))[:40],
                "type": "expression"}
               for c in (d.get("inputs") or []) if c.get("field")]
        outs = [{"field": str(c.get("field", "")).strip(),
                 "name": str(c.get("name", c.get("field", "")))[:40],
                 "type": "expression"}
                for c in (d.get("outputs") or []) if c.get("field")]
        ins = ins[:6]
        outs = outs[:4]
        if not (2 <= len(ins) <= 6) or not (1 <= len(outs) <= 4):
            return JSONResponse(
                {"error": "draft needs 2-6 condition columns and "
                          "1-4 output columns"}, status_code=400)
        valid, dropped = _draft_rows_valid(ins, outs, d.get("rows") or [])
        if not valid:
            return JSONResponse(
                {"error": "no valid rules drafted", "dropped": dropped},
                status_code=400)
        return {"mode": "new",
                "name": str(d.get("name", ""))[:120] or "Drafted policy",
                "description": str(d.get("description", ""))[:400],
                "inputs": [{"field": c["field"], "name": c["name"]}
                           for c in ins],
                "outputs": [{"field": c["field"], "name": c["name"]}
                            for c in outs],
                "rows": valid, "dropped": dropped}
    except Exception as e:
        return JSONResponse({"error": f"draft failed: {e!r}"},
                            status_code=400)


@app.get("/launch/{tid}", response_class=HTMLResponse)
async def launch_form(request: Request, tid: int, flash: str = ""):
    client = _authed(request)
    if not client:
        return RedirectResponse("/login", status_code=303)

    def q():
        with _db() as c:
            return c.execute(
                "SELECT name, description, goal_template, "
                "criteria_template, parameters, schedule, ruleset_id "
                "FROM portal_templates WHERE id=%s AND enabled",
                (tid,)).fetchone()
    t = await _run(q)
    if not t:
        return RedirectResponse("/", status_code=303)
    name, desc, goal_t, crit_t, params_json, sched, tpl_ruleset = t
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

    policy_html = ""
    if tpl_ruleset:
        def qp():
            with _db() as c:
                return c.execute(
                    "SELECT id, name, version, (client_id IS NULL) "
                    "FROM portal_rulesets WHERE enabled AND "
                    "(client_id IS NULL OR client_id=%s) ORDER BY "
                    "client_id IS NULL DESC, id", (client["id"],)).fetchall()
        pols = await _run(qp)
        opts = "".join(
            f'<option value="{pid}"{" selected" if pid == tpl_ruleset else ""}>'
            f'{_e(pname)} v{ver} · {"global" if glob else "mine"}</option>'
            for pid, pname, ver, glob in pols)
        policy_html = f"""
<label>decision policy to apply</label>
<select name="policy_id">{opts}</select>
<p class="note" style="margin-top:.3rem">The agent extracts the facts;
this policy decides — deterministically. Manage yours under
<a href="/policies" style="color:var(--brassb)">Policies</a>.</p>"""

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
{policy_html}
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

    # client may pick one of their own policies or a global one
    try:
        policy_id = int(form.get("policy_id") or 0)
    except ValueError:
        policy_id = 0

    def save():
        with _db() as c:
            rid = ruleset_id
            if policy_id:
                ok = c.execute(
                    "SELECT 1 FROM portal_rulesets WHERE id=%s AND enabled"
                    " AND (client_id IS NULL OR client_id=%s)",
                    (policy_id, client["id"])).fetchone()
                if ok:
                    rid = policy_id
            c.execute(
                "INSERT INTO portal_tasks (client_id, name, goal, "
                "success_criteria, inputs, schedule, next_run_at, "
                "ruleset_id) VALUES (%s,%s,%s,%s,%s,%s, now(), %s)",
                (client["id"], task_name, goal, crit,
                 "\n".join(f"{k}: {v}" for k, v in values.items()
                           if v), schedule, rid))
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

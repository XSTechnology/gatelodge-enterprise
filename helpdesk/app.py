"""Gatelodge helpdesk service — the Gatekeeper (phase 4 v1).

A customer-facing AI assistant whose every LLM call routes through a
Gatelodge gateway instance (dogfooding: alias chains, budgets, audit).

Endpoints:
  GET  /healthz                — liveness
  GET  /api/config             — public config for the widget
  POST /api/chat/session       — open a chat session (Turnstile-verified)
  POST /api/chat/message       — send a message; SSE stream back
  POST /api/chat/escalate      — collect email + hand off to a human
  GET  /api/admin/reviews      — review queue (Bearer REVIEW_KEY)
  POST /api/admin/reviewed     — mark a session reviewed

Flow per message (two-stage, both through the gateway):
  1. triage call (cheap model)  -> {doc, answerable, confident, legal}
  2. answer call (bigger model) -> streams tokens from the KB doc
  Not answerable / not confident / legal -> escalate (collect email).

Env:
  GATEWAY_URL        — OpenAI-compatible base of the gateway container
  GATEWAY_KEY        — budget-capped helpdesk gateway key
  TRIAGE_MODEL       — alias for triage (default "triage")
  ANSWER_MODEL       — alias for the answer (default "answer")
  DATABASE_URL       — Postgres (conversations + review queue)
  TURNSTILE_SECRET   — unset = dev mode (verification skipped)
  TURNSTILE_SITEKEY  — served via /api/config
  ALLOWED_ORIGINS    — comma-separated CORS origins
  RESEND_API_KEY     — escalation email delivery
  EMAIL_FROM         — e.g. "Gatekeeper <support@datagatelodge.com>"
  SUPPORT_EMAIL      — where escalations land
  REVIEW_KEY         — bearer for /api/admin/*
  PORT               — default 8100
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
import uuid
from pathlib import Path

import httpx
import psycopg
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

# ── config ────────────────────────────────────────────────────────────────

GATEWAY_URL = os.environ.get("GATEWAY_URL", "").rstrip("/")
GATEWAY_KEY = os.environ.get("GATEWAY_KEY", "")
TRIAGE_MODEL = os.environ.get("TRIAGE_MODEL", "triage")
ANSWER_MODEL = os.environ.get("ANSWER_MODEL", "answer")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
TURNSTILE_SECRET = os.environ.get("TURNSTILE_SECRET", "")
TURNSTILE_SITEKEY = os.environ.get("TURNSTILE_SITEKEY", "")
ALLOWED_ORIGINS = [o.strip() for o in
                   os.environ.get("ALLOWED_ORIGINS",
                                  "https://datagatelodge.com").split(",")
                   if o.strip()]
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
EMAIL_FROM = os.environ.get("EMAIL_FROM",
                            "Gatekeeper <support@datagatelodge.com>")
SUPPORT_EMAIL = os.environ.get("SUPPORT_EMAIL", "support@datagatelodge.com")
REVIEW_KEY = os.environ.get("REVIEW_KEY", "")
INBOUND_KEY = os.environ.get("INBOUND_KEY", "")
PORT = int(os.environ.get("PORT", "8100"))

MAX_HISTORY = 6            # messages of context sent to the answer model
MAX_MESSAGE_LEN = 4000
INBOUND_DAILY_CAP = 3      # auto-replies per sender per day

app = FastAPI(title="Gatelodge Gatekeeper", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── knowledge base ────────────────────────────────────────────────────────

KB_DIR = Path(__file__).resolve().parent / "kb"


def _is_internal(content: str) -> bool:
    """Docs marked `internal: true` are staff-only (competitive and
    support playbooks). They never join the retrieval catalog, never
    get intent-analyzed, and are never served to customers — the Lodge
    Office shows them with a badge instead."""
    return bool(re.search(r"^internal:\s*true\s*$", content, re.M | re.I))


def _load_kb() -> dict[str, dict]:
    docs = {}
    for p in sorted(KB_DIR.glob("*.md")):
        text = p.read_text(encoding="utf-8")
        if _is_internal(text):
            continue
        title = re.search(r"^title:\s*(.+)$", text, re.M)
        desc = re.search(r"^description:\s*(.+)$", text, re.M)
        docs[p.stem] = {
            "name": p.stem,
            "title": (title.group(1).strip() if title else p.stem),
            "summary": (desc.group(1).strip() if desc else ""),
            "content": text,
        }
    return docs


KB = _load_kb()

# ── database (Postgres via psycopg; sync calls wrapped in to_thread so
#    the service runs under any event loop — Proactor on Windows dev,
#    selector loops in production) ─────────────────────────────────────────

_schema = """
CREATE TABLE IF NOT EXISTS chat_sessions (
  id TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ip TEXT NOT NULL DEFAULT '',
  email TEXT,
  escalated BOOLEAN NOT NULL DEFAULT FALSE,
  needs_review BOOLEAN NOT NULL DEFAULT FALSE,
  message_count INT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS chat_messages (
  id BIGSERIAL PRIMARY KEY,
  session_id TEXT NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  confident BOOLEAN,
  doc TEXT,
  model TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_messages_session
  ON chat_messages (session_id, id);
CREATE TABLE IF NOT EXISTS kb_overrides (
  name TEXT PRIMARY KEY,
  content TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS inbound_emails (
  id TEXT PRIMARY KEY,
  from_addr TEXT NOT NULL,
  subject TEXT DEFAULT '',
  body TEXT DEFAULT '',
  status TEXT NOT NULL DEFAULT 'review',
  confident BOOLEAN,
  doc TEXT,
  topic TEXT,
  reply TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  handled_at TIMESTAMPTZ
);
ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS last_topic TEXT;
CREATE TABLE IF NOT EXISTS contacts (
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
);
ALTER TABLE email_log ADD COLUMN IF NOT EXISTS subject TEXT DEFAULT '';
ALTER TABLE email_log ADD COLUMN IF NOT EXISTS body TEXT DEFAULT '';
CREATE TABLE IF NOT EXISTS tasks (
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
);
CREATE TABLE IF NOT EXISTS task_runs (
  id BIGSERIAL PRIMARY KEY,
  task_id BIGINT NOT NULL,
  status TEXT DEFAULT 'running',
  classification TEXT DEFAULT '',
  summary TEXT DEFAULT '',
  report TEXT DEFAULT '',
  started_at TIMESTAMPTZ DEFAULT now(),
  finished_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS task_steps (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL,
  seq INT DEFAULT 0,
  tool TEXT DEFAULT '',
  description TEXT DEFAULT '',
  input TEXT DEFAULT '',
  output TEXT DEFAULT '',
  status TEXT DEFAULT 'ok',
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS tools (
  name TEXT PRIMARY KEY,
  description TEXT DEFAULT '',
  kind TEXT DEFAULT 'builtin',
  enabled BOOLEAN DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS tool_proposals (
  id BIGSERIAL PRIMARY KEY,
  capability TEXT NOT NULL,
  context TEXT DEFAULT '',
  status TEXT DEFAULT 'open',
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS rulesets (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  kind TEXT DEFAULT 'general',
  description TEXT DEFAULT '',
  version INT DEFAULT 1,
  jdm TEXT NOT NULL,
  enabled BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS kb_intent (
  doc TEXT NOT NULL,
  section TEXT NOT NULL,
  questions TEXT NOT NULL,
  summary TEXT DEFAULT '',
  topics TEXT DEFAULT '',
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (doc, section)
);
ALTER TABLE inbound_emails ADD COLUMN IF NOT EXISTS rule TEXT DEFAULT '';
"""


def _load_kb_overrides() -> dict:
    """DB overrides merged over the built-in docs (admin live edits).

    A row whose name is NOT in the built-in set is a document created in
    the Lodge Office — it is ADDED to the catalog (title/description
    parsed from its content, which the triage stage routes on)."""
    docs = dict(KB)
    if not DATABASE_URL:
        return docs
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            rows = conn.execute(
                "SELECT name, content FROM kb_overrides").fetchall()
    except Exception:
        return docs
    for name, content in rows:
        if _is_internal(content):
            # a saved edit that keeps the internal flag stays internal
            continue
        title = re.search(r"^title:\s*(.+)$", content, re.M)
        desc = re.search(r"^description:\s*(.+)$", content, re.M)
        base = docs.get(name)
        docs[name] = {
            "name": name,
            "title": (title.group(1).strip() if title
                      else (base["title"] if base else name)),
            "summary": (desc.group(1).strip() if desc
                        else (base["summary"] if base else "")),
            "content": content,
        }
    return docs


def _run(fn):
    return asyncio.to_thread(fn)


# ── retrieval (v2 intelligence) ───────────────────────────────────────────
# Hybrid: chunk the merged KB, score chunks with TF-IDF cosine vectors
# (keyword recall), then let the triage model rerank and pick 1-3 passages
# (semantic judgment, follow-up aware). Answers cite their passages and
# may draw on several docs. Everything rebuilds per request, so Lodge
# Office document CRUD applies instantly. When a real embedding model is
# available through the gateway, it can replace the TF-IDF scorer behind
# the same "scored chunks for a query" interface.

_STOP = frozenset("""a an and are as at be but by can could do does for from
has have how i if in into is it its may might must of on or our so than that
the their them then there these they this to too us was we what when where
which who why will with would you your""".split())


def _tok(text: str) -> list[str]:
    return [t for t in re.findall(r"[a-z0-9']+", text.lower())
            if len(t) > 2 and t not in _STOP]


def _chunk_kb(kb: dict, intents: dict | None = None) -> list[dict]:
    """Every '## ' section of every doc is a retrieval chunk. The doc's
    title/description/section-heading terms are folded into the chunk's
    term counts, so doc-level and section-level queries both hit.
    When an intent map exists for the section, its pre-analyzed
    QUESTIONS + summary + topics fold in too — user-language phrasing
    ("how much does it cost") matches lexically even when the prose
    doesn't share the words."""
    intents = intents or {}
    chunks: list[dict] = []
    for name, d in kb.items():
        for title, text in _split_sections(d["content"]):
            intent = intents.get((name, title))
            tf: dict[str, int] = {}
            src = f'{d["title"]} {d["summary"]} {title} {title} {text}'
            if intent:
                src += (" " + intent["summary"] + " "
                        + " ".join(intent["topics"]) + " "
                        + " ".join(intent["questions"]))
            for t in _tok(src):
                tf[t] = tf.get(t, 0) + 1
            chunks.append({"id": len(chunks), "doc": name,
                           "doc_title": d["title"], "section": title,
                           "text": text, "tf": tf,
                           "intent": intent})
    return chunks


def _build_index(chunks: list[dict]) -> dict:
    n = max(len(chunks), 1)
    df: dict[str, int] = {}
    for c in chunks:
        for t in c["tf"]:
            df[t] = df.get(t, 0) + 1
    idf = {t: math.log((n + 1) / (d + 1)) + 1.0 for t, d in df.items()}
    for c in chunks:
        c["vec"] = {t: (1.0 + math.log(cnt)) * idf.get(t, 1.0)
                    for t, cnt in c["tf"].items()}
        c["norm"] = math.sqrt(sum(w * w for w in c["vec"].values())) or 1.0
    return idf


def _search(query: str, chunks: list[dict], idf: dict,
            k: int = 8) -> list[dict]:
    qv: dict[str, float] = {}
    for t in _tok(query):
        qv[t] = qv.get(t, 0.0) + idf.get(t, 0.3)
    if not qv:
        return []
    qn = math.sqrt(sum(w * w for w in qv.values()))
    scored = []
    for c in chunks:
        dot = sum(w * c["vec"].get(t, 0.0) for t, w in qv.items())
        if dot > 0:
            scored.append((dot / (qn * c["norm"]), c))
    scored.sort(key=lambda x: -x[0])
    return [c for _, c in scored[:k]]


def _retrieve(query: str) -> tuple[list[dict], list[dict]]:
    """Merged KB -> (all chunks, top candidates for the query)."""
    kb = _load_kb_overrides()
    intents = _load_kb_intents(kb)
    chunks = _chunk_kb(kb, intents)
    idf = _build_index(chunks)
    return chunks, _search(query, chunks, idf)


# ── intent engine: pre-analyzed documents ─────────────────────────────────
# Every KB doc is analyzed once (at authoring/deploy time, not question
# time): an LLM reads each section and records what it contains and the
# questions it answers — phrased the way users actually ask. The map is
# data (kb_intent), reviewable in the Lodge Office, and folds into both
# retrieval stages: TF-IDF terms (user-language lexical match) and the
# triage router's candidate block (semantic pick without shipping the
# full passages).

INTENT_ANALYZE_SYSTEM = """You pre-analyze one section of product \
documentation so a help desk can route real user questions to it. Given \
the DOCUMENT and one SECTION, output ONLY JSON:

{"summary": "<1-2 sentences: what this section contains>",
 "topics": ["<short tag>", "..."],
 "questions": ["<question a user would ask that THIS section answers>", \
"..."]}

Requirements:
- 3-6 questions, phrased the way real users ask — include paraphrases
  and synonyms ("how much does it cost", "what's the price", "is there
  a free tier").
- Only questions this SECTION genuinely answers on its own.
- No meta commentary. Output ONLY the JSON."""


def _split_sections(content: str) -> list[tuple[str, str]]:
    """(title, text) for every '## ' section; the preamble is
    'overview'."""
    m = re.search(r"^description:.*$", content, re.M)
    body = content[m.end():] if m else content
    sections: list[tuple[str, list[str]]] = []
    cur_title, cur = "overview", []
    for ln in body.splitlines():
        h = re.match(r"^##\s+(.+?)\s*$", ln)
        if h:
            sections.append((cur_title, cur))
            cur_title, cur = h.group(1), []
        else:
            cur.append(ln)
    sections.append((cur_title, cur))
    return [(t, "\n".join(ls).strip()) for t, ls in sections
            if "\n".join(ls).strip()]


def _load_kb_intents(kb: dict) -> dict:
    """{(doc, section): intent-dict} for every analyzed section."""
    out: dict = {}
    if not DATABASE_URL:
        return out
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            rows = conn.execute(
                "SELECT doc, section, questions, summary, topics "
                "FROM kb_intent").fetchall()
    except Exception:
        return out
    for doc, section, qjson, summary, tjson in rows:
        try:
            questions = json.loads(qjson)
        except Exception:
            questions = []
        try:
            topics = json.loads(tjson)
        except Exception:
            topics = []
        if questions:
            out[(doc, section)] = {"questions": questions,
                                   "summary": summary or "",
                                   "topics": topics or []}
    return out


async def _analyze_section(doc_title: str, section: str,
                           text: str) -> dict:
    """One LLM call: what does this section contain + which questions
    does it answer."""
    user = (f"DOCUMENT: {doc_title}\n\nSECTION: {section}\n\n"
            f"{text[:3000]}\n\nOutput ONLY the JSON.")
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(
            f"{GATEWAY_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
            json={"model": TRIAGE_MODEL,
                  "messages": [
                      {"role": "system",
                       "content": INTENT_ANALYZE_SYSTEM},
                      {"role": "user", "content": user}],
                  "max_tokens": 600, "temperature": 0.1})
        content = r.json()["choices"][0]["message"]["content"] or ""
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        raise RuntimeError(f"unparseable analysis for {doc_title}/{section}")
    d = json.loads(m.group(0))
    questions = [str(q).strip()[:200] for q in (d.get("questions") or [])
                 if str(q).strip()][:8]
    if not questions:
        raise RuntimeError(f"no questions for {doc_title}/{section}")
    return {"summary": str(d.get("summary", ""))[:400],
            "topics": [str(t).strip()[:40] for t in (d.get("topics") or [])
                       if str(t).strip()][:6],
            "questions": questions}


def _write_kb_intent(name: str, rows: dict) -> None:
    """Upsert the analysis rows; drop rows for removed sections."""
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("DELETE FROM kb_intent WHERE doc=%s", (name,))
        for section, intent in rows.items():
            conn.execute(
                "INSERT INTO kb_intent (doc, section, questions, summary,"
                " topics) VALUES (%s, %s, %s, %s, %s)",
                (name, section, json.dumps(intent["questions"]),
                 intent["summary"], json.dumps(intent["topics"])))


async def analyze_doc(name: str | None = None) -> dict:
    """Analyze one doc (or every doc) and store its intent map.
    Returns {doc: sections_analyzed}."""
    kb = _load_kb_overrides()
    names = [name] if name else sorted(kb)
    results = {}
    for n in names:
        d = kb.get(n)
        if not d:
            continue
        rows: dict = {}
        for section, text in _split_sections(d["content"]):
            try:
                rows[section] = await _analyze_section(
                    d["title"], section, text)
            except Exception as e:
                print(f"[intent] analyze failed {n}/{section}: {e!r}")
        if rows:
            await _run(lambda r=rows: _write_kb_intent(n, r))
            results[n] = len(rows)
            print(f"[intent] {n}: {len(rows)} sections analyzed")
    return results


async def _seed_kb_intents() -> None:
    """Startup: analyze docs that have no intent map yet (idempotent,
    background — never blocks serving)."""
    try:
        kb = _load_kb_overrides()
        with psycopg.connect(DATABASE_URL) as conn:
            done = {r[0] for r in conn.execute(
                "SELECT DISTINCT doc FROM kb_intent").fetchall()}
        todo = [n for n in sorted(kb) if n not in done]
        for n in todo:
            print(f"[intent] seeding analysis for {n}")
            await analyze_doc(n)
        if todo:
            print(f"[intent] seed complete ({len(todo)} docs)")
    except Exception as e:
        print(f"[intent] seed error: {e!r}")


# ── task tools (v3: the task engine's hands) ─────────────────────────────
# Each tool is a small governed capability. Builtin tools run in-process;
# MCP tools can be registered later through the same registry (the kind
# column marks them). Every call is recorded in task_steps.

TOOLS: dict[str, dict] = {}


def tool(name: str, description: str):
    def deco(fn):
        TOOLS[name] = {"description": description, "fn": fn}
        return fn
    return deco


@tool("db_query",
      "Run a READ-ONLY SQL query against the shared Postgres — tables: "
      "chat_sessions, chat_messages, inbound_emails, contacts, leads, "
      "licenses, email_log, kb_overrides, tasks, task_runs. SELECT only; "
      "results capped at 50 rows. Input: the SQL.")
async def t_db_query(sql: str) -> str:
    s = (sql or "").strip().rstrip(";")
    if not s.lower().lstrip("(").startswith("select"):
        return "ERROR: only SELECT queries are allowed."

    def run():
        with psycopg.connect(DATABASE_URL) as conn:
            cur = conn.execute(s + " LIMIT 50")
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()[:50]
        return cols, rows
    try:
        cols, rows = await _run(run)
    except Exception as e:
        return f"ERROR: {e!r}"
    if not cols:
        return "(no columns)"
    out = [" | ".join(cols)]
    out += [" | ".join(str(v) for v in r) for r in rows]
    return "\n".join(out) or "(no rows)"


@tool("kb_search",
      "Search the Gatekeeper knowledge base with the hybrid retrieval "
      "engine. Input: a question or keywords. Returns the best passages.")
async def t_kb_search(query: str) -> str:
    try:
        _, cands = await _run(lambda: _retrieve(query or ""))
    except Exception as e:
        return f"ERROR: {e!r}"
    if not cands:
        return "(no passages found)"
    return "\n\n".join(f'[{c["doc"]} · {c["section"]}]\n{c["text"][:900]}'
                       for c in cands[:3])


@tool("kb_write",
      "Create or update a knowledge-base document (live for the agent on "
      "the next question). Input JSON: "
      '{"name": "doc-name", "title": "...", "description": "...", '
      '"content": "markdown with ## sections"}.')
async def t_kb_write(spec: str) -> str:
    try:
        d = json.loads(spec)
        name = str(d.get("name", "")).strip().lower()
        title = str(d.get("title", "")).strip()
        desc = str(d.get("description", "")).strip()
        content = str(d.get("content", "")).strip()
    except Exception:
        return "ERROR: input must be JSON with name/title/description/content"
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,40}", name) or not title \
            or not desc or not content:
        return "ERROR: name (slug), title, description and content required"
    doc = (f"# {title}\ntitle: {title}\ndescription: {desc}\n\n"
           + content + "\n")

    def run():
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute(
                "INSERT INTO kb_overrides (name, content) VALUES (%s, %s) "
                "ON CONFLICT (name) DO UPDATE SET content=%s, "
                "updated_at=now()", (name, doc, doc))
    await _run(run)
    return f"Document '{name}' saved — live on the next question."


@tool("email_draft",
      "Record an email DRAFT in the task report (never sends — the owner "
      "reviews and sends from the Lodge Office). Input JSON: "
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
    return ("DRAFT RECORDED (owner sends from the Lodge Office):\n"
            f"To: {to}\nSubject: {subject}\n\n{body[:4000]}")


@tool("email_send",
      "Actually SEND an email from support@datagatelodge.com. Use ONLY "
      "for internal notifications (to the owner/team) — customer-facing "
      "mail must use email_draft. Input JSON: "
      '{"to": "...", "subject": "...", "body": "..."}.')
async def t_email_send(spec: str) -> str:
    try:
        d = json.loads(spec)
        to = str(d.get("to", "")).strip().lower()
        subject = str(d.get("subject", "")).strip()
        body = str(d.get("body", "")).strip()
    except Exception:
        return "ERROR: input must be JSON with to/subject/body"
    if not to or "@" not in to or not subject or not body:
        return "ERROR: to, subject and body required"
    ok, detail = await send_reply_email(to, subject, body, "task")
    await _run_email_log(to, ok, detail, kind="task-email",
                         subject=subject, body=body)
    return f"{'SENT' if ok else 'SEND FAILED'}: {detail}"


@tool("web_fetch",
      "Fetch a URL over HTTPS and return the HTTP status plus a text "
      "excerpt. Input: the URL.")
async def t_web_fetch(url: str) -> str:
    url = (url or "").strip()
    if not url.startswith("https://") and not url.startswith("http://"):
        return "ERROR: provide a full http(s) URL"
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
            r = await c.get(url, headers={"user-agent": "gatekeeper-task"})
        text = " ".join(r.text.split())[:1500]
        return f"HTTP {r.status_code}\n{text}"
    except Exception as e:
        return f"ERROR: {e!r}"


@tool("gap_cluster",
      "Cluster the unanswered questions from the last 14 days (chats that "
      "needed review or escalated, plus review-queued inbound email) into "
      "knowledge-gap topics. No input needed.")
async def t_gap_cluster(_: str = "") -> str:
    def run():
        with psycopg.connect(DATABASE_URL) as conn:
            rows = conn.execute(
                "SELECT m.content FROM chat_messages m JOIN chat_sessions s"
                " ON s.id=m.session_id WHERE m.role='user' AND "
                "(s.needs_review OR s.escalated) AND m.created_at > "
                "now() - interval '14 days' LIMIT 200").fetchall()
            rows += conn.execute(
                "SELECT body FROM inbound_emails WHERE status='review' "
                "AND created_at > now() - interval '14 days' "
                "LIMIT 100").fetchall()
        return [r[0][:300] for r in rows if r[0] and r[0].strip()]
    qs = await _run(run)
    if len(qs) < 2:
        return "(not enough unanswered questions to cluster)"
    tfs = []
    for q in qs:
        tf: dict[str, int] = {}
        for t in _tok(q):
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

    def sim(i, j):
        a, an = vecs[i]
        b, bn = vecs[j]
        return sum(w * b.get(t, 0.0) for t, w in a.items()) / (an * bn)

    assigned = [False] * n
    clusters = []
    for i in range(n):
        if assigned[i]:
            continue
        members = [i]
        assigned[i] = True
        for j in range(i + 1, n):
            if not assigned[j] and sim(i, j) >= 0.22:
                members.append(j)
                assigned[j] = True
        if len(members) >= 2:
            counts: dict[str, int] = {}
            for m in members:
                for t in tfs[m]:
                    counts[t] = counts.get(t, 0) + 1
            shared = [t for t, c in counts.items()
                      if c >= (len(members) + 1) // 2]
            shared.sort(key=lambda t: -idf.get(t, 1.0))
            clusters.append((len(members), shared[:5],
                             [qs[m] for m in members[:3]]))
    clusters.sort(key=lambda c: -c[0])
    if not clusters:
        return "(no clusters — unanswered questions are all distinct)"
    out = []
    for count, terms, examples in clusters[:6]:
        out.append(f"{count} questions — topics: {', '.join(terms)}\n"
                   + "\n".join(f"  - {e[:100]}" for e in examples))
    return "\n\n".join(out)


@tool("report",
      "Record the final report for this task run (markdown). Use when the "
      "task is complete. Input: the report text.")
async def t_report(text: str) -> str:
    return f"REPORT RECORDED:\n{text[:8000]}"


def _init_schema() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(_schema)


def _create_session(sid: str, ip: str) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("INSERT INTO chat_sessions (id, ip) VALUES (%s, %s)",
                     (sid, ip))


def _record_user_message(sid: str, text: str) -> bool:
    """Insert the user message; returns False when the session is unknown."""
    with psycopg.connect(DATABASE_URL) as conn:
        row = conn.execute("SELECT id FROM chat_sessions WHERE id = %s",
                           (sid,)).fetchone()
        if not row:
            return False
        conn.execute("INSERT INTO chat_messages (session_id, role, content) "
                     "VALUES (%s, 'user', %s)", (sid, text))
        conn.execute("UPDATE chat_sessions SET message_count = "
                     "message_count + 1 WHERE id = %s", (sid,))
        return True


def _load_history(sid: str) -> list[dict]:
    with psycopg.connect(DATABASE_URL) as conn:
        rows = conn.execute(
            "SELECT role, content FROM chat_messages WHERE session_id = %s "
            "ORDER BY id DESC LIMIT %s", (sid, MAX_HISTORY)).fetchall()
        return [{"role": r[0], "content": r[1]} for r in reversed(rows)]


def _save_assistant(sid: str, answer: str, doc: str, model: str) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("INSERT INTO chat_messages (session_id, role, content,"
                     " confident, doc, model) VALUES (%s, 'assistant', %s,"
                     " TRUE, %s, %s)", (sid, answer, doc, model))


def _mark_session(sid: str, needs_review: bool, escalated: bool,
                  email: str | None, topic: str = "") -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("UPDATE chat_sessions SET needs_review = %s, "
                     "escalated = %s, email = COALESCE(%s, email), "
                     "last_topic = COALESCE(%s, last_topic) WHERE id = %s",
                     (needs_review, escalated, email, topic or None, sid))


def _transcript(sid: str) -> list[tuple] | None:
    with psycopg.connect(DATABASE_URL) as conn:
        rows = conn.execute("SELECT role, content FROM chat_messages WHERE "
                            "session_id = %s ORDER BY id", (sid,)).fetchall()
    return rows or None


def _reviews() -> list[dict]:
    with psycopg.connect(DATABASE_URL) as conn:
        sessions = conn.execute(
            "SELECT id, created_at, email, escalated, message_count "
            "FROM chat_sessions WHERE needs_review "
            "ORDER BY created_at DESC LIMIT 100").fetchall()
        out = []
        for s in sessions:
            msgs = conn.execute(
                "SELECT role, content, created_at FROM chat_messages "
                "WHERE session_id = %s ORDER BY id", (s[0],)).fetchall()
            out.append({"session_id": s[0], "created_at": str(s[1]),
                        "email": s[2], "escalated": s[3],
                        "messages": [{"role": m[0], "content": m[1],
                                      "at": str(m[2])} for m in msgs]})
        return out


def _mark_reviewed(sid: str) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("UPDATE chat_sessions SET needs_review = FALSE "
                     "WHERE id = %s", (sid,))


_schema_ok = False


async def _ensure_schema() -> None:
    global _schema_ok
    if not _schema_ok:
        await _run(_init_schema)
        _schema_ok = True


@app.on_event("startup")
async def init_db() -> None:
    if not DATABASE_URL:
        print("WARNING: DATABASE_URL unset — service cannot store chats")
        return
    try:
        await _ensure_schema()
    except Exception as e:            # DB may attach slightly after boot —
        print(f"WARNING: schema init deferred ({e!r}) — retried on first use")


# ── rate limiting (per-IP, in-memory; fronted by Cloudflare anyway) ──────

_HITS: dict[str, list[float]] = {}


def rate_ok(ip: str, limit: int, bucket: str, window_s: int = 3600) -> bool:
    now = time.time()
    key = f"{ip}|{bucket}"
    hits = [t for t in _HITS.get(key, []) if now - t < window_s]
    if len(hits) >= limit:
        _HITS[key] = hits
        return False
    hits.append(now)
    _HITS[key] = hits
    return True


def client_ip(request: Request) -> str:
    return (request.headers.get("cf-connecting-ip")
            or request.client.host if request.client else "?")


async def turnstile_ok(token: str, ip: str) -> bool:
    if not TURNSTILE_SECRET:
        print("WARNING: TURNSTILE_SECRET unset — verification SKIPPED "
              "(dev mode)")
        return True
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(
                "https://challenges.cloudflare.com/turnstile/v0/siteverify",
                data={"secret": TURNSTILE_SECRET, "response": token,
                      "remoteip": ip})
            return bool(r.json().get("success"))
    except Exception:
        return False


# ── gateway calls ─────────────────────────────────────────────────────────

RERANK_SYSTEM = """You triage customer questions for the Gatelodge help \
desk. Keyword search retrieved the CANDIDATE passages below; you pick the \
best ones.

Pick 1-3 candidate ids that answer the LATEST question (follow-ups refer \
to earlier turns — judge them in conversation context). Calibrate:
- confident=true ONLY when a passage clearly and fully answers it
- vague, borderline, or partially-covered questions: confident=false
- legal=true for requests to interpret the EULA/contract — always escalate
- legal=true also for certification or compliance-status questions \
(SOC 2, ISO, audit certifications), pricing negotiations or discounts \
(custom quotes, deal terms — NOT plain questions about published \
prices, which the passages answer), and security incident reports — \
these always go to the team, never the AI
- decision=true when the visitor asks for a RULING or eligibility — a \
refund, approval, discount, exception, entitlement — rather than \
information. Questions about what the policy IS are information \
(decision=false); asking to HAVE one applied to their case is a decision.

Reply with ONLY a JSON object, no other text:
{{"ids": [candidate ids], "confident": true|false, "legal": true|false, \
"decision": true|false, "topic": "<short topic>"}}

CANDIDATES:
{candidates}"""

ANSWER_SYSTEM = """You are the Gatekeeper, the support assistant for \
Gatelodge (by DataGateLodge, a division of XS Technologies Inc.).

Answer the customer's LATEST question using ONLY the knowledge passages \
below (the earlier conversation may add context). If the passages do not \
contain the answer, say you'll check with the team — never guess. Be \
concise, warm, and precise — 2-6 sentences. Use plain language; markdown \
bold is fine; no headings. Never invent features, prices, or policy. The \
product name is Gatelodge.

{passages}"""


def _candidates_block(cands: list[dict]) -> str:
    """Compact candidate descriptions for the triage router. Analyzed
    sections show their intent map (summary + the questions they
    answer) — a much stronger routing signal than raw text snippets;
    unanalyzed sections fall back to the snippet."""
    rows = []
    for c in cands:
        if c.get("intent"):
            it = c["intent"]
            qs = " / ".join(it["questions"][:4])
            rows.append(f'[{c["id"]}] doc={c["doc"]} · {c["section"]}\n'
                        f"    {it['summary']}\n"
                        f"    answers: {qs}")
        else:
            snippet = " ".join(c["text"].split())[:220]
            rows.append(f'[{c["id"]}] doc={c["doc"]} · {c["section"]}\n'
                        f"    {snippet}")
    return "\n".join(rows)


def _passages_block(chunks: list[dict]) -> str:
    parts = []
    for i, c in enumerate(chunks, 1):
        parts.append(f'--- PASSAGE {i} — {c["doc_title"]} · '
                     f'{c["section"]} ---\n{c["text"]}\n--- END ---')
    return "\n\n".join(parts)


async def triage(question: str, history: list[dict]) -> dict:
    """Stage 1 — hybrid retrieval (TF-IDF) + LLM rerank (semantic pick,
    calibrated confidence, follow-up aware). Fail-safe to escalate on
    any error."""
    empty = {"chunks": [], "confident": False, "legal": False,
             "decision": False, "topic": ""}
    if not GATEWAY_URL or not GATEWAY_KEY:
        return {**empty, "topic": "gateway-unconfigured"}

    # retrieval query: the question plus the previous user turn, so
    # follow-ups ("and after that?") still surface the right passages
    prev_users = [m["content"] for m in history if m["role"] == "user"]
    query = " ".join(prev_users[-2:] + [question])[-800:]
    try:
        _, cands = await _run(lambda: _retrieve(query))
    except Exception as e:
        print(f"[gatekeeper] retrieval error: {e!r}")
        return {**empty, "topic": "retrieval-error"}
    if not cands:
        return {**empty, "topic": "no-matches"}

    convo = "\n".join(f'{m["role"]}: {m["content"][:200]}'
                      for m in history[-4:])
    user_msg = (f"Conversation so far:\n{convo}\n\n"
                f"LATEST question: {question}") if convo else question
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{GATEWAY_URL}/v1/chat/completions",
                headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
                json={"model": TRIAGE_MODEL,
                      "messages": [
                          {"role": "system",
                           "content": RERANK_SYSTEM.format(
                               candidates=_candidates_block(cands))},
                          {"role": "user", "content": user_msg}],
                      "max_tokens": 600, "temperature": 0.0})
            content = r.json()["choices"][0]["message"]["content"] or ""
    except Exception as e:
        print(f"[gatekeeper] rerank error: {e!r}")
        return {**empty, "topic": "rerank-error"}
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        return {**empty, "topic": "rerank-unparseable"}
    try:
        d = json.loads(m.group(0))
        by_id = {c["id"]: c for c in cands}
        ids = [i for i in (d.get("ids") or [])
               if str(i).isdigit() and int(i) in by_id][:3]
        picked = [by_id[int(i)] for i in ids]
        return {"chunks": picked, "confident": bool(d.get("confident")),
                "legal": bool(d.get("legal")),
                "decision": bool(d.get("decision")),
                "topic": str(d.get("topic", ""))[:60]}
    except Exception:
        return {**empty, "topic": "rerank-unparseable"}


async def load_history(session_id: str) -> list[dict]:
    return await _run(lambda: _load_history(session_id))


# ── decision-capable chat: the agent decides, conversationally ────────────
# When triage detects a RULING request (refund, eligibility, exception),
# the agent extracts the policy's facts from the conversation, asks a
# natural follow-up for whatever is missing, and — only when the fact
# set is complete — a deterministic JDM policy decides. The LLM phrases
# the outcome; it never makes the call.

DECISION_FACT_HINTS = {
    "customer_tier": 'one of "standard", "premium", "enterprise"',
    "days_since_purchase": "days since the order was placed (number)",
    "order_amount_usd": "order amount in USD (number)",
    "reason": ('one of "defective", "late_delivery", "changed_mind", '
               '"duplicate_charge"'),
    "prior_refunds_90d": "refunds in the last 90 days (number, 0 if none)",
}

DECISION_EXTRACT_SYSTEM = """You extract structured facts for a \
deterministic decision policy from a support conversation. The FACTS the \
policy needs are listed below with hints. Read the ENTIRE conversation — \
facts may have been given in earlier turns.

Output ONLY JSON:
{{"facts": {{"<field>": <value>, ...}},
  "missing": ["<field>", ...],
  "ask": "<one warm follow-up question for the most important missing \
fact — empty string when nothing is missing>"}}

Rules:
- Include only values the visitor stated (or that follow directly, e.g.
  "never refunded before" → 0 for a refund-count fact).
- Correct JSON types: numbers unquoted, category values quoted.
- missing = policy facts with no value yet (empty list when complete).
- When missing is non-empty, write ONE natural question for the first
  missing fact. No lists, no preamble.

FACTS:
{schema}"""

DECISION_ANSWER_SYSTEM = """You are the Gatekeeper for Gatelodge. A \
deterministic refund policy (a rules engine, not your judgment) has \
evaluated the visitor's request against their details:

DECISION: {decision}
RULE FIRED: {rule}
POLICY: {policy}

Tell the visitor the outcome plainly and warmly in 2-4 sentences:
- State the decision clearly (approved / denied / needs human review).
- Attribute it to the refund policy applied to their details — do not
  add conditions, exceptions, or personal judgment.
- If it is a review or a denial, invite them to email {support} and the
  team will look at it personally.
- No headings, no markdown lists."""


def _decision_schema(kind: str) -> list[tuple[str, str]]:
    """(field, hint) pairs for a decision ruleset's input columns."""
    rs = _load_ruleset(kind)
    if not rs:
        return []
    try:
        g = json.loads(rs[2])
        fields = [i.get("field", "") for n in g.get("nodes", [])
                  if n.get("type") == "decisionTableNode"
                  for i in n.get("content", {}).get("inputs", [])]
        return [(f, DECISION_FACT_HINTS.get(f, "the value"))
                for f in fields if f]
    except Exception:
        return []


async def _extract_decision_facts(history: list[dict]) -> dict:
    """Accumulate the policy's facts from the whole conversation.
    Returns {facts, missing, ask}. Raises on unparseable output."""
    schema = "\n".join(f"- {f}: {h}" for f, h in _decision_schema(
        "chat-decision"))
    if not schema:
        raise RuntimeError("no chat-decision ruleset configured")
    convo = "\n".join(f'{m["role"]}: {m["content"][:400]}'
                      for m in history[-8:])
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(
            f"{GATEWAY_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
            json={"model": TRIAGE_MODEL,
                  "messages": [
                      {"role": "system",
                       "content": DECISION_EXTRACT_SYSTEM.format(
                           schema=schema)},
                      {"role": "user", "content": convo or "(empty)"}],
                  "max_tokens": 600, "temperature": 0.0})
        content = r.json()["choices"][0]["message"]["content"] or ""
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        raise RuntimeError("unparseable fact extraction")
    d = json.loads(m.group(0))
    facts = d.get("facts") or {}
    missing = [str(f) for f in (d.get("missing") or [])][:6]
    # trust the schema over the model for completeness
    known = {f for f, _ in _decision_schema("chat-decision")}
    missing = [f for f in known if f not in facts] or [
        f for f in missing if f in known]
    return {"facts": facts if isinstance(facts, dict) else {},
            "missing": missing,
            "ask": str(d.get("ask", ""))[:400]}


async def _decision_answer(question_history: list[dict], facts: dict,
                           decision: dict, policy: str) -> str:
    """Phrase a deterministic decision for the visitor (non-streaming)."""
    messages = ([{"role": "system",
                  "content": DECISION_ANSWER_SYSTEM.format(
                      decision=json.dumps(decision.get("result", {})),
                      rule=decision.get("result", {}).get("rule", ""),
                      policy=policy, support=SUPPORT_EMAIL)}]
                + question_history[-6:])
    async with httpx.AsyncClient(timeout=90) as c:
        r = await c.post(
            f"{GATEWAY_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
            json={"model": ANSWER_MODEL, "messages": messages,
                  "max_tokens": 800, "temperature": 0.3})
        data = r.json()
        if "choices" not in data:
            raise RuntimeError(f"gateway {r.status_code}")
        return (data["choices"][0]["message"]["content"] or "").strip()


# ── escalation email ──────────────────────────────────────────────────────


async def send_escalation_email(session_id: str, email: str,
                                transcript: str) -> tuple[bool, str]:
    if not RESEND_API_KEY:
        return False, "RESEND_API_KEY unset"
    sender = email or "(no email collected)"
    payload = {"from": EMAIL_FROM, "reply_to": email or SUPPORT_EMAIL,
               "to": [SUPPORT_EMAIL],
               "subject": f"Gatekeeper escalation — {session_id[:8]}",
               "text": (f"Session: {session_id}\nFrom: {sender}\n\n"
                        f"--- transcript ---\n{transcript}")}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post("https://api.resend.com/emails",
                             json=payload,
                             headers={"Authorization":
                                      f"Bearer {RESEND_API_KEY}"})
        if r.status_code in (200, 201):
            return True, r.json().get("id", "")
        return False, f"resend {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"send error: {e!r}"


# ── endpoints ─────────────────────────────────────────────────────────────


class SessionRequest(BaseModel):
    turnstile_token: str = ""


class MessageRequest(BaseModel):
    session_id: str = ""
    text: str = ""


class EscalateRequest(BaseModel):
    session_id: str = ""
    email: str = ""


class ReviewedRequest(BaseModel):
    session_id: str = ""


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "gatekeeper",
            "gateway": bool(GATEWAY_URL and GATEWAY_KEY),
            "db": bool(DATABASE_URL), "kb_docs": len(KB)}


@app.get("/api/config")
async def config():
    return {"turnstile_sitekey": TURNSTILE_SITEKEY,
            "name": "The Gatekeeper"}


@app.post("/api/chat/session")
async def create_session(body: SessionRequest, request: Request):
    ip = client_ip(request)
    if not rate_ok(ip, limit=40, bucket="session"):
        raise HTTPException(429, "hourly chat limit reached — the gate "
                                 "reopens within the hour")
    if not await turnstile_ok(body.turnstile_token, ip):
        raise HTTPException(403, "verification failed — please retry")
    if not DATABASE_URL:
        raise HTTPException(503, "storage unavailable")
    sid = uuid.uuid4().hex
    await _ensure_schema()
    await _run(lambda: _create_session(sid, ip))
    return {"session_id": sid}


@app.post("/api/chat/message")
async def chat_message(body: MessageRequest, request: Request):
    ip = client_ip(request)
    if not rate_ok(ip, limit=30, bucket="msg"):
        raise HTTPException(429, "too many messages — try again later")
    text = body.text.strip()[:MAX_MESSAGE_LEN]
    if not text:
        raise HTTPException(400, "empty message")
    if not body.session_id:
        raise HTTPException(400, "missing session id")

    # record the user message (returns False when the session is unknown)
    if not await _run(lambda: _record_user_message(body.session_id, text)):
        raise HTTPException(404, "unknown session")

    # history includes the just-recorded question as its last message
    history = await load_history(body.session_id)
    t = await triage(text, history[:-1])

    async def mark(needs_review: bool, escalated: bool = False,
                   email: str | None = None,
                   topic: str = "") -> None:
        await _run(lambda: _mark_session(body.session_id, needs_review,
                                         escalated, email, topic))

    async def sse(event: str, data: dict) -> str:
        return f"event: {event}\ndata: {json.dumps(data)}\n\n"

    async def stream():
        if t["legal"]:
            await mark(True, topic=t["topic"])
            yield await sse("escalate", {
                "reason": "legal",
                "message": ("Licensing-law questions go to the team — "
                            "can I take your email so a human follows up?")})
            return
        if t.get("decision"):
            # a RULING request: extract facts conversationally, ask for
            # what's missing, then let the deterministic policy decide
            try:
                ex = await _extract_decision_facts(history)
            except Exception as e:
                print(f"[gatekeeper] decision extract error: {e!r}")
                await mark(True, topic=t["topic"])
                yield await sse("escalate", {
                    "reason": "decision-error",
                    "message": ("I want to get this exactly right — can "
                                "I take your email? The team will follow "
                                "up.")})
                return
            if ex["missing"]:
                ask = ex["ask"] or ("Could you tell me your "
                                    f"{ex['missing'][0].replace('_', ' ')}?")
                await _run(lambda: _save_assistant(
                    body.session_id, ask, None, TRIAGE_MODEL))
                yield await sse("token", {"t": ask})
                yield await sse("done", {
                    "kind": "decision-pending",
                    "missing": ex["missing"], "topic": t["topic"]})
                return
            pol = await _run(lambda: _evaluate_policy(
                "chat-decision", ex["facts"]))
            if pol is None:
                await mark(True, topic=t["topic"])
                yield await sse("escalate", {
                    "reason": "no-policy",
                    "message": ("I want to get this exactly right — can "
                                "I take your email? The team will follow "
                                "up.")})
                return
            answer_parts: list[str] = []
            try:
                async with httpx.AsyncClient(timeout=90) as c:
                    async with c.stream(
                            "POST",
                            f"{GATEWAY_URL}/v1/chat/completions",
                            headers={"Authorization":
                                     f"Bearer {GATEWAY_KEY}"},
                            json={"model": ANSWER_MODEL,
                                  "messages": [
                                      {"role": "system", "content":
                                       DECISION_ANSWER_SYSTEM.format(
                                           decision=json.dumps(
                                               pol["result"]),
                                           rule=pol["result"].get(
                                               "rule", ""),
                                           policy=pol["policy"],
                                           support=SUPPORT_EMAIL)}]
                                  + history[-6:],
                                  "max_tokens": 800, "temperature": 0.3,
                                  "stream": True}) as r:
                        r.raise_for_status()
                        async for line in r.aiter_lines():
                            if not line.startswith("data: "):
                                continue
                            data = line[6:].strip()
                            if data == "[DONE]":
                                break
                            try:
                                delta = json.loads(data)["choices"][0].get(
                                    "delta", {})
                            except Exception:
                                continue
                            tok = delta.get("content")
                            if tok:
                                answer_parts.append(tok)
                                yield await sse("token", {"t": tok})
            except Exception as e:
                print(f"[gatekeeper] decision answer error: {e!r}")
                await mark(True, topic=t["topic"])
                yield await sse("escalate", {
                    "reason": "answer-error",
                    "message": ("Something went wrong on my side — can I "
                                "take your email? The team will follow "
                                "up.")})
                return
            answer = "".join(answer_parts)
            await _run(lambda: _save_assistant(
                body.session_id, answer, None, ANSWER_MODEL))
            yield await sse("done", {
                "kind": "decision",
                "decision": pol["result"].get("decision"),
                "rule": pol["result"].get("rule"),
                "policy": pol["policy"], "topic": t["topic"],
                "sources": []})
            return
        if not (t["confident"] and t["chunks"]):
            await mark(True, topic=t["topic"])
            yield await sse("escalate", {
                "reason": t["topic"] or "uncovered",
                "message": ("I want to get this exactly right — can I "
                            "take your email? The team will follow up.")})
            return

        picked = t["chunks"]
        sources = [{"doc": c["doc"], "section": c["section"]}
                   for c in picked]
        primary = picked[0]["doc"]
        messages = ([{"role": "system", "content": ANSWER_SYSTEM.format(
                        passages=_passages_block(picked))}]
                    + history)          # last message = the current question
        answer_parts: list[str] = []
        try:
            async with httpx.AsyncClient(timeout=90) as c:
                async with c.stream(
                        "POST", f"{GATEWAY_URL}/v1/chat/completions",
                        headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
                        json={"model": ANSWER_MODEL, "messages": messages,
                              "max_tokens": 1200, "temperature": 0.3,
                              "stream": True}) as r:
                    r.raise_for_status()
                    async for line in r.aiter_lines():
                        if not line.startswith("data: "):
                            continue
                        data = line[6:].strip()
                        if data == "[DONE]":
                            break
                        try:
                            delta = json.loads(data)["choices"][0].get(
                                "delta", {})
                        except Exception:
                            continue
                        tok = delta.get("content")
                        if tok:
                            answer_parts.append(tok)
                            yield await sse("token", {"t": tok})
        except Exception as e:
            print(f"[gatekeeper] answer error: {e!r}")
            await mark(True, topic=t["topic"])
            yield await sse("escalate", {
                "reason": "answer-error",
                "message": ("Something went wrong on my side — can I take "
                            "your email? The team will follow up.")})
            return

        answer = "".join(answer_parts)
        await _run(lambda: _save_assistant(body.session_id, answer,
                                           primary, ANSWER_MODEL))
        yield await sse("done", {"doc": primary, "topic": t["topic"],
                                 "sources": sources})

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


@app.post("/api/chat/escalate")
async def escalate(body: EscalateRequest, request: Request):
    ip = client_ip(request)
    if not rate_ok(ip, limit=10, bucket="esc"):
        raise HTTPException(429, "too many requests")
    email = body.email.strip().lower()[:200]
    if email and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        raise HTTPException(400, "a valid email address is required")
    if not body.session_id:
        raise HTTPException(400, "missing session id")

    rows = await _run(lambda: _transcript(body.session_id))
    if not rows:
        raise HTTPException(404, "unknown session")
    transcript = "\n\n".join(f"{r[0]}: {r[1]}" for r in rows)
    await _run(lambda: _mark_session(body.session_id, True, True, email))

    ok, detail = await send_escalation_email(body.session_id, email,
                                             transcript)
    print(f"[gatekeeper] escalation {body.session_id[:8]} "
          f"({email or 'no email'}): {'OK ' + detail if ok else 'FAIL ' + detail}")
    await _run_email_log(email, ok, detail,
                         subject=f"Gatekeeper escalation — "
                                 f"{body.session_id[:8]}",
                         body=transcript)
    return {"ok": True}      # identical whether or not the email delivered


# ── inbound email agent (support@ via CF Email Worker webhook) ────────────


class InboundEmail(BaseModel):
    message_id: str = ""
    from_addr: str = ""
    subject: str = ""
    text: str = ""
    raw: str = ""


def _insert_inbound(mid: str, frm: str, subject: str, body: str,
                    status: str, confident: bool, doc: str, topic: str,
                    reply: str, rule: str = "") -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(
            "INSERT INTO inbound_emails (id, from_addr, subject, body, "
            "status, confident, doc, topic, reply, rule, handled_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "CASE WHEN %s = 'review' THEN NULL ELSE now() END)",
            (mid, frm, subject, body, status, confident, doc, topic,
             reply, rule, status))


def _inbound_seen(mid: str) -> bool:
    with psycopg.connect(DATABASE_URL) as conn:
        return bool(conn.execute(
            "SELECT 1 FROM inbound_emails WHERE id = %s", (mid,)).fetchone())


def _inbound_sender_count(frm: str) -> int:
    with psycopg.connect(DATABASE_URL) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM inbound_emails WHERE from_addr = %s "
            "AND status = 'answered' AND created_at > now() - interval '1 day'",
            (frm,)).fetchone()[0]


async def answer_once(question: str, chunks: list[dict]) -> str:
    """Non-streaming answer (inbound email path)."""
    messages = ([{"role": "system", "content": ANSWER_SYSTEM.format(
                    passages=_passages_block(chunks))},
                {"role": "user", "content": question}])
    async with httpx.AsyncClient(timeout=90) as c:
        r = await c.post(
            f"{GATEWAY_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
            json={"model": ANSWER_MODEL, "messages": messages,
                  "max_tokens": 1200, "temperature": 0.3})
        return (r.json()["choices"][0]["message"]["content"] or "").strip()


async def send_reply_email(to: str, subject: str, answer: str,
                           mid: str) -> tuple[bool, str]:
    if not RESEND_API_KEY:
        return False, "RESEND_API_KEY unset"
    payload = {
        "from": EMAIL_FROM, "reply_to": SUPPORT_EMAIL, "to": [to],
        "subject": f"Re: {subject}" if subject else "Your question — Gatelodge",
        "text": (answer + "\n\n— The Gatekeeper, Gatelodge support agent\n"
                 f"(ref {mid[:12]})\n"),
        "headers": {"In-Reply-To": f"<{mid}>" if mid else None}}
    payload["headers"] = {k: v for k, v in payload["headers"].items() if v}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post("https://api.resend.com/emails", json=payload,
                             headers={"Authorization":
                                      f"Bearer {RESEND_API_KEY}"})
        if r.status_code in (200, 201):
            return True, r.json().get("id", "")
        return False, f"resend {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"send error: {e!r}"


@app.post("/api/email/inbound")
async def email_inbound(body: InboundEmail, request: Request):
    """Webhook for inbound support mail (CF Email Worker posts here).
    Confident questions get an immediate agent reply; anything else
    queues for the Lodge Office inbox."""
    if not INBOUND_KEY:
        raise HTTPException(503, "inbound email not configured")
    if request.headers.get("authorization", "") != f"Bearer {INBOUND_KEY}":
        raise HTTPException(401, "unauthorized")

    # raw MIME (worker) or structured fields (tests) — parse raw if given
    frm, subject, text = body.from_addr, body.subject, body.text
    if body.raw:
        import email as _email
        try:
            msg = _email.message_from_string(body.raw)
            frm = frm or _email.utils.parseaddr(msg.get("From", ""))[1]
            subject = subject or (msg.get("Subject", "") or "")
            parts = []
            if msg.is_multipart():
                for p in msg.walk():
                    if p.get_content_type() == "text/plain" \
                            and not p.get_filename():
                        parts.append(p.get_payload(decode=True).decode(
                            errors="replace"))
            else:
                parts.append(msg.get_payload(decode=True).decode(
                    errors="replace"))
            text = text or "\n".join(parts)
        except Exception as e:
            print(f"[gatekeeper] inbound parse error: {e!r}")

    frm = frm.strip().lower()[:200]
    subject = subject.strip()[:300]
    text = text.strip()[:8000]
    mid = (body.message_id.strip() or f"gen-{uuid.uuid4().hex}")[:200]
    if not frm or not text:
        raise HTTPException(400, "from_addr and text required")

    # loop guards: never reply to machines or ourselves
    local = frm.split("@")[0]
    if any(t in local for t in ("noreply", "no-reply", "auto", "donotreply",
                                "postmaster", "mailer-daemon")):
        return {"status": "ignored", "reason": "autoresponder"}
    if frm.endswith("@datagatelodge.com"):
        return {"status": "ignored", "reason": "self"}
    if await _run(lambda: _inbound_seen(mid)):
        return {"status": "ignored", "reason": "duplicate"}
    await _run(lambda: _contact_upsert(frm, source="inbound"))
    if await _run(lambda: _inbound_sender_count(frm)) >= INBOUND_DAILY_CAP:
        await _run(lambda: _insert_inbound(
            mid, frm, subject, text, "review", False, None,
            "daily-cap", ""))
        return {"status": "review", "reason": "daily-cap"}

    t = await triage(text, [])
    primary = t["chunks"][0]["doc"] if t["chunks"] else None
    # deterministic triage policy — thresholds live as a versioned ruleset,
    # not in code; the rule that fired is recorded for audit
    tri = await _run(lambda: _evaluate_policy("email-triage", {
        "confident": bool(t["confident"]),
        "legal": bool(t["legal"]),
        "kb_hits": len(t["chunks"])}))
    if tri is None:
        tri = {"policy": "builtin fallback", "performance": "",
               "result": {"action": "auto_reply" if (
                   t["confident"] and t["chunks"]) else "inbox",
                   "rule": "builtin"}}
    tri_rule = str(tri["result"].get("rule", ""))

    # one-shot decision request (refund/eligibility): extract the facts
    # from the email, let the deterministic policy decide. Missing facts
    # queue for the team — email has no follow-up loop.
    if t.get("decision"):
        try:
            ex = await _extract_decision_facts(
                [{"role": "user", "content": text}])
        except Exception as e:
            print(f"[gatekeeper] email decision extract error: {e!r}")
            ex = {"missing": ["facts"]}
        if ex["missing"]:
            await _run(lambda: _insert_inbound(
                mid, frm, subject, text, "review", False, None,
                "decision", json.dumps(ex["missing"])[:200],
                "decision-missing-facts"))
            return {"status": "review", "reason": "decision-missing-facts",
                    "missing": ex["missing"]}
        pol = await _run(lambda: _evaluate_policy(
            "chat-decision", ex["facts"]))
        if pol:
            try:
                answer = await _decision_answer(
                    [{"role": "user", "content": text}], ex["facts"],
                    pol["result"], pol["policy"])
            except Exception as e:
                print(f"[gatekeeper] email decision answer error: {e!r}")
                answer = ""
            if answer:
                ok, detail = await send_reply_email(frm, subject,
                                                    answer, mid)
                status = "answered" if ok else "review"
                await _run(lambda: _insert_inbound(
                    mid, frm, subject, text, status, True, None,
                    "decision", answer if ok else "",
                    pol["result"].get("rule", "")))
                await _run_email_log(
                    frm, ok, detail or "decision auto-reply",
                    kind="gatekeeper-decision-reply",
                    subject=f"Re: {subject}" if subject
                    else "Your refund request — Gatelodge",
                    body=answer)
                print(f"[gatekeeper] inbound {mid[:12]} from {frm}: "
                      f"DECISION {pol['result'].get('decision')} "
                      f"(rule {pol['result'].get('rule')})")
                return {"status": status,
                        "decision": pol["result"].get("decision"),
                        "rule": pol["result"].get("rule"),
                        "policy": pol["policy"]}
        # fall through: no policy or phrasing failed -> human review
        await _run(lambda: _insert_inbound(
            mid, frm, subject, text, "review", False, None,
            "decision", "", "decision-error"))
        return {"status": "review", "reason": "decision-error"}

    if tri["result"].get("action") != "auto_reply":
        await _run(lambda: _insert_inbound(
            mid, frm, subject, text, "review", False, primary,
            t["topic"], "", tri_rule))
        print(f"[gatekeeper] inbound queued for review "
              f"({t['topic'] or 'uncovered'} · rule {tri_rule} · "
              f"{tri['policy']})")
        return {"status": "review", "reason": t["topic"] or "uncovered",
                "rule": tri_rule, "policy": tri["policy"]}

    try:
        answer = await answer_once(text, t["chunks"])
    except Exception as e:
        print(f"[gatekeeper] inbound answer error: {e!r}")
        await _run(lambda: _insert_inbound(
            mid, frm, subject, text, "review", False, primary,
            t["topic"], "", tri_rule))
        return {"status": "review", "reason": "answer-error",
                "rule": tri_rule, "policy": tri["policy"]}

    ok, detail = await send_reply_email(frm, subject, answer, mid)
    status = "answered" if ok else "review"
    await _run(lambda: _insert_inbound(
        mid, frm, subject, text, status, True, primary, t["topic"],
        answer if ok else "", tri_rule))
    await _run_email_log(frm, ok, detail or "inbound auto-reply",
                         kind="gatekeeper-email-reply",
                         subject=f"Re: {subject}" if subject
                         else "Your question — Gatelodge",
                         body=answer)
    print(f"[gatekeeper] inbound {mid[:12]} from {frm}: "
          f"{'answered (' + primary + ')' if ok else 'QUEUED (send failed)'}"
          f" · rule {tri_rule}")
    return {"status": status, "doc": primary, "rule": tri_rule,
            "policy": tri["policy"],
            "sources": [{"doc": c["doc"], "section": c["section"]}
                        for c in t["chunks"]]}


def _log_email(to_addr: str, ok: bool, detail: str,
               kind: str = "gatekeeper-escalation",
               subject: str = "", body: str = "") -> None:
    if not DATABASE_URL:
        return
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute(
                "INSERT INTO email_log (kind, to_addr, serial, ok, detail,"
                " subject, body) VALUES (%s, %s, '', %s, %s, %s, %s)",
                (kind, to_addr or "unknown", int(bool(ok)),
                 str(detail)[:300], subject[:300], body[:20000]))
    except Exception as e:
        print(f"[gatekeeper] email_log write failed: {e!r}")


async def _run_email_log(to_addr: str, ok: bool, detail: str,
                         kind: str = "gatekeeper-escalation",
                         subject: str = "", body: str = "") -> None:
    await _run(lambda: _log_email(to_addr, ok, detail, kind, subject, body))


def _contact_upsert(email: str, name: str = "", company: str = "",
                    source: str = "") -> None:
    """Add/refresh an address-book contact (auto-capture)."""
    import datetime
    email = email.strip().lower()[:200]
    if not email or "@" not in email:
        return
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(
        timespec="seconds")
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(
            "INSERT INTO contacts (email, name, company, source, first_seen,"
            " last_seen, message_count) VALUES (%s, %s, %s, %s, %s, %s, 1) "
            "ON CONFLICT (email) DO UPDATE SET "
            "name = CASE WHEN EXCLUDED.name != '' THEN EXCLUDED.name "
            "ELSE contacts.name END, "
            "company = CASE WHEN EXCLUDED.company != '' THEN EXCLUDED.company"
            " ELSE contacts.company END, "
            "source = CASE WHEN contacts.source IN ('', 'manual') AND "
            "EXCLUDED.source != '' THEN EXCLUDED.source "
            "ELSE contacts.source END, "
            "last_seen = EXCLUDED.last_seen, "
            "message_count = contacts.message_count + 1",
            (email, name.strip()[:120], company.strip()[:160], source,
             now, now))


def _require_review_key(request: Request) -> None:
    if not REVIEW_KEY:
        raise HTTPException(503, "review not configured")
    auth = request.headers.get("authorization", "")
    if auth != f"Bearer {REVIEW_KEY}":
        raise HTTPException(401, "invalid review key")


# ── task engine (v3): goal → plan → execute → review → report ────────────

OWNER_EMAIL = os.environ.get("OWNER_EMAIL", "sales@xs.technology")
MAX_TASK_STEPS = 8

TASK_SYSTEM = """You are the Gatekeeper task agent. Complete the user's \
task by calling the provided function capabilities, one at a time.

When the task is complete, reply as plain text content (NOT a tool \
call) with ONLY a JSON object:
{{"done": true, "report": "<final report in markdown>"}}

Rules:
- At most {max_steps} capability calls; make them count.
- Chain results: use earlier outputs to build later inputs.
- Use email_draft for customer-facing mail (never email_send).
- If no capability can achieve part of the goal, say so in the report."""

REVIEW_SYSTEM = """You review a completed agent task. Given the GOAL, the \
SUCCESS CRITERIA, and the TRANSCRIPT of tool calls, judge the outcome.

Reply with ONLY a JSON object:
{{"classification": "achieved" | "partial" | "failed" | "review", \
"summary": "<2-3 sentences for the owner>"}}

- achieved: the goal was met and the criteria satisfied
- partial: some of the goal was met; say what is missing
- failed: the goal was not met
- review: the result needs a human decision before it counts"""


@app.get("/api/kb/prompts")
async def kb_prompts(request: Request):
    """The prompt templates, for the Lodge Office's security lint.
    Read-only: prompts are code — findings are fixed in the repo."""
    _require_review_key(request)
    return {"prompts": [
        {"name": "intent-analyze",
         "role": "pre-analyzes KB sections into the intent map",
         "text": INTENT_ANALYZE_SYSTEM},
        {"name": "rerank",
         "role": "triages questions: candidate pick + confidence + "
                 "legal/decision flags",
         "text": RERANK_SYSTEM},
        {"name": "answer",
         "role": "phrases customer-facing answers from passages",
         "text": ANSWER_SYSTEM},
        {"name": "decision-extract",
         "role": "extracts structured facts for a ruling",
         "text": DECISION_EXTRACT_SYSTEM},
        {"name": "decision-answer",
         "role": "phrases a deterministic decision outcome",
         "text": DECISION_ANSWER_SYSTEM},
        {"name": "task-agent",
         "role": "drives the task agent's plan/execute loop",
         "text": TASK_SYSTEM},
        {"name": "task-review",
         "role": "judges a completed task run",
         "text": REVIEW_SYSTEM},
    ]}


async def _llm_json(system: str, messages: list[dict],
                     model: str) -> str:
    """One non-streaming completion; returns raw content."""
    async with httpx.AsyncClient(timeout=90) as c:
        r = await c.post(
            f"{GATEWAY_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
            json={"model": model,
                  "messages": [{"role": "system", "content": system}]
                  + messages,
                  "max_tokens": 1200, "temperature": 0.1})
        data = r.json()
        if "choices" not in data:
            raise RuntimeError(f"gateway {r.status_code}: "
                               f"{str(data)[:300]}")
        return (data["choices"][0]["message"]["content"] or "").strip()


def _tools_spec() -> list[dict]:
    """The registry as native function-tool declarations (groq-safe:
    with tools declared, the model's native tool calls are legal)."""
    return [{"type": "function", "function": {
        "name": name,
        "description": d["description"][:600],
        "parameters": {"type": "object",
                       "properties": {"input": {
                           "type": "string",
                           "description": "text input for this capability"}},
                       "required": ["input"]}}}
        for name, d in sorted(TOOLS.items())]


async def _plan_step(messages: list[dict]) -> dict:
    """One planning turn with native function tools declared.
    Returns {"tool": ..., "input": ..., "call": <raw assistant msg>}
    or {"done": true, "report": ...} or raises."""
    async with httpx.AsyncClient(timeout=90) as c:
        r = await c.post(
            f"{GATEWAY_URL}/v1/chat/completions",
            headers={"Authorization": f"Bearer {GATEWAY_KEY}"},
            json={"model": TRIAGE_MODEL,
                  "messages": [{"role": "system", "content":
                                TASK_SYSTEM.format(
                                    max_steps=MAX_TASK_STEPS)}]
                  + messages,
                  "tools": _tools_spec(), "tool_choice": "auto",
                  "max_tokens": 1200, "temperature": 0.1})
        data = r.json()
        if "choices" not in data:
            raise RuntimeError(f"gateway {r.status_code}: "
                               f"{str(data)[:300]}")
    msg = data["choices"][0]["message"]
    # native tool call path
    for tc in (msg.get("tool_calls") or []):
        fn = tc.get("function", {})
        try:
            args = json.loads(fn.get("arguments") or "{}")
        except Exception:
            args = {}
        inp = args.get("input") or args.get("url") or \
            args.get("query") or ""
        # sanitize the assistant message for the next round — groq
        # rejects unknown fields (reasoning) and null content
        clean = {"role": "assistant", "content": msg.get("content") or "",
                 "tool_calls": [
                     {"id": tc.get("id", ""),
                      "type": "function",
                      "function": {"name": fn.get("name", ""),
                                   "arguments": fn.get("arguments")
                                   or "{}"}}]}
        return {"tool": fn.get("name", ""), "input": str(inp),
                "call_id": tc.get("id", ""), "assistant": clean}
    # text path — JSON in content (done/report, or a text tool call)
    content = (msg.get("content") or "").strip()
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        raise RuntimeError(f"unparseable agent response: {content[:200]}")
    d = json.loads(m.group(0))
    if d.get("done"):
        return {"done": True, "report": str(d.get("report", "")).strip(),
                "assistant": {"role": "assistant", "content": content}}
    return {"tool": str(d.get("tool", "")).strip(),
            "input": str(d.get("input", "")).strip(),
            "call_id": "", "assistant": {"role": "assistant",
                                         "content": content}}


def _tools_block() -> str:
    return "\n".join(f"- {name}: {d['description']}"
                     for name, d in sorted(TOOLS.items()))


def _propose_tool(capability: str, context: str) -> None:
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute(
                "INSERT INTO tool_proposals (capability, context) "
                "VALUES (%s, %s)", (capability[:200], context[:2000]))
    except Exception as e:
        print(f"[tasks] proposal write failed: {e!r}")


async def execute_task(task_id: int) -> None:
    """Run one task end-to-end: ReAct loop, review, report, notify."""
    with psycopg.connect(DATABASE_URL) as conn:
        row = conn.execute(
            "SELECT name, goal, success_criteria, inputs, notify "
            "FROM tasks WHERE id = %s", (task_id,)).fetchone()
    if not row:
        return
    name, goal, criteria, inputs, notify = row

    def start_run():
        with psycopg.connect(DATABASE_URL) as conn:
            return conn.execute(
                "INSERT INTO task_runs (task_id) VALUES (%s) "
                "RETURNING id", (task_id,)).fetchone()[0]
    run_id = await _run(start_run)
    print(f"[tasks] starting '{name}' (run {run_id})")

    def user_prompt():
        p = f"TASK: {goal}"
        if criteria:
            p += f"\n\nSUCCESS CRITERIA: {criteria}"
        if inputs:
            p += f"\n\nINPUTS / CONTEXT: {inputs}"
        return p

    messages: list[dict] = [{"role": "user", "content": user_prompt()}]
    transcript: list[str] = []
    report = ""
    drafts: list[str] = []

    for step_no in range(MAX_TASK_STEPS):
        try:
            d = await _plan_step(messages)
        except Exception as e:
            print(f"[tasks] plan error: {e!r}")
            report = f"Agent error during planning: {e!r}"
            break
        if d.get("done"):
            report = d["report"]
            break
        tool_name = d["tool"]
        tool_input = d["input"]
        call_id = d.get("call_id", "")

        # thread the assistant turn (native call or text JSON)
        messages.append(d["assistant"])

        # deterministic pre-tool gate — policy decides BEFORE execution
        gate = await _run(lambda: _evaluate_policy(
            "task-tool-gate",
            {"tool": tool_name, "notify": bool(notify)}))

        if tool_name not in TOOLS:
            output = (f"ERROR: no capability named '{tool_name}'. "
                      f"Available: {', '.join(sorted(TOOLS))}")
            status = "blocked"
            await _run(lambda: _propose_tool(
                tool_name, f"wanted during task '{name}': "
                f"{tool_input[:300]}"))
        elif gate is not None \
                and gate["result"].get("allow") is False:
            rule = gate["result"].get("rule", "")
            output = (f"BLOCKED by policy {gate['policy']} "
                      f"(rule: {rule}). The tool was not executed.")
            status = "blocked"
            print(f"[tasks] gate blocked {tool_name}: {rule}")
        else:
            try:
                output = str(await TOOLS[tool_name]["fn"](tool_input))[:6000]
                status = "ok"
            except Exception as e:
                output = f"ERROR: {e!r}"
                status = "error"
        if tool_name == "report":
            report = tool_input
        if tool_name == "email_draft" and output.startswith("DRAFT"):
            drafts.append(output)

        transcript.append(f"TOOL {tool_name} ({status}) — input: "
                          f"{tool_input[:200]}\noutput: {output[:600]}")
        if call_id:
            messages.append({"role": "tool", "tool_call_id": call_id,
                             "content": output[:4000]})
        else:
            messages.append({"role": "user",
                             "content": f"TOOL OUTPUT ({tool_name}):\n"
                                        f"{output}"})

        def save_step(sn=step_no, tn=tool_name,
                      ti=tool_input, ou=output, st=status):
            with psycopg.connect(DATABASE_URL) as conn:
                conn.execute(
                    "INSERT INTO task_steps (run_id, seq, tool, "
                    "description, input, output, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (run_id, sn, tn, "", ti[:4000], ou[:6000], st))
        await _run(save_step)

    if not report:
        report = "(no final report produced)"

    # review pass — a separate judgment against the success criteria
    classification, summary = "review", ""
    try:
        rv = await _llm_json(
            REVIEW_SYSTEM,
            [{"role": "user",
              "content": f"GOAL: {goal}\n\nSUCCESS CRITERIA: "
                         f"{criteria or '(none specified)'}\n\n"
                         f"TRANSCRIPT:\n" + "\n\n".join(transcript)
                         + f"\n\nFINAL REPORT:\n{report}"}],
            ANSWER_MODEL)
        m = re.search(r"\{.*\}", rv, re.S)
        if m:
            rd = json.loads(m.group(0))
            classification = str(rd.get("classification", "review"))
            summary = str(rd.get("summary", ""))[:1000]
            if classification not in ("achieved", "partial", "failed",
                                      "review"):
                classification = "review"
    except Exception as e:
        print(f"[tasks] review error: {e!r}")
        summary = f"Review error: {e!r}"

    # deterministic check — countable evidence the LLM review can miss:
    # every URL referenced in the goal/inputs should have been fetched
    urls_expected = len(re.findall(
        r"https?://[^\s,)\"]+", f"{goal}\n{inputs or ''}"))
    fetches_done = sum(1 for t in transcript
                       if t.startswith("TOOL web_fetch ("))
    if urls_expected and fetches_done < urls_expected:
        det_note = (f"Deterministic check: {fetches_done} of "
                    f"{urls_expected} referenced URLs were actually "
                    f"fetched.")
        summary = (det_note + " " + summary).strip()
        if classification in ("achieved", "review"):
            classification = "partial"
        print(f"[tasks] {det_note}")

        def save_det_step():
            with psycopg.connect(DATABASE_URL) as conn:
                conn.execute(
                    "INSERT INTO task_steps (run_id, seq, tool, "
                    "description, input, output, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (run_id, 900, "deterministic_check",
                     "countable evidence check", "",
                     det_note, "ok"))
        await _run(save_det_step)

    # notify the owner
    if notify:
        body = (f"Task: {name}\nOutcome: {classification}\n\n{summary}\n\n"
                f"--- report ---\n{report}\n\n--- transcript ---\n"
                + "\n\n".join(transcript)[:4000]
                + "\n\n(Lodge Office → Tasks for full detail)")
        ok, _ = await send_reply_email(
            OWNER_EMAIL, f"[task] {name} — {classification}", body,
            f"task-run-{run_id}")
        await _run_email_log(OWNER_EMAIL, ok, "task report",
                             kind="task-report",
                             subject=f"[task] {name} — {classification}",
                             body=body)

    def finish():
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute(
                "UPDATE task_runs SET status=%s, classification=%s, "
                "summary=%s, report=%s, finished_at=now() WHERE id=%s",
                (classification, classification, summary, report, run_id))
            interval = {"hourly": "1 hour", "daily": "1 day",
                        "weekly": "7 days"}.get(
                _task_schedule(task_id), None)
            conn.execute(
                "UPDATE tasks SET run_now=FALSE, last_run_at=now(), "
                "next_run_at=" + ("now() + interval '" + interval + "'"
                                  if interval else "NULL") +
                " WHERE id=%s", (task_id,))
    await _run(finish)
    print(f"[tasks] '{name}' run {run_id}: {classification}")


def _task_schedule(task_id: int) -> str:
    with psycopg.connect(DATABASE_URL) as conn:
        row = conn.execute("SELECT schedule FROM tasks WHERE id=%s",
                           (task_id,)).fetchone()
    return row[0] if row else "manual"


# ── deterministic policy engine (system rulesets, fail-open) ─────────────

def _load_ruleset(kind: str):
    """(name, version, jdm) for the enabled ruleset of this kind."""
    if not DATABASE_URL:
        return None
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            row = conn.execute(
                "SELECT name, version, jdm FROM rulesets "
                "WHERE kind=%s AND enabled ORDER BY id LIMIT 1",
                (kind,)).fetchone()
        return row
    except Exception:
        return None


def _evaluate_policy(kind: str, facts: dict) -> dict | None:
    """Evaluate a system ruleset. None = no policy configured (fail-open)."""
    rs = _load_ruleset(kind)
    if not rs:
        return None
    try:
        from zen import ZenEngine
        decision = ZenEngine().create_decision(json.loads(rs[2]))
        out = decision.evaluate(facts)
        return {"policy": f"{rs[0]} v{rs[1]}",
                "result": out.get("result", {}) if isinstance(out, dict)
                else {},
                "performance": out.get("performance", "")
                if isinstance(out, dict) else ""}
    except Exception as e:
        print(f"[tasks] policy {kind} error: {e!r}")
        return None


def _table_jdm(name: str, inputs: list[str], outputs: list[str],
               rules: list[tuple]) -> str:
    """Build a single-table JDM graph. rules = [(id, [cells...], [outs])]."""
    ins = [{"field": f, "id": f"c{i+1}", "name": f, "type": "expression"}
           for i, f in enumerate(inputs)]
    outs = [{"field": f, "id": f"o{i+1}", "name": f, "type": "expression"}
            for i, f in enumerate(outputs)]
    jrules = []
    for rid, cells, values in rules:
        r = {"_id": rid}
        for j, v in enumerate(cells):
            r[f"c{j+1}"] = v
        for j, v in enumerate(values):
            r[f"o{j+1}"] = v
        jrules.append(r)
    return json.dumps({
        "edges": [{"id": "e1", "sourceId": "n-input", "targetId": "n-table"},
                  {"id": "e2", "sourceId": "n-table", "targetId": "n-out"}],
        "nodes": [{"id": "n-input", "type": "inputNode", "name": "Facts"},
                  {"id": "n-out", "type": "outputNode", "name": "Decision"},
                  {"id": "n-table", "type": "decisionTableNode",
                   "name": name,
                   "content": {"hitPolicy": "first", "inputs": ins,
                               "outputs": outs, "rules": jrules}}]})


DEFAULT_RULESETS = [
    ("Task tool gate", "task-tool-gate",
     "Deterministic pre-tool gate for the admin task agent: every tool "
     "call is allowed or blocked by policy before execution.",
     ["tool", "notify"], ["allow", "rule"], [
         ("no-notify-email", ['== "email_send"', "== false"],
          ["false", '"no-notify-email"']),
         ("allow-all", ["", ""], ["true", '"allow-all"']),
     ]),
    ("Inbound email triage", "email-triage",
     "Deterministic triage for inbound support mail: confident KB "
     "questions auto-answer, everything else queues for review.",
     ["confident", "legal", "kb_hits"], ["action", "rule"], [
         ("legal-queue", ["", "== true", ""],
          ['"inbox"', '"legal-queue"']),
         ("no-kb", ["", "", "== 0"], ['"inbox"', '"no-kb"']),
         ("confident-faq", ["== true", "", ""],
          ['"auto_reply"', '"confident-faq"']),
         ("not-confident", ["", "", ""],
          ['"inbox"', '"not-confident"']),
     ]),
    ("Visitor refund eligibility", "chat-decision",
     "Deterministic refund policy for visitor requests (chat + email): "
     "the agent extracts the facts, this policy decides.",
     ["prior_refunds_90d", "customer_tier", "days_since_purchase",
      "order_amount_usd", "reason"], ["decision", "rule"], [
         ("guard-abuse", [">= 3", "", "", "", ""],
          ['"manual_review"', '"guard-abuse"']),
         ("ent-90d", ["", '== "enterprise"', "<= 90", "", ""],
          ['"approved"', '"ent-90d"']),
         ("std-defective", ["", '== "standard"', "<= 30", "<= 500",
                            '== "defective"'],
          ['"approved"', '"std-defective"']),
         ("premium-mindchange", ["", '== "premium"', "<= 30", "<= 200",
                                 '== "changed_mind"'],
          ['"approved"', '"premium-mindchange"']),
         ("std-late", ["", '== "standard"', "<= 14", "",
                       '== "late_delivery"'],
          ['"approved"', '"std-late"']),
         ("fallback-deny", ["", "", "", "", ""],
          ['"denied"', '"fallback-deny"']),
     ]),
]


def _sync_rulesets() -> None:
    """Seed/refresh the system policies (idempotent, by kind)."""
    with psycopg.connect(DATABASE_URL) as conn:
        for name, kind, desc, ins, outs, rules in DEFAULT_RULESETS:
            conn.execute(
                "INSERT INTO rulesets (name, kind, description, jdm) "
                "SELECT %s, %s, %s, %s WHERE NOT EXISTS "
                "(SELECT 1 FROM rulesets WHERE kind=%s)",
                (name, kind, desc, _table_jdm(name, ins, outs, rules),
                 kind))
        # dedupe guard: keep the lowest id per kind
        conn.execute(
            "DELETE FROM rulesets a USING rulesets b "
            "WHERE a.kind = b.kind AND a.id > b.id")


async def run_due_tasks() -> None:
    if not DATABASE_URL:
        return

    def due():
        with psycopg.connect(DATABASE_URL) as conn:
            return [r[0] for r in conn.execute(
                "SELECT id FROM tasks WHERE enabled AND (run_now OR "
                "(next_run_at IS NOT NULL AND next_run_at <= now())) "
                "ORDER BY id").fetchall()]
    for tid in await _run(due):
        try:
            await execute_task(tid)
        except Exception as e:
            print(f"[tasks] task {tid} crashed: {e!r}")
            try:

                def mark(tid=tid):
                    with psycopg.connect(DATABASE_URL) as conn:
                        conn.execute(
                            "UPDATE tasks SET run_now=FALSE, last_run_at="
                            "now() WHERE id=%s", (tid,))
                await _run(mark)
            except Exception:
                pass


async def _task_loop() -> None:
    while True:
        try:
            await run_due_tasks()
        except Exception as e:
            print(f"[tasks] loop error: {e!r}")
        await asyncio.sleep(30)


@app.on_event("startup")
async def start_task_runner() -> None:
    if not DATABASE_URL or os.environ.get("RUN_TASKS") == "0":
        return
    # keep the tools registry visible to the Lodge Office
    def sync_tools():
        with psycopg.connect(DATABASE_URL) as conn:
            for name, d in TOOLS.items():
                conn.execute(
                    "INSERT INTO tools (name, description, kind) "
                    "VALUES (%s, %s, 'builtin') "
                    "ON CONFLICT (name) DO UPDATE SET description=%s",
                    (name, d["description"], d["description"]))
    try:
        await _run(sync_tools)
    except Exception as e:
        print(f"[tasks] tool sync failed: {e!r}")
    # seed the deterministic policy rulesets (tool gate + email triage)
    try:
        await _run(_sync_rulesets)
    except Exception as e:
        print(f"[tasks] ruleset sync failed: {e!r}")
    # pre-analyze documents that have no intent map yet (background)
    asyncio.create_task(_seed_kb_intents())
    asyncio.create_task(_task_loop())


@app.get("/api/admin/tasks/run")
async def admin_run_tasks(request: Request):
    _require_review_key(request)
    await run_due_tasks()
    return {"ok": True}


class ReanalyzeRequest(BaseModel):
    doc: str = ""


@app.post("/api/kb/reanalyze")
async def kb_reanalyze(body: ReanalyzeRequest, request: Request):
    """Re-run the intent analysis for one doc (or all docs when doc is
    empty). Called by the Lodge Office after a KB save — the fresh map
    goes live on the next question."""
    _require_review_key(request)
    results = await analyze_doc(body.doc.strip() or None)
    return {"ok": True, "analyzed": results}


@app.get("/api/admin/reviews")
async def reviews(request: Request):
    _require_review_key(request)
    return {"reviews": await _run(_reviews)}


@app.post("/api/admin/reviewed")
async def reviewed(body: ReviewedRequest, request: Request):
    _require_review_key(request)
    if not body.session_id:
        raise HTTPException(400, "missing session id")
    await _run(lambda: _mark_reviewed(body.session_id))
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

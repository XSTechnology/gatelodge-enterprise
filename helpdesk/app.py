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
PORT = int(os.environ.get("PORT", "8100"))

MAX_HISTORY = 6            # messages of context sent to the answer model
MAX_MESSAGE_LEN = 4000

app = FastAPI(title="Gatelodge Gatekeeper", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── knowledge base ────────────────────────────────────────────────────────

KB_DIR = Path(__file__).resolve().parent / "kb"


def _load_kb() -> dict[str, dict]:
    docs = {}
    for p in sorted(KB_DIR.glob("*.md")):
        text = p.read_text(encoding="utf-8")
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


def _chunk_kb(kb: dict) -> list[dict]:
    """Every '## ' section of every doc is a retrieval chunk. The doc's
    title/description/section-heading terms are folded into the chunk's
    term counts, so doc-level and section-level queries both hit."""
    chunks: list[dict] = []
    for name, d in kb.items():
        content = d["content"]
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
        for title, lines in sections:
            text = "\n".join(lines).strip()
            if not text:
                continue
            tf: dict[str, int] = {}
            for t in _tok(f'{d["title"]} {d["summary"]} {title} {title} '
                          f"{text}"):
                tf[t] = tf.get(t, 0) + 1
            chunks.append({"id": len(chunks), "doc": name,
                           "doc_title": d["title"], "section": title,
                           "text": text, "tf": tf})
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
    chunks = _chunk_kb(kb)
    idf = _build_index(chunks)
    return chunks, _search(query, chunks, idf)


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
                  email: str | None) -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("UPDATE chat_sessions SET needs_review = %s, "
                     "escalated = %s, email = COALESCE(%s, email) "
                     "WHERE id = %s", (needs_review, escalated, email, sid))


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

Reply with ONLY a JSON object, no other text:
{{"ids": [candidate ids], "confident": true|false, "legal": true|false, \
"topic": "<short topic>"}}

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
    rows = []
    for c in cands:
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
    empty = {"chunks": [], "confident": False, "legal": False, "topic": ""}
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
                "topic": str(d.get("topic", ""))[:60]}
    except Exception:
        return {**empty, "topic": "rerank-unparseable"}


async def load_history(session_id: str) -> list[dict]:
    return await _run(lambda: _load_history(session_id))


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
                   email: str | None = None) -> None:
        await _run(lambda: _mark_session(body.session_id, needs_review,
                                         escalated, email))

    async def sse(event: str, data: dict) -> str:
        return f"event: {event}\ndata: {json.dumps(data)}\n\n"

    async def stream():
        if t["legal"]:
            await mark(True)
            yield await sse("escalate", {
                "reason": "legal",
                "message": ("Licensing-law questions go to the team — "
                            "can I take your email so a human follows up?")})
            return
        if not (t["confident"] and t["chunks"]):
            await mark(True)
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
            await mark(True)
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
    await _run_email_log(email, ok, detail)
    return {"ok": True}      # identical whether or not the email delivered


def _log_email(to_addr: str, ok: bool, detail: str) -> None:
    if not DATABASE_URL:
        return
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute(
                "INSERT INTO email_log (kind, to_addr, serial, ok, detail) "
                "VALUES ('gatekeeper-escalation', %s, '', %s, %s)",
                (to_addr or "unknown", int(bool(ok)),
                 str(detail)[:300]))
    except Exception as e:
        print(f"[gatekeeper] email_log write failed: {e!r}")


async def _run_email_log(to_addr: str, ok: bool, detail: str) -> None:
    await _run(lambda: _log_email(to_addr, ok, detail))


def _require_review_key(request: Request) -> None:
    if not REVIEW_KEY:
        raise HTTPException(503, "review not configured")
    auth = request.headers.get("authorization", "")
    if auth != f"Bearer {REVIEW_KEY}":
        raise HTTPException(401, "invalid review key")


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

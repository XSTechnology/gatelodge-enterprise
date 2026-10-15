"""Gatekeeper service test suite.

Run from helpdesk/ with the fork venv:
    ..\\.venv\\Scripts\\python.exe test_helpdesk.py

Requires a local Postgres on :5433 (docker run -d --name helpdesk-pg \
  -p 5433:5432 -e POSTGRES_PASSWORD=test -e POSTGRES_DB=helpdesk \
  postgres:16-alpine) — an in-process mock gateway is started on :8435.

Covers: healthz/config, session + SSE message flow (confident answer),
escalate paths (legal / not-answerable), escalation email collection,
review queue, reviewed, 404/400 handling.
"""
import json
import os
import sys
import threading
import time

os.environ.setdefault("DATABASE_URL",
                      "postgresql://postgres:test@127.0.0.1:5433/helpdesk")
os.environ.setdefault("GATEWAY_URL", "http://127.0.0.1:8435")
os.environ.setdefault("GATEWAY_KEY", "test-key")
os.environ.setdefault("REVIEW_KEY", "review-key")
os.environ.pop("TURNSTILE_SECRET", None)
os.environ.pop("RESEND_API_KEY", None)      # escalation email -> fail-ok

# ── mock gateway on :8435 ─────────────────────────────────────────────────
from fastapi import FastAPI, Request                      # noqa: E402
from fastapi.responses import StreamingResponse           # noqa: E402

mock = FastAPI()
MODE = {"triage": "confident"}


@mock.post("/v1/chat/completions")
async def completions(request: Request):
    body = await request.json()
    model = body.get("model")
    if model == "triage":
        if MODE["triage"] == "confident":
            d = {"doc": "presales", "answerable": True, "confident": True,
                 "legal": False, "topic": "what-is"}
        elif MODE["triage"] == "legal":
            d = {"doc": "eula-summary", "answerable": True, "confident":
                 True, "legal": True, "topic": "license-legal"}
        else:
            d = {"doc": None, "answerable": False, "confident": False,
                 "legal": False, "topic": "off-topic"}
        return {"choices": [{"message": {"content": json.dumps(d)}}]}

    async def gen():
        for tok in ("Gatelodge ", "is a ", "self-hosted ", "AI gateway."):
            yield ("data: " + json.dumps(
                {"choices": [{"delta": {"content": tok}}]}) + "\n\n")
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


import uvicorn                                             # noqa: E402
server = uvicorn.Server(uvicorn.Config(mock, host="127.0.0.1",
                                       port=8435, log_level="error"))
threading.Thread(target=server.run, daemon=True).start()
time.sleep(1.5)

import app                                                 # noqa: E402
from fastapi.testclient import TestClient                  # noqa: E402

fails = []


def check(name, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + name +
          ("" if cond else f"   {extra}"))
    if not cond:
        fails.append(name)


def sse_events(text):
    events = []
    for block in text.split("\n\n"):
        ev, data = None, None
        for line in block.strip().splitlines():
            if line.startswith("event: "):
                ev = line[7:]
            elif line.startswith("data: "):
                data = line[6:]
        if ev:
            events.append((ev, json.loads(data) if data else {}))
    return events


with TestClient(app.app) as client:
    # 1. health + config
    r = client.get("/healthz")
    check("healthz", r.status_code == 200 and r.json()["ok"], r.text)
    check("gateway configured", r.json()["gateway"] is True)
    check("db configured", r.json()["db"] is True)
    check("kb docs = builtin count",
          r.json()["kb_docs"] == len(app.KB), r.text)
    r = client.get("/api/config")
    check("config name", r.json().get("name") == "The Gatekeeper")

    # 2. session
    r = client.post("/api/chat/session", json={})
    check("session 200", r.status_code == 200, r.text)
    sid = r.json().get("session_id", "")
    check("session id", len(sid) == 32, sid)

    # 3. confident answer -> tokens + done
    MODE["triage"] = "confident"
    r = client.post("/api/chat/message",
                    json={"session_id": sid, "text": "What is Gatelodge?"})
    check("message 200", r.status_code == 200, r.text[:300])
    evs = sse_events(r.text)
    kinds = [e for e, _ in evs]
    toks = [d.get("t", "") for e, d in evs if e == "token"]
    done = next((d for e, d in evs if e == "done"), {})
    check("token events", len(toks) == 4, str(toks))
    check("answer text", "".join(toks) ==
          "Gatelodge is a self-hosted AI gateway.", str(toks))
    check("done event", "done" in kinds and done.get("doc") == "presales",
          str(evs))
    check("no escalate", "escalate" not in kinds)

    # 4. legal question -> always escalate
    MODE["triage"] = "legal"
    r = client.post("/api/chat/message",
                    json={"session_id": sid,
                          "text": "Is reverse engineering allowed?"})
    evs = sse_events(r.text)
    esc = next((d for e, d in evs if e == "escalate"), {})
    check("legal escalates", esc.get("reason") == "legal", str(evs))

    # 5. off-topic -> escalate
    MODE["triage"] = "off"
    r = client.post("/api/chat/message",
                    json={"session_id": sid, "text": "make me a pizza"})
    evs = sse_events(r.text)
    esc = next((d for e, d in evs if e == "escalate"), {})
    check("off-topic escalates", esc.get("reason") == "off-topic", str(evs))

    # 6. escalate endpoint
    r = client.post("/api/chat/escalate",
                    json={"session_id": sid, "email": "cust@example.com"})
    check("escalate ok", r.status_code == 200 and r.json()["ok"], r.text)
    r = client.post("/api/chat/escalate",
                    json={"session_id": sid, "email": "not-an-email"})
    check("escalate bad email 400", r.status_code == 400)
    r = client.post("/api/chat/escalate",
                    json={"session_id": "nope", "email": "a@b.co"})
    check("escalate unknown session 404", r.status_code == 404)

    # 7. empty message / unknown session
    r = client.post("/api/chat/message",
                    json={"session_id": sid, "text": "   "})
    check("empty message 400", r.status_code == 400)
    r = client.post("/api/chat/message",
                    json={"session_id": "zz", "text": "hi"})
    check("unknown session 404", r.status_code == 404)

    # 8. review queue
    r = client.get("/api/admin/reviews")
    check("reviews 401 without key", r.status_code == 401)
    r = client.get("/api/admin/reviews",
                   headers={"Authorization": "Bearer review-key"})
    check("reviews 200", r.status_code == 200, r.text[:200])
    revs = r.json().get("reviews", [])
    target = next((x for x in revs if x["session_id"] == sid), None)
    check("session in reviews", target is not None)
    if target:
        check("review has transcript",
              any(m["role"] == "user" for m in target["messages"]))
        check("review has email", target["email"] == "cust@example.com")

    # 9. mark reviewed
    r = client.post("/api/admin/reviewed", json={"session_id": sid},
                    headers={"Authorization": "Bearer review-key"})
    check("reviewed ok", r.status_code == 200 and r.json()["ok"])
    r = client.get("/api/admin/reviews",
                   headers={"Authorization": "Bearer review-key"})
    check("reviewed removed",
          not any(x["session_id"] == sid
                  for x in r.json().get("reviews", [])))

# 10. DB content check (messages persisted for the answered exchange)
import psycopg                                             # noqa: E402
with psycopg.connect(os.environ["DATABASE_URL"]) as c:
    rows = c.execute("SELECT role, confident, doc FROM chat_messages "
                     "WHERE session_id = %s ORDER BY id",
                     (sid,)).fetchall()
check("user+assistant stored",
      any(r[0] == "user" for r in rows)
      and any(r[0] == "assistant" and r[1] and r[2] == "presales"
              for r in rows), str(rows))

# 11. KB override merge: existing names patched, NEW names ADDED to the
#     catalog (docs created in the Lodge Office) with parsed title/desc
with psycopg.connect(os.environ["DATABASE_URL"]) as c:
    c.execute("INSERT INTO kb_overrides (name, content) VALUES (%s, %s) "
              "ON CONFLICT (name) DO UPDATE SET content = %s",
              ("channel-faq",
               "# Channel FAQ\ntitle: Channel partners — FAQ\n"
               "description: Partner trial questions\n\n## How long?\n"
               "\n90 days.",
               "# Channel FAQ\ntitle: Channel partners — FAQ\n"
               "description: Partner trial questions\n\n## How long?\n"
               "\n90 days."))
    c.execute("INSERT INTO kb_overrides (name, content) VALUES (%s, %s) "
              "ON CONFLICT (name) DO UPDATE SET content = %s",
              ("presales", "title: Overridden\n"
               "description: overridden desc\n\npatched body", 
               "title: Overridden\n"
               "description: overridden desc\n\npatched body"))
merged = app._load_kb_overrides()
check("override patches existing doc",
      merged["presales"]["title"] == "Overridden"
      and "patched body" in merged["presales"]["content"])
check("NEW doc joins catalog",
      "channel-faq" in merged
      and merged["channel-faq"]["title"] == "Channel partners — FAQ"
      and merged["channel-faq"]["summary"]
      == "Partner trial questions"
      and "90 days" in merged["channel-faq"]["content"],
      str(merged.get("channel-faq"))[:100])
check("builtin docs still present",
      len(merged) >= len(app.KB) + 1)
with psycopg.connect(os.environ["DATABASE_URL"]) as c:
    c.execute("DELETE FROM kb_overrides")

print()
if fails:
    print("FAILED:", ", ".join(fails))
    sys.exit(1)
print("ALL PASS")

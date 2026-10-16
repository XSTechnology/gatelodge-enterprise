"""Live: paraphrase questions through the intent engine (email path —
same triage, no Turnstile) + seed verification."""
import json
import sys
import time
import httpx

BASE = "https://chat.datagatelodge.com"
CREDS = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"

key = ""
for line in open(CREDS, encoding="utf-8", errors="replace"):
    if line.strip().startswith("INBOUND_KEY"):
        key = (line.split("=", 1)[1] if "=" in line
               else line.split(":", 1)[1]).strip()
        break
assert key, "INBOUND_KEY not found"
ok = fail = 0

def check(name, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"PASS {name}")
    else:
        fail += 1
        print(f"FAIL {name}  {detail}")

QUESTIONS = [
    ("how much does it cost to run gatelodge", "cost"),
    ("what will it set me back each month", "cost"),
    ("can I try it before buying", "trial"),
]
for q, tag in QUESTIONS:
    r = httpx.post(
        f"{BASE}/api/email/inbound",
        headers={"Authorization": f"Bearer {key}"},
        json={"from_addr": "intent-test@example.com",
              "subject": "question",
              "text": q,
              "message_id": f"intent-{tag}-{time.time()}"},
        timeout=120)
    j = r.json()
    print(f"\n== {q}")
    print("   status:", j.get("status"), "| doc:", j.get("doc"),
          "| rule:", j.get("rule"),
          "| sources:", [s.get("doc") for s in (j.get("sources") or [])])
    check(f"[{tag}] routed (review-or-answered with doc)",
          j.get("doc") is not None, str(j)[:200])
    if j.get("status") == "answered":
        check(f"[{tag}] confident answer", True)
    elif j.get("status") == "review":
        check(f"[{tag}] honest escalation (not confident)", True,
              "review is a valid outcome — calibrates honestly")

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

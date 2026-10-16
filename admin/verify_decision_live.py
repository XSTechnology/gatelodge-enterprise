"""Live: decision-capable agent — refund emails through the policy."""
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
assert key
ok = fail = 0

def check(name, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"PASS {name}")
    else:
        fail += 1
        print(f"FAIL {name}  {detail}")

def send(text, tag):
    r = httpx.post(
        f"{BASE}/api/email/inbound",
        headers={"Authorization": f"Bearer {key}"},
        json={"from_addr": "decision-test@example.com",
              "subject": "refund request",
              "text": text,
              "message_id": f"dec-live-{tag}-{time.time()}"},
        timeout=150)
    return r.json()

# 1. complete facts -> deterministic decision
j = send("Can I get a refund? I'm a premium customer, bought 12 days "
         "ago for $150, and I changed my mind about the purchase. I "
         "haven't requested any refunds before.", "full")
print("complete:", json.dumps(j)[:300])
check("complete facts -> decision", j.get("decision") is not None
      and j.get("rule") is not None, str(j))
check("decision is approved or denied deterministically",
      j.get("decision") in ("approved", "denied", "manual_review")
      and j.get("policy", "").startswith("Visitor refund"), str(j))

# 2. missing facts -> queued for the team (email has no follow-up loop)
j = send("Can I get a refund please? I changed my mind about my "
         "order.", "missing")
print("missing:", json.dumps(j)[:300])
check("missing facts -> human review",
      j.get("status") == "review"
      and "missing" in str(j.get("reason", "")), str(j))

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

"""Live: inbound email webhook now routes through the JDM triage policy.
Nonsense question -> not confident -> inbox queue with the fired rule."""
import json
import sys
import httpx

BASE = "https://chat.datagatelodge.com"
CREDS = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"

key = ""
for line in open(CREDS, encoding="utf-8", errors="replace"):
    if line.strip().startswith("INBOUND_KEY"):
        key = line.split("=", 1)[1].strip() or line.split(":", 1)[1].strip()
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

r = httpx.post(
    f"{BASE}/api/email/inbound",
    headers={"Authorization": f"Bearer {key}"},
    json={"from_addr": "policy-test@example.com",
          "subject": "policy engine live test",
          "text": "urple gorbind flobber mastique tonight?",
          "message_id": f"policy-test-{__import__('time').time()}"},
    timeout=90)
j = r.json()
print("status:", r.status_code, "response:", json.dumps(j)[:300])
check("webhook accepted", r.status_code == 200, r.text[:200])
check("queued for review", j.get("status") == "review", str(j))
check("policy named", "fallback" in str(j.get("policy", "")) or "triage" in str(j.get("policy", "")).lower(), str(j.get("policy")))
check("rule recorded", j.get("rule") in ("no-kb", "not-confident"), str(j.get("rule")))

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

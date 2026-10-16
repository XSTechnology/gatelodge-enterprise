"""Quick routing verification: doc assignment per question."""
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

QUESTIONS = [
    ("can I export the audit log to our SIEM", "export", "evaluation"),
    ("what happens if openai goes down", "failover",
     ("faq", "evaluation", "presales")),
    ("how is gatelodge different from litellm", "compare", "comparison"),
    ("is gatelodge soc 2 certified", "soc2", "ANY"),
    ("what is shadow ai and how do you stop it", "shadow", "governance"),
    ("can we use local models and cloud models together", "localcloud",
     "faq"),
    ("what should we ask when evaluating an ai gateway", "eval",
     "evaluation"),
    ("what is your pricing floor for the competitive playbook",
     "internal-leak", "NOT:competitive-internal"),
]

ok = fail = 0
for q, tag, want in QUESTIONS:
    try:
        r = httpx.post(
            f"{BASE}/api/email/inbound",
            headers={"Authorization": f"Bearer {key}"},
            json={"from_addr": "kb-final@example.com", "subject": "q",
                  "text": q,
                  "message_id": f"kbfin-{tag}-{time.time()}"},
            timeout=180)
        j = r.json()
    except Exception as e:
        print(f"FAIL {tag}: {type(e).__name__}")
        fail += 1
        continue
    doc = j.get("doc") or ""
    if want == "ANY":
        good = doc != "competitive-internal"
    elif isinstance(want, str) and want.startswith("NOT:"):
        good = doc != want[4:]
    elif isinstance(want, tuple):
        good = doc in want
    else:
        good = doc == want
    status = "PASS" if good else "FAIL"
    if good:
        ok += 1
    else:
        fail += 1
    print(f"{status} {tag:16} -> {doc or j.get('reason', '')[:40]}")

print(f"\n{ok} pass / {fail} fail")

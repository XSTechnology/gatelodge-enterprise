"""Live verification of intent-engine hardening (gatekeeper:19 +
lodge-office:15 + chat.js v4). Escalation behavior via the email path
(no Turnstile), Lodge Office boxes over LAN, live site assets."""
import time

import httpx

CHAT = "https://chat.datagatelodge.com"
SITE = "https://datagatelodge.com"
LO = "http://10.1.2.217:8111"
CREDS = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"
inbound = admin_key = ""
for line in open(CREDS, encoding="utf-8", errors="replace"):
    ls = line.strip()
    for want in ("INBOUND_KEY", "ADMIN_KEY"):
        if ls.startswith(want):
            v = (ls.split("=", 1)[1] if "=" in ls
                 else ls.split(":", 1)[1]).strip()
            if want == "INBOUND_KEY":
                inbound = v
            else:
                admin_key = v

out = []
ok = fail = 0


def check(name, cond, extra=""):
    global ok, fail
    out.append(f"{'PASS' if cond else 'FAIL'} {name}"
               + ("" if cond else f"   {extra}"))
    ok, fail = ok + (1 if cond else 0), fail + (0 if cond else 1)


# 1. escalation behavior through the data-driven prompt (email path)
QS = [
    ("are you soc 2 certified", "cert",
     "ESCALATE"),
    ("how much does it cost to run gatelodge", "price", "ANSWER"),
    ("can you give us a discount on 50 licenses", "discount",
     "ESCALATE"),
    ("what happens when the trial ends", "trial", "ANSWER"),
]
for q, tag, want in QS:
    try:
        r = httpx.post(
            f"{CHAT}/api/email/inbound",
            headers={"Authorization": f"Bearer {inbound}"},
            json={"from_addr": f"i2-{tag}@example.com", "subject": "q",
                  "text": q, "message_id": f"i2-{tag}-{time.time()}"},
            timeout=180)
        j = r.json()
    except Exception as e:
        check(f"{tag} ({want.lower()})", False, type(e).__name__)
        continue
    doc = j.get("doc") or ""
    rule = j.get("rule") or ""
    if want == "ESCALATE":
        good = rule.startswith("legal") or j.get("status") == "review"
        verdict = f"rule={rule} reason={str(j.get('reason'))[:24]}"
    else:
        good = bool(doc) and rule.startswith("confident")
        verdict = f"doc={doc} rule={rule}"
    check(f"{tag} ({want.lower()})", good, verdict)

# 2. Lodge Office policies screen: both new boxes
with httpx.Client(base_url=LO, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": admin_key})
    r = c.get("/policies")
    check("policies: escalation topics box live",
          "Escalation topics" in r.text
          and "Pricing negotiations" in r.text
          and "custom quotes" in r.text)
    check("policies: agent settings box live",
          "Agent settings" in r.text and "retrieval_top_k" in r.text
          and "inbound_daily_cap" in r.text)

# 3. live site: v4 asset + suggestion styles + brand fix
r = httpx.get(f"{SITE}/gatelodge.html", timeout=30)
check("site references chat.js v4", "chat.js?v=20261018" in r.text)
r = httpx.get(f"{SITE}/chat.js?v=20261018", timeout=30)
check("chat.js v4 has suggestions",
      "renderSuggestions" in r.text and "gk-sugg" in r.text)
check("chat.js brand fixed",
      "datagatelodge.solutions" not in r.text
      and "datagatelodge.com" in r.text)
r = httpx.get(f"{SITE}/styles.css", timeout=30)
check("styles carry gk-sugg", ".gk-sugg" in r.text)

out.append(f"\n{ok} pass / {fail} fail")
with open(r"C:\Users\dlang\AppData\Local\Temp\verify_i2.txt", "w",
          encoding="utf-8") as f:
    f.write("\n".join(out))
print("done")

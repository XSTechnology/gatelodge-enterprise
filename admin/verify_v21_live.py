"""Live verification of the inbound email agent + new admin screens."""
import httpx

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
LODGE = "http://10.1.2.217:8111"
CRED = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"


def cred(name):
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"{name} not found")


INBOUND_KEY = cred("INBOUND_KEY")
ADMIN_KEY = cred("ADMIN_KEY")
checks = []

with httpx.Client(headers={"user-agent": UA}, follow_redirects=True,
                  timeout=120) as c:
    # 1. gatekeeper healthy
    r = c.get("https://chat.datagatelodge.com/healthz")
    checks.append(("chat healthz", r.status_code == 200
                   and r.json()["kb_docs"] == 9))

    # 2. inbound webhook requires the key
    r = c.post("https://chat.datagatelodge.com/api/email/inbound",
               json={"from_addr": "a@b.co", "text": "hi"})
    checks.append(("inbound rejects no key", r.status_code == 401))

    # 3. confident inbound -> auto-reply (real models + real Resend send
    #    to the owner's drop address — one test email will land there)
    r = c.post("https://chat.datagatelodge.com/api/email/inbound",
               headers={"Authorization": f"Bearer {INBOUND_KEY}"},
               json={"message_id": "live-test-001",
                     "from_addr": "sales@xs.technology",
                     "subject": "Gatekeeper inbound test",
                     "text": "What happens when the 30 day trial ends?"})
    body = r.json()
    checks.append(("inbound confident -> answered",
                   r.status_code == 200 and body.get("status") == "answered",
                   r.text[:200]))
    checks.append(("answered from faq with sources",
                   body.get("doc") == "faq" and bool(body.get("sources")),
                   r.text[:200]))

    # 4. uncertain inbound -> review queue
    r = c.post("https://chat.datagatelodge.com/api/email/inbound",
               headers={"Authorization": f"Bearer {INBOUND_KEY}"},
               json={"message_id": "live-test-002",
                     "from_addr": "sales@xs.technology",
                     "subject": "Uncertain test",
                     "text": "Can you integrate with our internal HR "
                             "system and book meetings for me?"})
    checks.append(("inbound uncertain -> review",
                   r.json().get("status") == "review", r.text[:200]))

# 5. Lodge Office: inbox + analytics render with the real data
lc = httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True)
lc.post("/login", data={"key": ADMIN_KEY})
r = lc.get("/inbox")
checks.append(("inbox shows the traffic",
               r.status_code == 200
               and "sales@xs.technology" in r.text
               and "Uncertain test" in r.text
               and "Gatekeeper inbound test" in r.text))
r = lc.get("/analytics")
checks.append(("analytics renders",
               r.status_code == 200 and "answer confidence" in r.text
               and "Knowledge-base hit rates" in r.text
               and "bar-fill" in r.text))
checks.append(("analytics shows inbound stats",
               "inbound auto-replied" in r.text))

for name, ok, *extra in checks:
    print(("OK  " if ok else "FAIL") + " " + name
          + ("  — " + extra[0] if extra and not ok else ""))
print("\nLIVE " + ("VERIFIED" if all(c[1] for c in checks)
                   else "PROBLEMS FOUND"))

"""Deploy the Gatekeeper mail worker to Cloudflare and flip support@.

1. Upload mail-worker.js as the 'gatelodge-mail' worker (module syntax).
2. Set its secrets: WEBHOOK_URL + INBOUND_KEY.
3. Switch the support@ Email Routing rule from forward-to-sales to
   send-to-worker (catch-all untouched).
4. Print the final rule state.

Token comes from the keyring — never printed.
"""
import json

import httpx
import keyring

TOKEN = keyring.get_password("workshop", "CLOUDFLARE_API_TOKEN")
ACCOUNT = "728d583f1a24c4b3dc8658c4e2dac9ae"
ZONE = "c8636bf700c64992db3261d2ff010d24"          # datagatelodge.com
WORKER = "gatelodge-mail"
WEBHOOK = "https://chat.datagatelodge.com/api/email/inbound"
WORKER_JS = (r"C:\Users\dlang\Workspace\gatelodge_enterprise\helpdesk"
             r"\deploy\mail-worker.js")
CRED = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"


def cred(name):
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"{name} not found")


INBOUND_KEY = cred("INBOUND_KEY")
H = {"Authorization": f"Bearer {TOKEN}"}
BASE = "https://api.cloudflare.com/client/v4"

ok = True
with httpx.Client(timeout=90) as c:
    # 0. token sanity
    r = c.get(f"{BASE}/user/tokens/verify", headers=H)
    j = r.json()
    print("token verify:", j.get("success") and j["result"]["status"])
    ok &= bool(j.get("success"))

    # 1. upload the worker (module syntax -> multipart with metadata)
    source = open(WORKER_JS, encoding="utf-8").read()
    meta = json.dumps({"main_module": "worker.js",
                       "compatibility_date": "2024-09-01"})
    r = c.put(f"{BASE}/accounts/{ACCOUNT}/workers/scripts/{WORKER}",
              headers=H,
              files={"metadata": (None, meta, "application/json"),
                     "worker.js": ("worker.js", source,
                                   "application/javascript+module")})
    j = r.json()
    print("worker upload:", j.get("success"),
          "" if j.get("success") else json.dumps(j.get("errors"))[:300])
    ok &= bool(j.get("success"))

    # 2. secrets
    for name, text in (("WEBHOOK_URL", WEBHOOK), ("INBOUND_KEY", INBOUND_KEY)):
        r = c.put(f"{BASE}/accounts/{ACCOUNT}/workers/scripts/{WORKER}/secrets",
                  headers=H,
                  json={"name": name, "text": text, "type": "secret_text"})
        j = r.json()
        print(f"secret {name}:", j.get("success"),
              "" if j.get("success") else json.dumps(j.get("errors"))[:200])
        ok &= bool(j.get("success"))

    # 3. find + flip the support@ rule
    r = c.get(f"{BASE}/zones/{ZONE}/email/routing/rules?per_page=50",
              headers=H)
    rules = r.json().get("result", [])
    target = next((rule for rule in rules for m in rule.get("matchers", [])
                   if str(m.get("value", "")).lower()
                   == "support@datagatelodge.com"), None)
    if not target:
        print("support@ rule NOT FOUND — rules:",
              [(m.get("value"), r2.get("name")) for r2 in rules
               for m in r2.get("matchers", [])])
        ok = False
    else:
        print("current rule:", target.get("name"), target.get("actions"))
        r = c.put(f"{BASE}/zones/{ZONE}/email/routing/rules/{target['id']}",
                  headers=H,
                  json={"name": "Gatekeeper agent (support@)",
                        "enabled": True,
                        "matchers": target["matchers"],
                        "actions": [{"type": "worker", "value": [WORKER]}],
                        "priority": target.get("priority", 0)})
        j = r.json()
        print("rule flip:", j.get("success"),
              "" if j.get("success") else json.dumps(j.get("errors"))[:300])
        ok &= bool(j.get("success"))

    # 4. final state
    r = c.get(f"{BASE}/zones/{ZONE}/email/routing/rules?per_page=50",
              headers=H)
    for rule in r.json().get("result", []):
        for m in rule.get("matchers", []):
            if str(m.get("value", "")).lower() == "support@datagatelodge.com":
                print("FINAL support@ rule ->", rule.get("actions"),
                      "| enabled:", rule.get("enabled"))

print("\nDEPLOY " + ("OK" if ok else "PROBLEMS"))

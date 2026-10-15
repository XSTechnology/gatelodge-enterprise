"""Live verification of Gatekeeper v2 intelligence."""
import httpx

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
LODGE = "http://10.1.2.217:8111"
CRED = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"

checks = []


def read_key():
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.upper().startswith("ADMIN_KEY"):
            return line.split("=", 1)[-1].strip()
    raise SystemExit("ADMIN_KEY not found")


key = read_key()

with httpx.Client(headers={"user-agent": UA}, follow_redirects=True,
                  timeout=30) as c:
    # 1. public gatekeeper
    r = c.get("https://chat.datagatelodge.com/healthz")
    checks.append(("chat healthz", r.status_code == 200
                   and r.json()["kb_docs"] == 9))
    r = c.get("https://chat.datagatelodge.com/api/config")
    checks.append(("chat config", r.status_code == 200
                   and r.json().get("name") == "The Gatekeeper"))

    # 2. widget code carries citations
    r = c.get("https://datagatelodge.com/chat.js?v=20261015")
    checks.append(("chat.js citations code", r.status_code == 200
                   and "renderSources" in r.text))
    r = c.get("https://datagatelodge.com/gatelodge.html")
    checks.append(("site on ?v=20261015", "chat.js?v=20261015" in r.text
                   and "styles.css?v=20261015" in r.text))
    r = c.get("https://datagatelodge.com/styles.css?v=20261015")
    checks.append(("styles carry .gk-src", ".gk-src" in r.text))

    # 3. lodge office: gaps panel + sync button render with real data
    lc = httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True)
    lc.post("/login", data={"key": key})
    r = lc.get("/kb")
    checks.append(("kb screen renders", r.status_code == 200
                   and "New document" in r.text
                   and "Sync FAQ from site" in r.text))
    checks.append(("gap suggestions panel (real data)",
                   "Knowledge gaps" in r.text))
    r = lc.get("/kb/new", params={"title": "t", "description": "d",
                                  "content": "c"})
    checks.append(("prefilled draft page", r.status_code == 200
                   and 'value="t"' in r.text))

for name, ok in checks:
    print(("OK  " if ok else "FAIL") + " " + name)
print("\nLIVE " + ("VERIFIED" if all(o for _, o in checks)
                   else "PROBLEMS FOUND"))

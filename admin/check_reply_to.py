"""Check Reply-To via the Lodge Office emails screen + Resend API."""
import re
import subprocess

import httpx

LODGE = "http://10.1.2.217:8111"
CRED = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"


def cred(name):
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"{name} not found")


ADMIN_KEY = cred("ADMIN_KEY")

with httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": ADMIN_KEY})
    r = c.get("/emails")
    # pull the gatekeeper-email-reply rows from the rendered table
    txt = r.text
    rows = re.findall(
        r"<tr>(.*?)</tr>", txt, re.S)
    hits = [row for row in rows if "gatekeeper-email-reply" in row]
    print(f"gatekeeper-email-reply rows on screen: {len(hits)}")
    for h in hits[:3]:
        plain = re.sub(r"<[^>]+>", " | ", h)
        print("  ", " ".join(plain.split())[:180])

# resend ids look like uuid-ish strings in the detail column
ids = re.findall(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-"
                 r"[0-9a-f]{12}", txt)
print("\nresend ids visible:", ids[:5])
if ids:
    rid = ids[0]
    js = (
        "import keyring, httpx, json; "
        "k = keyring.get_password('workshop', 'RESEND_ADMIN_API_KEY'); "
        "r = httpx.get('https://api.resend.com/emails/" + rid + "', "
        "headers={'Authorization': 'Bearer ' + k}, timeout=30); "
        "d = r.json(); "
        "print(json.dumps({k: d.get(k) for k in "
        "('id','from','to','reply_to','subject')}, indent=2))"
    )
    out = subprocess.run(
        ["uv", "run", "--with", "keyring", "--with", "httpx",
         "python", "-c", js], capture_output=True, text=True,
        timeout=120, cwd=r"C:\Users\dlang\Workspace\gatelodge_enterprise")
    print("\nResend record:")
    print(out.stdout or out.stderr[-400:])

"""Push the FAQ KB doc live via the Lodge Office (kb_overrides).

Reads the ADMIN_KEY from gateway_secrets (never printed), logs in,
and POSTs /kb/save. The same file is committed in helpdesk/kb/ and
admin/kb/ for the next gatekeeper image build.
"""
import httpx

BASE = "http://10.1.2.217:8111"
CRED = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"
FAQ = r"C:\Users\dlang\Workspace\gatelodge_enterprise\helpdesk\kb\faq.md"


def read_key():
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.upper().startswith("ADMIN_KEY"):
            return line.split("=", 1)[-1].strip()
    raise SystemExit("ADMIN_KEY not found in credentials file")


key = read_key()
content = open(FAQ, encoding="utf-8").read()

with httpx.Client(base_url=BASE, timeout=30, follow_redirects=True) as c:
    r = c.post("/login", data={"key": key})
    if "lodge" not in r.url.path and r.status_code != 200:
        # login redirects to / — treat non-200 as failure
        raise SystemExit(f"login failed: {r.status_code} {r.url}")

    r = c.post("/kb/save", data={"name": "faq", "content": content})
    print("kb/save:", r.status_code, r.url)

    # confirm it's listed and readable
    r = c.get("/kb/edit", params={"name": "faq"})
    print("kb/edit:", r.status_code,
          "contains-FAQ-title" if "FAQ" in r.text else "CONTENT MISSING",
          len(r.text), "bytes")

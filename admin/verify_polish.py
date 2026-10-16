"""Live verification of portal polish (gatekeeper:20, lodge-office:16,
task-portal:7, gatelodge-licensing:6)."""
import httpx

LO = "http://10.1.2.217:8111"
CREDS = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"
admin_key = ""
for line in open(CREDS, encoding="utf-8", errors="replace"):
    if line.strip().startswith("ADMIN_KEY"):
        admin_key = (line.split("=", 1)[1] if "=" in line
                     else line.split(":", 1)[1]).strip()
        break

out = []
ok = fail = 0


def check(name, cond, extra=""):
    global ok, fail
    out.append(f"{'PASS' if cond else 'FAIL'} {name}"
               + ("" if cond else f"   {extra}"))
    ok, fail = ok + (1 if cond else 0), fail + (0 if cond else 1)


# 1. root redirects (no follow)
r = httpx.get("https://chat.datagatelodge.com/", follow_redirects=False,
              timeout=30)
check("chat root redirects to the site",
      r.status_code == 302
      and r.headers.get("location", "").startswith(
          "https://datagatelodge.com"),
      f"{r.status_code} {r.headers.get('location')}")
r = httpx.get("https://get.datagatelodge.com/", follow_redirects=False,
              timeout=30)
check("get root redirects to the download section",
      r.status_code == 302
      and r.headers.get("location", "")
      == "https://datagatelodge.com/gatelodge.html#download",
      f"{r.status_code} {r.headers.get('location')}")

# 2. Lodge Office policy editor: reorder + duplicate buttons
with httpx.Client(base_url=LO, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": admin_key})
    r = c.get("/policies")
    rid = re.search if False else None
    import re as _re
    m = _re.search(r"/policy/(\d+)", r.text)
    r = c.get(f"/policy/{m.group(1)}")
    check("lodge-office editor has reorder + duplicate",
          "moveRule" in r.text and "dupRule" in r.text
          and "rowBtns" in r.text and "duplicate rule" in r.text)

# 3. portal editor (login as demo client, then a policy page)
with httpx.Client(base_url="https://portal.datagatelodge.com",
                  timeout=30, follow_redirects=True) as c:
    r = c.post("/login", data={"key": "portal-demo-key-2026"})
    r = c.get("/policies")
    check("portal policies list renders", r.status_code == 200
          and "policy" in r.text.lower())
    m = _re.search(r"/policy/(\d+)", r.text)
    if m:
        r = c.get(f"/policy/{m.group(1)}")
        check("portal editor has reorder + duplicate",
              "moveRule" in r.text and "dupRule" in r.text
              and "rowBtns" in r.text)
    else:
        check("portal editor has reorder + duplicate", False,
              "no policy id on list")

# 4. licensing still works after the redirect addition
r = httpx.get("https://get.datagatelodge.com/api/config", timeout=30)
check("licensing config still serves", r.status_code == 200
      and "payment_link" in r.text)

out.append(f"\n{ok} pass / {fail} fail")
with open(r"C:\Users\dlang\AppData\Local\Temp\verify_p3.txt", "w",
          encoding="utf-8") as f:
    f.write("\n".join(out))
print("done")

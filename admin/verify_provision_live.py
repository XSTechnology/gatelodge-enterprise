"""Live E2E: provision a client in the Lodge Office, sign into the
portal with the generated key, pause blocks login, then clean up."""
import re
import sys
import httpx
import paramiko

LO = "http://10.1.2.217:8111"
PORTAL = "https://portal.datagatelodge.com"
CREDS = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"
PW = "@231Garton"

admin = ""
for line in open(CREDS, encoding="utf-8", errors="replace"):
    if line.strip().startswith("ADMIN_KEY"):
        admin = (line.split("=", 1)[1] if "=" in line
                 else line.split(":", 1)[1]).strip()

ok = fail = 0

def check(name, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"PASS {name}")
    else:
        fail += 1
        print(f"FAIL {name}  {detail}")

lo = httpx.Client(timeout=60, follow_redirects=False, base_url=LO)
lo.post("/login", data={"key": admin})

r = lo.get("/clients")
check("clients screen live", r.status_code == 200
      and "Create client + key" in r.text, r.status_code)

r = lo.post("/clients/new", data={
    "name": "Live Test Partner", "company": "Northwind Co",
    "email": "northwind@example.com", "brand": "Northwind Desk",
    "email_key": ""})
loc = r.headers.get("location", "")
check("client created", r.status_code == 303 and "key:+portal-" in loc, loc)
key = loc.split("key:+")[1] if "key:+" in loc else ""
print("generated key:", key[:16] + "...")
check("key format", key.startswith("portal-") and len(key) > 20, key)

# sign into the PORTAL with the provisioned key
p = httpx.Client(timeout=60, follow_redirects=False,
                 headers={"User-Agent": "Mozilla/5.0"})
r = p.post(f"{PORTAL}/login", data={"key": key})
check("portal login with provisioned key",
      r.status_code == 303 and r.headers.get("location") == "/", r.status_code)
r = p.get(f"{PORTAL}/")
check("portal branded for new client", "Northwind Desk" in r.text,
      "brand missing")

# pause the client -> the key stops working
r = lo.get("/clients")
m = re.search(r'Live Test Partner.*?name="id" value="(\d+)"',
              r.text, re.S)
cid = m.group(1) if m else ""
check("client id found in list", bool(cid), r.text[:200])
if cid:
    lo.post("/clients/toggle", data={"id": cid})
    r = p.post(f"{PORTAL}/login", data={"key": key})
    check("paused key rejected",
          r.status_code == 303
          and "/login" in r.headers.get("location", ""), r.status_code)
    lo.post("/clients/toggle", data={"id": cid})   # re-activate

# cleanup: remove the test client (direct DB — no delete by design)
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("10.1.2.217", username="coolify", password=PW, timeout=15)
stdin, stdout, _ = ssh.exec_command(
    "sudo -S -p '' docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge"
    " -d gatelodge -c \"DELETE FROM portal_clients WHERE name="
    "'Live Test Partner'\"", timeout=60)
stdin.write(PW + "\n")
out = stdout.read().decode("utf-8", "replace")
ssh.close()
check("test client cleaned up", "DELETE 1" in out, out.strip())

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

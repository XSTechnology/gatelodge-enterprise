"""Deploy the Gatekeeper stack to the VM (after the image transfer).

Brings up:
  gatekeeper-gw  — closed-edition gateway (v2.3.0), groq triage/answer
                   aliases, budget-capped helpdesk key, enterprise license
  gatekeeper     — the helpdesk service, on the same user-defined network
                   + attached to the Postgres network

Secrets are read locally (file/keyring) or from the VM (turnstile from the
licensing container env) — never printed.
"""
import json
import re
import secrets
import sys
import time
from pathlib import Path

import keyring
import paramiko

# SECURITY (review fix): SSH credentials removed from source. And ROTATE
# the server password -- git history still contains the old literal.
import os
HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = os.environ["GK_SSH_PASSWORD"]
TMP = Path(r"C:\Users\dlang\AppData\Local\Temp\workshop_"
           r"833c5d6b-9aa2-4380-a63b-7c740c69a0e5")
FORK = Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise")
MARKER = TMP / "transfer_done.txt"
OUT = TMP / "deploy_gatekeeper_out.txt"

# 0. image transfer must be DONE
if not MARKER.exists() or MARKER.read_text().strip() != "DONE":
    print("image transfer not complete:", MARKER.read_text().strip()
          if MARKER.exists() else "marker missing")
    sys.exit(2)

# 1. gather local secrets
GROQ = Path(r"C:\Users\dlang\Workspace\gateway_secrets\groq_key.txt"
            ).read_text().strip()
RESEND = None
for svc, usr in (("workshop", "RESEND_API_KEY"), ("Workshop", "RESEND_API_KEY")):
    try:
        RESEND = keyring.get_password(svc, usr)
    except Exception:
        RESEND = None
    if RESEND:
        break
if not RESEND:
    print("RESEND key missing")
    sys.exit(1)

pg_out = (TMP / "vm_postgres_out6.txt").read_text(encoding="utf-8",
                                                  errors="replace")
m = re.search(r"gatelodge:([^@\s]+)@zrqxzbetinodrkqrfd1berbq", pg_out)
if not m:
    print("postgres password not found in creation output")
    sys.exit(1)
PG_PW = m.group(1)
DB_URL = (f"postgresql://gatelodge:{PG_PW}@zrqxzbetinodrkqrfd1berbq"
          f":5432/gatelodge")

GK_KEY = "gk-" + secrets.token_urlsafe(36)
REVIEW_KEY = "rvw-" + secrets.token_urlsafe(24)

# 2. gateway config
gw_config = {
    "port": 8110, "bind": "0.0.0.0",
    "models_dir": "/app/data/models",
    "gateway_keys": [GK_KEY],
    "budget_usd_month": 25,
    "backends": {
        "groq": {
            "type": "openai",
            "base_url": "https://api.groq.com/openai/v1",
            "api_key_env": "GROQ_API_KEY",
            "models": {
                "triage": "openai/gpt-oss-20b",
                "answer": "openai/gpt-oss-120b",
            },
        },
    },
}

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()

# 3. upload files
def mkdirs(path):
    parts = path.strip("/").split("/")
    cur = ""
    for p in parts:
        cur += "/" + p
        try:
            sftp.stat(cur)
        except FileNotFoundError:
            sftp.mkdir(cur)

mkdirs("/home/coolify/gatekeeper/helpdesk/kb")
mkdirs("/home/coolify/gatekeeper/gateway-data/models")
for f in ("app.py", "requirements.txt", "Dockerfile"):
    sftp.put(str(FORK / "helpdesk" / f),
             f"/home/coolify/gatekeeper/helpdesk/{f}")
for f in sorted((FORK / "helpdesk" / "kb").glob("*.md")):
    sftp.put(str(f), f"/home/coolify/gatekeeper/helpdesk/kb/{f.name}")
sftp.put(str(TMP / "gatekeeper_license.json"),
         "/home/coolify/gatekeeper/gateway-data/license.json")
with sftp.open("/home/coolify/gatekeeper/gateway-data/gateway_config.json",
               "w") as f:
    f.write(json.dumps(gw_config, indent=2))
print("uploaded helpdesk files + license + gateway config")
sftp.close()

# 4. run the stack
SCRIPT = f"""set -e
cd /home/coolify/gatekeeper
echo "== build gatekeeper image =="
docker build -t gatekeeper:1 helpdesk > /tmp/gk_build.log 2>&1 || {{
  echo BUILD_FAILED; tail -20 /tmp/gk_build.log; exit 1; }}
echo BUILD_OK
docker network create gatehouse 2>/dev/null || true
echo "== gateway container =="
docker rm -f gatekeeper-gw 2>/dev/null || true
docker run -d --name gatekeeper-gw --restart unless-stopped \\
  --network gatehouse \\
  -p 127.0.0.1:8110:8110 \\
  -v /home/coolify/gatekeeper/gateway-data:/app/data \\
  -e GROQ_API_KEY='{GROQ}' \\
  gatelodge-enterprise:closed
sleep 6
docker logs gatekeeper-gw 2>&1 | tail -6
echo "== helpdesk container =="
docker rm -f gatekeeper 2>/dev/null || true
docker run -d --name gatekeeper -p 8105:8100 --restart unless-stopped \\
  --network gatehouse \\
  -e GATEWAY_URL=http://gatekeeper-gw:8110 \\
  -e GATEWAY_KEY='{GK_KEY}' \\
  -e DATABASE_URL='{DB_URL}' \\
  -e TURNSTILE_SECRET="$(docker inspect licensing --format '{{{{range .Config.Env}}}}{{{{println .}}}}{{{{end}}}}' | grep '^TURNSTILE_SECRET=' | cut -d= -f2-)" \\
  -e TURNSTILE_SITEKEY="$(docker inspect licensing --format '{{{{range .Config.Env}}}}{{{{println .}}}}{{{{end}}}}' | grep '^TURNSTILE_SITEKEY=' | cut -d= -f2-)" \\
  -e ALLOWED_ORIGINS='https://datagatelodge.com,https://www.datagatelodge.com,https://staging.datagatelodge.com' \\
  -e RESEND_API_KEY='{RESEND}' \\
  -e EMAIL_FROM='Gatekeeper <support@datagatelodge.com>' \\
  -e SUPPORT_EMAIL=support@datagatelodge.com \\
  -e REVIEW_KEY='{REVIEW_KEY}' \\
  gatekeeper:1
sleep 5
echo "== attach postgres network =="
PGNET=$(docker inspect zrqxzbetinodrkqrfd1berbq --format '{{{{range $k, $_ := .NetworkSettings.Networks}}}}{{{{$k}}}} {{{{end}}}}' | awk '{{print $1}}')
echo "pg network: $PGNET"
docker network connect "$PGNET" gatekeeper
echo "== health =="
echo -n "gatekeeper :8105 -> "
curl -s http://localhost:8105/healthz
echo
echo -n "gateway :8110 -> "
docker exec gatekeeper-gw python -c "
import urllib.request
r = urllib.request.Request('http://127.0.0.1:8110/healthz',
      headers={{'Authorization': 'Bearer {GK_KEY}'}})
print(urllib.request.urlopen(r, timeout=10).read().decode()[:200])
"
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s",
                                         timeout=420)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
cli.close()

result = out + ("\nSTDERR: " + err[-400:] if err.strip() else "")
print(result)
(TMP / "gatekeeper_credentials.txt").write_text(
    f"GATEWAY_KEY={GK_KEY}\nREVIEW_KEY={REVIEW_KEY}\n", encoding="utf-8")
OUT.write_text(result, encoding="utf-8")
print("\nDEPLOY_DONE (keys saved to gatekeeper_credentials.txt)")

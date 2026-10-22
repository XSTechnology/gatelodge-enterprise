"""Deploy gatekeeper:12 (policy engine: pre-tool gates + deterministic
review checks + email triage as a JDM ruleset)."""
import pathlib

import paramiko

# SECURITY (review fix): SSH credentials removed from source. And ROTATE
# the server password -- git history still contains the old literal.
import os
HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = os.environ["GK_SSH_PASSWORD"]
FORK = pathlib.Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise")

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
sftp.put(str(FORK / "helpdesk" / "app.py"),
         "/home/coolify/gatekeeper/helpdesk/app.py")
sftp.put(str(FORK / "helpdesk" / "requirements.txt"),
         "/home/coolify/gatekeeper/helpdesk/requirements.txt")
sftp.close()
print("uploaded app.py + requirements.txt")

SCRIPT = r"""set -e
envof() { docker inspect "$1" --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep "^$2=" | cut -d= -f2-; }
DB_URL=$(envof gatekeeper DATABASE_URL)
RESEND=$(envof gatekeeper RESEND_API_KEY)
REVIEW_KEY=$(envof gatekeeper REVIEW_KEY)
INBOUND_KEY=$(envof gatekeeper INBOUND_KEY)
TURNSTILE_SECRET=$(envof licensing TURNSTILE_SECRET)
TURNSTILE_SITEKEY=$(envof licensing TURNSTILE_SITEKEY)
GK_KEY=$(envof lodge-office GATEWAY_KEY)

cd /home/coolify/gatekeeper
docker build -t gatekeeper:12 helpdesk > /tmp/g12.log 2>&1 || {
  echo GK_BUILD_FAILED; tail -20 /tmp/g12.log; exit 1; }
echo BUILD_OK
docker rm -f gatekeeper > /dev/null
docker create --name gatekeeper -p 8105:8100 --restart unless-stopped \
  --network gatehouse \
  -e GATEWAY_URL=http://gatekeeper-gw:8110 \
  -e GATEWAY_KEY="$GK_KEY" \
  -e DATABASE_URL="$DB_URL" \
  -e TURNSTILE_SECRET="$TURNSTILE_SECRET" \
  -e TURNSTILE_SITEKEY="$TURNSTILE_SITEKEY" \
  -e ALLOWED_ORIGINS='https://datagatelodge.com,https://www.datagatelodge.com,https://staging.datagatelodge.com' \
  -e RESEND_API_KEY="$RESEND" \
  -e EMAIL_FROM='Gatekeeper <support@datagatelodge.com>' \
  -e SUPPORT_EMAIL=support@datagatelodge.com \
  -e REVIEW_KEY="$REVIEW_KEY" \
  -e INBOUND_KEY="$INBOUND_KEY" \
  -e OWNER_EMAIL=sales@xs.technology \
  gatekeeper:12 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 8
echo -n "healthz: "
docker exec gatekeeper python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:120])
"
echo "== zen importable =="
docker exec gatekeeper python -c "from zen import ZenEngine; print('zen OK')"
echo "== system rulesets seeded =="
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -t -A -F ' | ' -c \\
  "SELECT kind, name, version, enabled FROM rulesets ORDER BY kind"
echo "== gate evaluate smoke (should allow web_fetch) =="
docker exec gatekeeper python -c "
import json, psycopg, os
from zen import ZenEngine
conn = psycopg.connect(os.environ['DATABASE_URL'])
row = conn.execute(\\"SELECT name, version, jdm FROM rulesets WHERE kind='task-tool-gate' AND enabled\\").fetchone()
d = ZenEngine().create_decision(json.loads(row[2]))
r = d.evaluate({'tool': 'web_fetch', 'notify': False})
print(row[0], 'v' + str(row[1]), '->', json.dumps(r['result']))
"
docker ps --filter name=gatekeeper --format '{{.Names}} | {{.Image}} | {{.Status}}' | head -2
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s", timeout=600)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
print(stdout.read().decode(errors="replace"))
err = stderr.read().decode(errors="replace").strip()
if err:
    print("STDERR:", err[-300:])
cli.close()


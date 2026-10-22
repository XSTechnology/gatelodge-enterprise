"""Deploy intent-engine hardening — gatekeeper:19 + lodge-office:15.

Gatekeeper: escalation criteria as data (escalation_topics table feeds
the rerank prompt's legal list; seeded EULA/certification/pricing/
security, fallback to code defaults), agent settings as data
(retrieval_top_k, rerank_pick_max, inbound_daily_cap — read per
question, 15s cache), chat suggestions in the SSE done event (2-3
questions from the routed doc's intent map, other sections).
Lodge Office: escalation-topics + agent-settings boxes on the Policies
screen (edit/toggle/save with range validation).
Site: chat.js v4 suggestion chips + .gk-sugg styles + ?v=20261018
(served live from site/ by the :3040 origin).
"""
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
sftp.put(str(FORK / "admin" / "app.py"),
         "/home/coolify/admin/app.py")
sftp.close()
print("uploaded helpdesk/app.py + admin/app.py")

SCRIPT = r"""set -e
envof() { docker inspect "$1" --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep "^$2=" | cut -d= -f2-; }
DB_URL=$(envof gatekeeper DATABASE_URL)
RESEND=$(envof gatekeeper RESEND_API_KEY)
REVIEW_KEY=$(envof gatekeeper REVIEW_KEY)
INBOUND_KEY=$(envof gatekeeper INBOUND_KEY)
TURNSTILE_SECRET=$(envof licensing TURNSTILE_SECRET)
TURNSTILE_SITEKEY=$(envof licensing TURNSTILE_SITEKEY)
ADMIN_KEY=$(envof lodge-office ADMIN_KEY)
GK_KEY=$(envof lodge-office GATEWAY_KEY)

echo "== build lodge-office:15 =="
cd /home/coolify/admin
docker build -t lodge-office:15 . > /tmp/b15.log 2>&1 || {
  echo ADMIN_BUILD_FAILED; tail -20 /tmp/b15.log; exit 1; }
echo BUILD_OK
docker rm -f lodge-office > /dev/null
docker create --name lodge-office \
  -p 10.1.2.217:8111:8111 --restart unless-stopped \
  --network gatehouse \
  -e DATABASE_URL="$DB_URL" \
  -e ADMIN_KEY="$ADMIN_KEY" \
  -e RESEND_API_KEY="$RESEND" \
  -e SUPPORT_EMAIL=support@datagatelodge.com \
  -e GATEWAY_URL=http://gatekeeper-gw:8110 \
  -e GATEWAY_KEY="$GK_KEY" \
  -e HELPDESK_URL=http://gatekeeper:8100 \
  -e REVIEW_KEY="$REVIEW_KEY" \
  -e LICENSING_URL=http://10.1.2.217:8100 \
  -e SITE_FAQ_URL=https://datagatelodge.com/gatelodge.html \
  lodge-office:15 > /dev/null
docker network connect coolify lodge-office
docker start lodge-office > /dev/null
sleep 5
echo -n "admin healthz: "
docker exec lodge-office python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8111/healthz', timeout=10).read().decode()[:100])
"

echo "== build gatekeeper:19 =="
cd /home/coolify/gatekeeper
docker build -t gatekeeper:19 helpdesk > /tmp/g19.log 2>&1 || {
  echo GK_BUILD_FAILED; tail -20 /tmp/g19.log; exit 1; }
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
  gatekeeper:19 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 10
echo -n "gatekeeper healthz: "
docker exec gatekeeper python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:120])
"
echo "== governance data seeded =="
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -t -A \
  -c "SELECT topic FROM escalation_topics WHERE enabled ORDER BY id"
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -t -A \
  -c "SELECT key || '=' || value FROM agent_settings ORDER BY key"
echo "== suggestions engine against live intent map =="
docker exec gatekeeper python -c "
import os, sys
sys.path.insert(0, '/app')
os.environ.setdefault('DATABASE_URL', '')
import app
s = app._suggestions('faq', [])
print('faq suggestions:', s)
"
echo "== containers =="
docker ps --format '{{.Names}} | {{.Image}} | {{.Status}}' | grep -E 'gatekeeper|lodge'
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s", timeout=900)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
print(stdout.read().decode(errors="replace"))
err = stderr.read().decode(errors="replace").strip()
if err:
    print("STDERR:", err[-400:])
cli.close()

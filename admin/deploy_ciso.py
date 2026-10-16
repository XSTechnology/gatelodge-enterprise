"""Deploy the CISO messaging pass — gatekeeper:21 + lodge-office:17.

evaluation.md gains the 'Security concerns mapped to controls' section
(corrected CISO table + the API-scope boundary statement). Uploaded to
BOTH build contexts (the admin/kb staleness gotcha), then the intent
map for the doc is re-analyzed so the new section routes.
"""
import pathlib

import paramiko

HOST, USER, PW = "10.1.2.217", "coolify", "@231Garton"
FORK = pathlib.Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise")

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
sftp.put(str(FORK / "helpdesk" / "kb" / "evaluation.md"),
         "/home/coolify/gatekeeper/helpdesk/kb/evaluation.md")
sftp.put(str(FORK / "helpdesk" / "kb" / "evaluation.md"),
         "/home/coolify/admin/kb/evaluation.md")
sftp.close()
print("uploaded evaluation.md to both build contexts")

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

echo "== build gatekeeper:22 =="
cd /home/coolify/gatekeeper
docker build -t gatekeeper:22 helpdesk > /tmp/g22.log 2>&1 || {
  echo GK_BUILD_FAILED; tail -20 /tmp/g22.log; exit 1; }
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
  gatekeeper:22 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 10
echo -n "gatekeeper healthz: "
docker exec gatekeeper python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:120])
"

echo "== re-analyze evaluation.md intent map =="
docker exec gatekeeper python -c "
import os, urllib.request, json
r = urllib.request.Request('http://127.0.0.1:8100/api/kb/reanalyze',
    data=json.dumps({'doc': 'evaluation'}).encode(),
    headers={'Authorization': 'Bearer ' + os.environ['REVIEW_KEY'],
             'Content-Type': 'application/json'})
print(urllib.request.urlopen(r, timeout=180).read().decode()[:200])
"

echo "== build lodge-office:18 =="
cd /home/coolify/admin
docker build -t lodge-office:18 . > /tmp/b18.log 2>&1 || {
  echo ADMIN_BUILD_FAILED; tail -20 /tmp/b18.log; exit 1; }
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
  lodge-office:18 > /dev/null
docker network connect coolify lodge-office
docker start lodge-office > /dev/null
sleep 5
echo -n "admin healthz: "
docker exec lodge-office python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8111/healthz', timeout=10).read().decode()[:100])
"
echo "== new section in both images =="
docker exec gatekeeper grep -c "Is prompt content logged" /app/kb/evaluation.md
docker exec lodge-office grep -c "Is prompt content logged" /app/kb/evaluation.md
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

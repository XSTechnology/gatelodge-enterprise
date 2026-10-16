"""Deploy the intent engine — gatekeeper:13 + lodge-office:9.

gatekeeper: pre-analyzed KB (kb_intent), reanalyze endpoint, intent-
enriched retrieval + triage router, startup seeding.
lodge-office: KB save triggers re-analysis, intent map visible on the
edit screen + Re-analyze button (new envs: HELPDESK_URL, REVIEW_KEY).
"""
import pathlib

import paramiko

HOST, USER, PW = "10.1.2.217", "coolify", "@231Garton"
FORK = pathlib.Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise")

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
sftp.put(str(FORK / "helpdesk" / "app.py"),
         "/home/coolify/gatekeeper/helpdesk/app.py")
sftp.put(str(FORK / "helpdesk" / "requirements.txt"),
         "/home/coolify/gatekeeper/helpdesk/requirements.txt")
sftp.put(str(FORK / "admin" / "app.py"),
         "/home/coolify/admin/app.py")
sftp.close()
print("uploaded helpdesk + admin sources")

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

echo "== build lodge-office:9 =="
cd /home/coolify/admin
docker build -t lodge-office:9 . > /tmp/b9.log 2>&1 || {
  echo ADMIN_BUILD_FAILED; tail -20 /tmp/b9.log; exit 1; }
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
  lodge-office:9 > /dev/null
docker network connect coolify lodge-office
docker start lodge-office > /dev/null
sleep 5
echo -n "admin healthz: "
docker exec lodge-office python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8111/healthz', timeout=10).read().decode()[:100])
"

echo "== build gatekeeper:13 =="
cd /home/coolify/gatekeeper
docker build -t gatekeeper:13 helpdesk > /tmp/g13.log 2>&1 || {
  echo GK_BUILD_FAILED; tail -20 /tmp/g13.log; exit 1; }
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
  gatekeeper:13 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 8
echo -n "gatekeeper healthz: "
docker exec gatekeeper python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:120])
"
echo "== intent seed (background) starts on boot; give it a moment =="
sleep 20
docker logs --tail 15 gatekeeper 2>&1 | grep -E "intent|Uvicorn" | tail -8 || true
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

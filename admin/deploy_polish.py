"""Deploy portal polish — gatekeeper:20 + lodge-office:16 + task-portal:7
+ gatelodge-licensing:6.

- Policy grid editors (portal + Lodge Office): rule reorder (up/down —
  order decides first-match) + duplicate rule (values copied
  explicitly; cloneNode alone loses typed values).
- Root redirects: chat.datagatelodge.com/ -> the site (the chat lives
  in the site's widget); get.datagatelodge.com/ -> the site's download
  section (the binary download lives here, the pitch lives there).
  Both were 404s.
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
sftp.put(str(FORK / "admin" / "app.py"), "/home/coolify/admin/app.py")
for f in ("app.py", "requirements.txt", "Dockerfile", "seed.sql"):
    sftp.put(str(FORK / "portal" / f), f"/home/coolify/portal/{f}")
for f in ("app.py", "requirements.txt", "Dockerfile"):
    sftp.put(str(FORK / "licensing" / f), f"/home/coolify/licensing/{f}")
sftp.close()
print("uploaded gatekeeper + admin + portal + licensing sources")

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
LIC_ADMIN_KEY=$(envof licensing ADMIN_KEY)
STRIPE_WH=$(envof licensing STRIPE_WEBHOOK_SECRET)

echo "== build gatekeeper:20 =="
cd /home/coolify/gatekeeper
docker build -t gatekeeper:20 helpdesk > /tmp/g20.log 2>&1 || {
  echo GK_BUILD_FAILED; tail -20 /tmp/g20.log; exit 1; }
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
  gatekeeper:20 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 8
echo -n "gatekeeper healthz: "
docker exec gatekeeper python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:120])
"

echo "== build lodge-office:16 =="
cd /home/coolify/admin
docker build -t lodge-office:16 . > /tmp/b16.log 2>&1 || {
  echo ADMIN_BUILD_FAILED; tail -20 /tmp/b16.log; exit 1; }
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
  lodge-office:16 > /dev/null
docker network connect coolify lodge-office
docker start lodge-office > /dev/null
sleep 5
echo -n "admin healthz: "
docker exec lodge-office python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8111/healthz', timeout=10).read().decode()[:100])
"

echo "== build task-portal:7 =="
cd /home/coolify/portal
docker build -t task-portal:7 . > /tmp/portal7.log 2>&1 || {
  echo PORTAL_BUILD_FAILED; tail -25 /tmp/portal7.log; exit 1; }
echo BUILD_OK
docker rm -f task-portal > /dev/null
docker create --name task-portal -p 8106:8106 --restart unless-stopped \
  --network gatehouse \
  -e DATABASE_URL="$DB_URL" \
  -e GATEWAY_URL=http://gatekeeper-gw:8110 \
  -e GATEWAY_KEY="$GK_KEY" \
  task-portal:7 > /dev/null
docker network connect coolify task-portal
docker start task-portal > /dev/null
sleep 8
echo -n "portal healthz: "
docker exec task-portal python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8106/healthz', timeout=10).read().decode()[:120])
"

echo "== build gatelodge-licensing:6 =="
cd /home/coolify/licensing
docker build -t gatelodge-licensing:6 . > /tmp/l6.log 2>&1 || {
  echo LIC_BUILD_FAILED; tail -20 /tmp/l6.log; exit 1; }
echo BUILD_OK
docker rm -f licensing > /dev/null
docker create --name licensing -p 8100:8100 --restart unless-stopped \
  -v licensing-data:/data \
  -v /home/coolify/licensing/binary:/app/binary:ro \
  -v /home/coolify/licensing/keys:/app/keys:ro \
  -e TURNSTILE_SECRET="$TURNSTILE_SECRET" \
  -e TURNSTILE_SITEKEY="$TURNSTILE_SITEKEY" \
  -e ALLOWED_ORIGINS='https://datagatelodge.com,https://www.datagatelodge.com,https://staging.datagatelodge.com' \
  -e BINARY_PATH=/app/binary/gatelodge-community.exe \
  -e PAYMENT_LINK=https://buy.stripe.com/test_14AaEY0hrdVmbwegqR7g400 \
  -e STRIPE_WEBHOOK_SECRET="$STRIPE_WH" \
  -e LICENSE_PRIVATE_KEY=/app/keys/license_signing_key.pem \
  -e EMAIL_PROVIDER=resend \
  -e RESEND_API_KEY="$RESEND" \
  -e DATABASE_URL="$DB_URL" \
  -e ADMIN_KEY="$LIC_ADMIN_KEY" \
  gatelodge-licensing:6 > /dev/null
docker network connect coolify licensing
docker start licensing > /dev/null
sleep 6
echo -n "licensing healthz: "
docker exec licensing python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:100])
"

echo "== root redirects =="
echo -n "chat root: "
docker exec gatekeeper python -c "
import urllib.request
r = urllib.request.urlopen('http://127.0.0.1:8100/', timeout=10)
print(r.status, r.url)
" || true
echo -n "get root: "
docker exec licensing python -c "
import urllib.request
r = urllib.request.urlopen('http://127.0.0.1:8100/', timeout=10)
print(r.status, r.url)
" || true
echo "== containers =="
docker ps --format '{{.Names}} | {{.Image}} | {{.Status}}' | grep -E 'gatekeeper|lodge|portal|licensing'
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

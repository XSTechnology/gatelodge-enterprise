"""Deploy the mail room — lodge-office:6 + gatekeeper:9 +
gatelodge-licensing:5 (contacts + email content logging everywhere)."""
import pathlib

import paramiko

HOST, USER, PW = "10.1.2.217", "coolify", "@231Garton"
FORK = pathlib.Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise")

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()


def mkdirs(path):
    parts = path.strip("/").split("/")
    cur = ""
    for p in parts:
        cur += "/" + p
        try:
            sftp.stat(cur)
        except FileNotFoundError:
            sftp.mkdir(cur)


mkdirs("/home/coolify/admin/kb")
for f in ("app.py", "requirements.txt", "Dockerfile"):
    sftp.put(str(FORK / "admin" / f), f"/home/coolify/admin/{f}")
for f in sorted((FORK / "admin" / "kb").glob("*.md")):
    sftp.put(str(f), f"/home/coolify/admin/kb/{f.name}")

mkdirs("/home/coolify/gatekeeper/helpdesk/kb")
for f in ("app.py", "requirements.txt", "Dockerfile"):
    sftp.put(str(FORK / "helpdesk" / f),
             f"/home/coolify/gatekeeper/helpdesk/{f}")
for f in sorted((FORK / "helpdesk" / "kb").glob("*.md")):
    sftp.put(str(f), f"/home/coolify/gatekeeper/helpdesk/kb/{f.name}")

mkdirs("/home/coolify/licensing")
for f in ("app.py", "requirements.txt", "Dockerfile"):
    sftp.put(str(FORK / "licensing" / f), f"/home/coolify/licensing/{f}")
print("uploaded admin + gatekeeper + licensing sources")
sftp.close()

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

echo "== build lodge-office:6 =="
cd /home/coolify/admin
docker build -t lodge-office:6 . > /tmp/b6.log 2>&1 || {
  echo ADMIN_BUILD_FAILED; tail -20 /tmp/b6.log; exit 1; }
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
  -e LICENSING_URL=http://10.1.2.217:8100 \
  -e SITE_FAQ_URL=https://datagatelodge.com/gatelodge.html \
  lodge-office:6 > /dev/null
docker network connect coolify lodge-office
docker start lodge-office > /dev/null
sleep 5
echo -n "admin healthz: "
docker exec lodge-office python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8111/healthz', timeout=10).read().decode()[:100])
"

echo "== build gatekeeper:9 =="
cd /home/coolify/gatekeeper
docker build -t gatekeeper:9 helpdesk > /tmp/g9.log 2>&1 || {
  echo GK_BUILD_FAILED; tail -20 /tmp/g9.log; exit 1; }
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
  gatekeeper:9 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 6
echo -n "gatekeeper healthz: "
docker exec gatekeeper python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:120])
"

echo "== build gatelodge-licensing:5 =="
cd /home/coolify/licensing
docker build -t gatelodge-licensing:5 . > /tmp/l5.log 2>&1 || {
  echo LIC_BUILD_FAILED; tail -20 /tmp/l5.log; exit 1; }
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
  gatelodge-licensing:5 > /dev/null
docker network connect coolify licensing
docker start licensing > /dev/null
sleep 6
echo -n "licensing healthz: "
docker exec licensing python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:100])
"

echo "== mail room schema check (contacts table live) =="
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -t -A -c \
  "SELECT COUNT(*) FROM contacts"
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -t -A -c \
  "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='email_log' AND column_name IN ('subject','body')"

echo "== containers =="
docker ps --format '{{.Names}} | {{.Image}} | {{.Status}}' | grep -E 'lodge|gatekeeper|licensing|zrqx'
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s", timeout=900)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
cli.close()
print(out)
if err.strip():
    print("STDERR:", err[-500:])

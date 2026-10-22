"""Deploy the support playbook expansion — gatekeeper:16 + lodge-office:12.

what changed:
- helpdesk/kb/support-internal.md: diagnostic ladder (User -> Auth ->
  Context -> Alias -> Policy -> Budget -> Routing -> Provider -> Audit,
  adapted to the actual product) + quick reference card. Internal doc —
  never served to customers, so no re-analysis needed.
- admin/kb/ was STALE on the VM (9 docs — the knowledge expansion
  upload skipped it, so the live Lodge Office KB screen missed
  evaluation/comparison/both internal docs + extended faq/governance).
  This deploy syncs all 13 docs into the admin build context.
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
# gatekeeper: only the changed doc
sftp.put(str(FORK / "helpdesk" / "kb" / "support-internal.md"),
         "/home/coolify/gatekeeper/helpdesk/kb/support-internal.md")
# lodge office: full sync of the kb seed set (was 9 docs, now 13)
for kb in sorted((FORK / "helpdesk" / "kb").glob("*.md")):
    sftp.put(str(kb), f"/home/coolify/admin/kb/{kb.name}")
print("uploaded: support-internal.md -> gatekeeper kb, 13 docs -> admin kb")
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

echo "== build lodge-office:12 =="
cd /home/coolify/admin
docker build -t lodge-office:12 . > /tmp/b12.log 2>&1 || {
  echo ADMIN_BUILD_FAILED; tail -20 /tmp/b12.log; exit 1; }
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
  lodge-office:12 > /dev/null
docker network connect coolify lodge-office
docker start lodge-office > /dev/null
sleep 5
echo -n "admin healthz: "
docker exec lodge-office python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8111/healthz', timeout=10).read().decode()[:100])
"
echo -n "admin kb docs: "
docker exec lodge-office sh -c 'ls /app/kb/*.md | wc -l'

echo "== build gatekeeper:16 =="
cd /home/coolify/gatekeeper
docker build -t gatekeeper:16 helpdesk > /tmp/g16.log 2>&1 || {
  echo GK_BUILD_FAILED; tail -20 /tmp/g16.log; exit 1; }
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
  gatekeeper:16 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 8
echo -n "gatekeeper healthz: "
docker exec gatekeeper python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:120])
"
echo "== playbook content live in both containers =="
docker exec lodge-office grep -c "diagnostic ladder" /app/kb/support-internal.md
docker exec gatekeeper grep -c "diagnostic ladder" /app/kb/support-internal.md
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

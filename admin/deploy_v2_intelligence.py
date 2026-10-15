"""Deploy Gatekeeper v2 intelligence — lodge-office:3 + gatekeeper:7.

- gatekeeper: hybrid retrieval (TF-IDF chunks + LLM rerank), multi-doc
  passages, calibrated confidence, follow-up context, cited sources.
- lodge office: knowledge-gap suggestions + FAQ site→KB sync.
Same recipe as deploy_kb_crud.py (secrets from running containers).
"""
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
print("uploaded admin + gatekeeper sources")
sftp.close()

SCRIPT = r"""set -e
envof() { docker inspect "$1" --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep "^$2=" | cut -d= -f2-; }

DB_URL=$(envof gatekeeper DATABASE_URL)
RESEND=$(envof gatekeeper RESEND_API_KEY)
REVIEW_KEY=$(envof gatekeeper REVIEW_KEY)
TURNSTILE_SECRET=$(envof licensing TURNSTILE_SECRET)
TURNSTILE_SITEKEY=$(envof licensing TURNSTILE_SITEKEY)
ADMIN_KEY=$(envof lodge-office ADMIN_KEY)
GK_KEY=$(envof lodge-office GATEWAY_KEY)

echo "== build lodge-office:3 =="
cd /home/coolify/admin
docker build -t lodge-office:3 . > /tmp/admin_build3.log 2>&1 || {
  echo ADMIN_BUILD_FAILED; tail -20 /tmp/admin_build3.log; exit 1; }
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
  lodge-office:3 > /dev/null
docker network connect coolify lodge-office
docker start lodge-office > /dev/null
sleep 5
echo -n "admin healthz: "
docker exec lodge-office python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8111/healthz', timeout=10).read().decode()[:100])
"

echo "== build gatekeeper:7 =="
cd /home/coolify/gatekeeper
docker build -t gatekeeper:7 helpdesk > /tmp/gk_build7.log 2>&1 || {
  echo GK_BUILD_FAILED; tail -20 /tmp/gk_build7.log; exit 1; }
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
  gatekeeper:7 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 6
echo -n "gatekeeper healthz: "
docker exec gatekeeper python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:120])
"

echo "== retrieval smoke (inside the container) =="
docker exec -w /app gatekeeper python -c "
import sys; sys.path.insert(0, '/app')
import app
chunks, cands = app._retrieve('what happens when the trial ends')
print('chunks:', len(chunks), 'candidates:', [c['doc'] for c in cands[:3]])
chunks, cands = app._retrieve('purple elephant zebra quantum')
print('nonsense candidates:', [c['doc'] for c in cands[:3]])
"

echo "== containers =="
docker ps --format '{{.Names}} | {{.Image}} | {{.Status}}' | grep -E 'lodge|gatekeeper|licensing|zrqx'
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s", timeout=600)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
cli.close()
print(out)
if err.strip():
    print("STDERR:", err[-400:])

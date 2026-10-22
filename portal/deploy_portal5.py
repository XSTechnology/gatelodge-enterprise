"""Deploy task-portal:5 ??? decision policy pilot (zen-engine rulesets)."""
import paramiko

# SECURITY (review fix): SSH credentials removed from source. And ROTATE
# the server password -- git history still contains the old literal.
import os
HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = os.environ["GK_SSH_PASSWORD"]
FORK = r"C:\Users\dlang\Workspace\gatelodge_enterprise"

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)

sftp = cli.open_sftp()
for f in ("app.py", "requirements.txt", "Dockerfile", "seed.sql"):
    sftp.put(rf"{FORK}\portal\{f}", f"/home/coolify/portal/{f}")
print("sources uploaded")
sftp.close()

SCRIPT = r"""set -e
envof() { docker inspect "$1" --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep "^$2=" | cut -d= -f2-; }
DB_URL=$(envof gatekeeper DATABASE_URL)
GK_KEY=$(envof lodge-office GATEWAY_KEY)

cd /home/coolify/portal
echo "== build task-portal:5 =="
docker build -t task-portal:5 . > /tmp/portal5.log 2>&1 || {
  echo PORTAL_BUILD_FAILED; tail -25 /tmp/portal5.log; exit 1; }
echo BUILD_OK

echo "== start container FIRST (schema init on startup) =="
docker rm -f task-portal > /dev/null 2>&1 || true
docker create --name task-portal -p 8106:8106 --restart unless-stopped \
  --network gatehouse \
  -e DATABASE_URL="$DB_URL" \
  -e GATEWAY_URL=http://gatekeeper-gw:8110 \
  -e GATEWAY_KEY="$GK_KEY" \
  task-portal:5 > /dev/null
docker network connect coolify task-portal
docker start task-portal > /dev/null
sleep 8

echo "== dedupe templates (seed batch has no unique constraint) =="
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -c \
  "DELETE FROM portal_templates a USING portal_templates b WHERE a.id > b.id AND a.name = b.name"

echo "== seed ruleset + decision template (idempotent) =="
docker cp /home/coolify/portal/seed.sql zrqxzbetinodrkqrfd1berbq:/tmp/
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -f /tmp/seed.sql
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -t -A -c \
  "SELECT 'ruleset: ' || name || ' v' || version || ' (' || length(jdm) || ' chars)' FROM portal_rulesets"
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -t -A -c \
  "SELECT 'template: ' || name || ' -> ruleset ' || coalesce(ruleset_id::text, '-') FROM portal_templates ORDER BY id"

echo -n "healthz: "
docker exec task-portal python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8106/healthz', timeout=10).read().decode()[:120])
"
echo "== zen-engine importable in container =="
docker exec task-portal python -c "from zen import ZenEngine; print('zen OK')"
echo "== schema columns applied =="
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -t -A -c \
  "SELECT column_name FROM information_schema.columns WHERE table_name='portal_runs' ORDER BY ordinal_position"
echo "== containers =="
docker ps --format '{{.Names}} | {{.Image}} | {{.Status}}' | grep -E 'portal|gatekeeper'
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s", timeout=600)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
print(stdout.read().decode(errors="replace"))
err = stderr.read().decode(errors="replace").strip()
if err:
    print("STDERR:", err[-400:])
cli.close()


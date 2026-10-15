"""Redeploy portal as task-portal:3 (launch sets next_run_at + run-again)."""
import paramiko

HOST, USER, PW = "10.1.2.217", "coolify", "@231Garton"
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
echo "== build task-portal:3 =="
docker build -t task-portal:3 . > /tmp/portal3.log 2>&1 || {
  echo PORTAL_BUILD_FAILED; tail -25 /tmp/portal3.log; exit 1; }
echo BUILD_OK

# clean up the empty-URL E2E task from the earlier test run
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -c \
  "DELETE FROM portal_tasks WHERE name='E2E verification task'" || true

docker rm -f task-portal > /dev/null 2>&1 || true
docker create --name task-portal -p 8106:8106 --restart unless-stopped \
  --network gatehouse \
  -e DATABASE_URL="$DB_URL" \
  -e GATEWAY_URL=http://gatekeeper-gw:8110 \
  -e GATEWAY_KEY="$GK_KEY" \
  task-portal:3 > /dev/null
docker network connect coolify task-portal
docker start task-portal > /dev/null
sleep 5
echo -n "healthz: "
docker exec task-portal python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8106/healthz', timeout=10).read().decode()[:120])
"
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

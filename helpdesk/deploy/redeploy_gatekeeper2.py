"""Redeploy gatekeeper:2 (20/hr session limit) — env carried from the
running container; restart clears the in-memory rate buckets."""
import re
from pathlib import Path

import paramiko

# SECURITY (review fix): SSH credentials removed from source. And ROTATE
# the server password -- git history still contains the old literal.
import os
HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = os.environ["GK_SSH_PASSWORD"]
TMP = Path(r"C:\Users\dlang\AppData\Local\Temp\workshop_"
           r"833c5d6b-9aa2-4380-a63b-7c740c69a0e5")
FORK = Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise")

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
sftp.put(str(FORK / "helpdesk" / "app.py"),
         "/home/coolify/gatekeeper/helpdesk/app.py")
sftp.close()
print("uploaded app.py")

SCRIPT = """set -e
cd /home/coolify/gatekeeper
docker build -t gatekeeper:2 helpdesk > /tmp/gk_build3.log 2>&1 || {
  echo BUILD_FAILED; tail -20 /tmp/gk_build3.log; exit 1; }
echo BUILD_OK
# carry env from the running container, recreate with create->connect->start
ENVS=$(docker inspect gatekeeper --format '{{range .Config.Env}}{{println .}}{{end}}' \\
  | grep -E '^[A-Za-z_][A-Za-z0-9_]*=' \\
  | grep -vE '^(PATH|LANG|GPG|PYTHON|HOSTNAME|HOME)' \\
  | sed 's/^/-e /')
docker rm -f gatekeeper > /dev/null
docker create --name gatekeeper -p 8105:8100 --restart unless-stopped \\
  --network gatehouse $ENVS gatekeeper:2 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 5
echo -n "healthz: "; curl -s http://localhost:8105/healthz; echo
docker ps --filter name=gatekeeper --format '{{.Names}} {{.Image}} {{.Status}}'
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s",
                                         timeout=240)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
cli.close()
print(out)
if err.strip():
    print("STDERR:", err[-300:])

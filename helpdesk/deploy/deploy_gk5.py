"""Deploy gatekeeper:5 (agent-services KB doc) + verify."""
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
sftp.put(rf"{FORK}\helpdesk\kb\agent-services.md",
         "/home/coolify/gatekeeper/helpdesk/kb/agent-services.md")
sftp.close()
print("uploaded agent-services.md")

SCRIPT = """set -e
cd /home/coolify/gatekeeper
docker build -t gatekeeper:5 helpdesk > /tmp/gk_build6.log 2>&1 || {
  echo BUILD_FAILED; tail -20 /tmp/gk_build6.log; exit 1; }
echo BUILD_OK
docker rm -f gatekeeper > /dev/null
ENVS=$(docker inspect gatekeeper --format '{{range .Config.Env}}{{println .}}{{end}}' \\
  | grep -E '^[A-Za-z_][A-Za-z0-9_]*=' \\
  | grep -vE '^(PATH|LANG|GPG|PYTHON|HOSTNAME|HOME)' \\
  | sed 's/^/-e /')
docker create --name gatekeeper -p 8105:8100 --restart unless-stopped \\
  --network gatehouse $ENVS gatekeeper:5 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 5
docker exec gatekeeper python -c "
import urllib.request
r = urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10)
print('healthz:', r.read().decode()[:130])
"
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s", timeout=180)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
cli.close()
print(out)
if err.strip():
    print("STDERR:", err[-300:])

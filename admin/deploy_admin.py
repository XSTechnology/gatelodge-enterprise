"""Deploy the Lodge Office (admin) to the VM + redeploy gatekeeper:4.

- admin container: LAN-bound 10.1.2.217:8111 (no tunnel, no DNS, no
  internet exposure by construction), networks gatehouse (gateway panel)
  + coolify (postgres)
- gatekeeper: rebuilt with KB overrides + escalation email_log
"""
import re
from pathlib import Path

import keyring
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

RESEND = keyring.get_password("workshop", "RESEND_API_KEY")
pg_out = (TMP / "vm_postgres_out6.txt").read_text(encoding="utf-8",
                                                  errors="replace")
PG_PW = re.search(r"gatelodge:([^@\s]+)@zrqxzbetinodrkqrfd1berbq",
                  pg_out).group(1)
DB_URL = (f"postgresql://gatelodge:{PG_PW}@zrqxzbetinodrkqrfd1berbq"
          f":5432/gatelodge")
creds = (TMP / "gatekeeper_credentials.txt").read_text().splitlines()
def cred(name):
    return [l.split("=", 1)[1] for l in creds
            if l.startswith(name + "=")][0]
GK_KEY = cred("GATEWAY_KEY")
REVIEW_KEY = cred("REVIEW_KEY")
ADMIN_KEY = cred("ADMIN_KEY")

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()

# admin files
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
# gatekeeper app.py (kb overrides + email log)
sftp.put(str(FORK / "helpdesk" / "app.py"),
         "/home/coolify/gatekeeper/helpdesk/app.py")
print("uploaded admin + gatekeeper files")
sftp.close()

SCRIPT = f"""set -e
echo "== build admin image =="
cd /home/coolify/admin
docker build -t lodge-office:1 . > /tmp/admin_build.log 2>&1 || {{
  echo ADMIN_BUILD_FAILED; tail -20 /tmp/admin_build.log; exit 1; }}
echo BUILD_OK
echo "== run admin (LAN-only) =="
docker rm -f lodge-office 2>/dev/null || true
docker create --name lodge-office \\
  -p 10.1.2.217:8111:8111 --restart unless-stopped \\
  --network gatehouse \\
  -e DATABASE_URL='{DB_URL}' \\
  -e ADMIN_KEY='{ADMIN_KEY}' \\
  -e RESEND_API_KEY='{RESEND}' \\
  -e SUPPORT_EMAIL=support@datagatelodge.com \\
  -e GATEWAY_URL=http://gatekeeper-gw:8110 \\
  -e GATEWAY_KEY='{GK_KEY}' \\
  -e LICENSING_URL=http://10.1.2.217:8100 \\
  lodge-office:1 > /dev/null
docker network connect coolify lodge-office
docker start lodge-office > /dev/null
sleep 5
echo -n "admin healthz: "
docker exec lodge-office python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8111/healthz', timeout=10).read().decode()[:100])
"
echo "== rebuild gatekeeper:4 (kb overrides + email log) =="
cd /home/coolify/gatekeeper
docker build -t gatekeeper:4 helpdesk > /tmp/gk_build5.log 2>&1 || {{
  echo GK_BUILD_FAILED; tail -20 /tmp/gk_build5.log; exit 1; }}
docker rm -f gatekeeper > /dev/null
docker create --name gatekeeper -p 8105:8100 --restart unless-stopped \\
  --network gatehouse \\
  -e GATEWAY_URL=http://gatekeeper-gw:8110 \\
  -e GATEWAY_KEY='{GK_KEY}' \\
  -e DATABASE_URL='{DB_URL}' \\
  -e TURNSTILE_SECRET="$(docker inspect licensing --format '{{{{range .Config.Env}}}}{{{{println .}}}}{{{{end}}}}' | grep '^TURNSTILE_SECRET=' | cut -d= -f2-)" \\
  -e TURNSTILE_SITEKEY="$(docker inspect licensing --format '{{{{range .Config.Env}}}}{{{{println .}}}}{{{{end}}}}' | grep '^TURNSTILE_SITEKEY=' | cut -d= -f2-)" \\
  -e ALLOWED_ORIGINS='https://datagatelodge.com,https://www.datagatelodge.com,https://staging.datagatelodge.com' \\
  -e RESEND_API_KEY='{RESEND}' \\
  -e EMAIL_FROM='Gatekeeper <support@datagatelodge.com>' \\
  -e SUPPORT_EMAIL=support@datagatelodge.com \\
  -e REVIEW_KEY='{REVIEW_KEY}' \\
  gatekeeper:4 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 5
echo -n "gatekeeper healthz: "
docker exec gatekeeper python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:100])
"
echo "== public exposure check (should be LAN-only) =="
docker ps --filter name=lodge-office --format '{{{{.Names}}}} {{{{{{{{.Ports}}}}}}}}'
echo "== all containers =="
docker ps --format '{{{{.Names}}}} | {{{{.Status}}}}' | grep -E "lodge|gatekeeper|licensing|zrqx"
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s",
                                         timeout=420)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
cli.close()
print(out)
if err.strip():
    print("STDERR:", err[-400:])

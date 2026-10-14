"""Redeploy gatekeeper:3 (40/hr session limit) — explicit env recipe
(the env-carry pipeline breaks on EMAIL_FROM's spaces/angle brackets)."""
import re
from pathlib import Path

import keyring
import paramiko

HOST, USER, PW = "10.1.2.217", "coolify", "@231Garton"
TMP = Path(r"C:\Users\dlang\AppData\Local\Temp\workshop_"
           r"833c5d6b-9aa2-4380-a63b-7c740c69a0e5")
FORK = Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise")

RESEND = keyring.get_password("workshop", "RESEND_API_KEY")
creds = (TMP / "gatekeeper_credentials.txt").read_text().splitlines()
GK_KEY = [l for l in creds if l.startswith("GATEWAY_KEY=")][0].split("=", 1)[1]
REVIEW_KEY = [l for l in creds if l.startswith("REVIEW_KEY=")][0].split("=", 1)[1]
pg_out = (TMP / "vm_postgres_out6.txt").read_text(encoding="utf-8",
                                                  errors="replace")
PG_PW = re.search(r"gatelodge:([^@\s]+)@zrqxzbetinodrkqrfd1berbq",
                  pg_out).group(1)
DB_URL = (f"postgresql://gatelodge:{PG_PW}@zrqxzbetinodrkqrfd1berbq"
          f":5432/gatelodge")

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
sftp.put(str(FORK / "helpdesk" / "app.py"),
         "/home/coolify/gatekeeper/helpdesk/app.py")
sftp.close()
print("uploaded app.py")

SCRIPT = f"""set -e
cd /home/coolify/gatekeeper
docker build -t gatekeeper:3 helpdesk > /tmp/gk_build4.log 2>&1 || {{
  echo BUILD_FAILED; tail -20 /tmp/gk_build4.log; exit 1; }}
echo BUILD_OK
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
  gatekeeper:3 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 6
echo "== verify from inside the container (VM-local curl to :8105 is flaky) =="
docker exec gatekeeper python -c "
import urllib.request
print('healthz:', urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:120])
"
docker ps --filter name=gatekeeper --format '{{{{.Names}}}} | {{{{.Image}}}} | {{{{.Status}}}}'
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

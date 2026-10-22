"""Redeploy fix: groq-named license + create-connect-start ordering.

SECURITY (review finding): hardcoded SSH credentials removed -- see
fix_gk5.py's note. Env/keyring only; rotate the server password.
"""
import os
import re
import sys
from pathlib import Path

import keyring
import paramiko

HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = (os.environ.get("GK_SSH_PASSWORD")
      or keyring.get_password("gatelodge", "ssh_password"))
if not PW:
    raise SystemExit("Set GK_SSH_PASSWORD (or keyring 'gatelodge'/"
                     "'ssh_password') -- credentials are no longer in source.")
TMP = Path(r"C:\Users\dlang\AppData\Local\Temp\workshop_"
           r"833c5d6b-9aa2-4380-a63b-7c740c69a0e5")
FORK = Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise")

GROQ = Path(r"C:\Users\dlang\Workspace\gateway_secrets\groq_key.txt"
            ).read_text().strip()
RESEND = keyring.get_password("workshop", "RESEND_API_KEY")
creds = (TMP / "gatekeeper_credentials.txt").read_text().splitlines()
GK_KEY = [l for l in creds if l.startswith("GATEWAY_KEY=")][0].split("=", 1)[1]
REVIEW_KEY = [l for l in creds if l.startswith("REVIEW_KEY=")][0].split("=", 1)[1]
DB_PW_LINE = None
import re
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
sftp.put(str(TMP / "gatekeeper_license2.json"),
         "/home/coolify/gatekeeper/gateway-data/license.json")
print("uploaded fixed app.py + groq-named license")
sftp.close()

SCRIPT = f"""set -e
cd /home/coolify/gatekeeper
docker build -t gatekeeper:1 helpdesk > /tmp/gk_build2.log 2>&1 || {{
  echo BUILD_FAILED; tail -20 /tmp/gk_build2.log; exit 1; }}
echo BUILD_OK
echo "== restart gateway (new license) =="
docker rm -f gatekeeper-gw 2>/dev/null || true
docker run -d --name gatekeeper-gw --restart unless-stopped \\
  --network gatehouse -p 127.0.0.1:8110:8110 \\
  -v /home/coolify/gatekeeper/gateway-data:/app/data \\
  -e GROQ_API_KEY='{GROQ}' \\
  gatelodge-enterprise:closed
sleep 6
docker logs gatekeeper-gw 2>&1 | grep -i "entitled\\|License:" | tail -3
echo "== helpdesk: create -> connect -> start =="
docker rm -f gatekeeper 2>/dev/null || true
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
  gatekeeper:1 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 6
echo -n "healthz: "; curl -s http://localhost:8105/healthz; echo
docker ps --filter name=gatekeeper --format '{{{{.Names}}}} {{{{.Status}}}}'
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s",
                                         timeout=300)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
cli.close()
print(out)
if err.strip():
    print("STDERR:", err[-400:])

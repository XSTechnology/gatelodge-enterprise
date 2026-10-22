"""Fix: recreate gatekeeper:5 with explicit env (env-carry broke because
docker inspect ran after docker rm).

SECURITY (review finding): this file previously hardcoded the deployment
SSH host/user/password. The password has been REMOVED from source -- it
now comes from the GK_SSH_PASSWORD environment variable (or the OS
keyring). REMOVING IT FROM GIT DOES NOT ROTATE IT: the old value was in
the repository history and must be changed on the server itself before
this repo can be considered clean."""
import os
import re
from pathlib import Path

import keyring
import paramiko

# ROTATE THE SERVER PASSWORD -- then set it here via env or keyring.
# History still contains the old literal; only a server-side change
# invalidates it.
HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = (os.environ.get("GK_SSH_PASSWORD")
      or keyring.get_password("gatelodge", "ssh_password"))
if not PW:
    raise SystemExit("Set GK_SSH_PASSWORD (or keyring 'gatelodge'/"
                     "'ssh_password') -- credentials are no longer in source.")
TMP = Path(r"C:\Users\dlang\AppData\Local\Temp\workshop_"
           r"833c5d6b-9aa2-4380-a63b-7c740c69a0e5")

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

SCRIPT = f"""set -e
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
  gatekeeper:5 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 6
docker exec gatekeeper python -c "
import urllib.request
r = urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10)
print('healthz:', r.read().decode()[:150])
"
docker ps --filter name=gatekeeper --format '{{{{.Names}}}} {{{{.Image}}}} {{{{.Status}}}}'
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s", timeout=120)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
cli.close()
print(out)
if err.strip():
    print("STDERR:", err[-300:])

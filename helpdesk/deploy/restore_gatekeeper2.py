"""URGENT restore + fix: recreate the gatekeeper container with explicit
env (the env-carry pipeline broke on EMAIL_FROM's spaces/angle brackets),
image gatekeeper:2 (already built with the 20/hr limit)."""
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

GROQ = Path(r"C:\Users\dlang\Workspace\gateway_secrets\groq_key.txt"
            ).read_text().strip()
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
  gatekeeper:2 > /dev/null
docker network connect coolify gatekeeper
docker start gatekeeper > /dev/null
sleep 5
echo -n "healthz: "; curl -s http://localhost:8105/healthz; echo
docker ps --filter name=gatekeeper --format '{{{{.Names}}}} {{{{.Image}}}} {{{{{{{{.Status}}}}}}}}'
"""

stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s",
                                         timeout=120)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
cli.close()
print(out)
if err.strip():
    print("STDERR:", err[-300:])

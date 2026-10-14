"""Phase-3 deploy: migrate licensing data to Postgres + redeploy the
licensing container (image :4) with DATABASE_URL + ADMIN_KEY, attached
to the coolify network for Postgres reachability."""
import re
import secrets
from pathlib import Path

import keyring
import paramiko

HOST, USER, PW = "10.1.2.217", "coolify", "@231Garton"
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

# shared admin key (licensing admin-issue + admin service)
cred_file = TMP / "gatekeeper_credentials.txt"
creds = cred_file.read_text().splitlines()
ADMIN_KEY = ([l for l in creds if l.startswith("ADMIN_KEY=")] or [None])[0]
if not ADMIN_KEY:
    ADMIN_KEY = "adm-" + secrets.token_urlsafe(24)
    with open(cred_file, "a") as f:
        f.write(f"ADMIN_KEY={ADMIN_KEY}\n")
else:
    ADMIN_KEY = ADMIN_KEY.split("=", 1)[1]

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
for f in ("app.py", "requirements.txt"):
    sftp.put(str(FORK / "licensing" / f),
             f"/home/coolify/licensing/{f}")
sftp.put(str(FORK / "helpdesk" / "deploy" / "migrate_licensing.py"),
         "/home/coolify/licensing/migrate_licensing.py")
print("uploaded licensing files + migration script")
sftp.close()

SCRIPT = f"""set -e
echo "== migrate data =="
docker cp licensing:/data/leads.db /home/coolify/licensing/leads-migrate.db
docker cp /home/coolify/licensing/leads-migrate.db gatekeeper:/tmp/leads.db
docker cp /home/coolify/licensing/migrate_licensing.py gatekeeper:/tmp/m.py
docker exec gatekeeper python /tmp/m.py
docker exec -u root gatekeeper rm -f /tmp/leads.db /tmp/m.py
rm -f /home/coolify/licensing/leads-migrate.db
echo "== rebuild licensing image :4 =="
cd /home/coolify/licensing
docker build -t gatelodge-licensing:4 . > /tmp/lic_build4.log 2>&1 || {{
  echo BUILD_FAILED; tail -20 /tmp/lic_build4.log; exit 1; }}
echo BUILD_OK
echo "== recreate licensing container (Postgres + admin key) =="
docker rm -f licensing > /dev/null
docker create --name licensing -p 8100:8100 --restart unless-stopped \\
  -v licensing-data:/data \\
  -v /home/coolify/licensing/binary:/app/binary:ro \\
  -v /home/coolify/licensing/keys:/app/keys:ro \\
  -e TURNSTILE_SECRET=0x4AAAAAAFV9I6dv3WCUnvKqz1e2eO7-gvg \\
  -e TURNSTILE_SITEKEY=0x4AAAAAAFV9IxUZL72oI6yE \\
  -e ALLOWED_ORIGINS='https://datagatelodge.com,https://www.datagatelodge.com,https://staging.datagatelodge.com' \\
  -e BINARY_PATH=/app/binary/gatelodge-community.exe \\
  -e PAYMENT_LINK=https://buy.stripe.com/test_14AaEY0hrdVmbwegqR7g400 \\
  -e STRIPE_WEBHOOK_SECRET=whsec_cvu4ArjPAQy1YKpAUwczt2XxXMnzWYtq \\
  -e LICENSE_PRIVATE_KEY=/app/keys/license_signing_key.pem \\
  -e EMAIL_PROVIDER=resend \\
  -e RESEND_API_KEY='{RESEND}' \\
  -e DATABASE_URL='{DB_URL}' \\
  -e ADMIN_KEY='{ADMIN_KEY}' \\
  gatelodge-licensing:4 > /dev/null
docker network connect coolify licensing
docker start licensing > /dev/null
sleep 6
echo -n "healthz: "; docker exec licensing python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8100/healthz', timeout=10).read().decode()[:120])
"
echo "== verify data in postgres =="
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -t -c \\
  "SELECT (SELECT COUNT(*) FROM licenses) || ' licenses, ' || \\
          (SELECT COUNT(*) FROM leads) || ' leads, ' || \\
          (SELECT COUNT(*) FROM email_log) || ' emails'"
echo "== admin-issue probe (bad key) =="
docker exec licensing python -c "
import urllib.request, urllib.error, json
req = urllib.request.Request('http://127.0.0.1:8100/api/admin/issue',
    data=json.dumps({{'email':'x@y.co'}}).encode(),
    headers={{'Content-Type':'application/json',
             'Authorization':'Bearer wrong'}}, method='POST')
try:
    urllib.request.urlopen(req, timeout=10)
    print('ERROR: bad key accepted')
except urllib.error.HTTPError as e:
    print('admin-issue guard:', e.code, '(401 expected)')
"
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

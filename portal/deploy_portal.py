"""Deploy the task portal: portal:1 on the VM + DNS + tunnel ingress."""
import pathlib

import httpx
import keyring

import paramiko

# SECURITY (review fix): SSH credentials removed from source. And ROTATE
# the server password -- git history still contains the old literal.
import os
HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = os.environ["GK_SSH_PASSWORD"]
FORK = pathlib.Path(r"C:\Users\dlang\Workspace\gatelodge_enterprise")
ZONE = "c8636bf700c64992db3261d2ff010d24"          # datagatelodge.com
TUNNEL_CNAME = "2107a01d.cfargotunnel.com"          # vm-rehearsal

# ── 1. DNS record (portal.datagatelodge.com → tunnel) ─────────────────────
TOKEN = keyring.get_password("workshop", "CLOUDFLARE_API_TOKEN")
with httpx.Client(timeout=30) as c:
    # check existing
    r = c.get(f"https://api.cloudflare.com/client/v4/zones/{ZONE}/dns_records"
              f"?name=portal.datagatelodge.com",
              headers={"Authorization": f"Bearer {TOKEN}"})
    existing = r.json().get("result", [])
    if existing:
        print("DNS record exists:", existing[0]["type"],
              existing[0]["content"])
    else:
        r = c.post(
            f"https://api.cloudflare.com/client/v4/zones/{ZONE}/dns_records",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json={"type": "CNAME", "name": "portal.datagatelodge.com",
                  "content": TUNNEL_CNAME, "proxied": True})
        j = r.json()
        print("DNS created:", j.get("success"),
              "" if j.get("success") else str(j.get("errors"))[:200])

# ── 2. tunnel ingress (cloudflared config on the VM) ─────────────────────
# The vm-rehearsal tunnel config lives on the VM; add the portal ingress
cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
stdin, _, _ = cli.exec_command("mkdir -p /home/coolify/portal")
stdin.channel.recv_exit_status()
sftp = cli.open_sftp()
for f in ("app.py", "requirements.txt", "Dockerfile"):
    sftp.put(str(FORK / "portal" / f), f"/home/coolify/portal/{f}")
sftp.put(str(FORK / "portal" / "seed.sql"),
         "/home/coolify/portal/seed.sql")
print("uploaded portal sources")
sftp.close()

SCRIPT = r"""set -e
envof() { docker inspect "$1" --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep "^$2=" | cut -d= -f2-; }
DB_URL=$(envof gatekeeper DATABASE_URL)
GK_KEY=$(envof lodge-office GATEWAY_KEY)

echo "== build portal:1 =="
cd /home/coolify/portal
docker build -t task-portal:1 . > /tmp/portal1.log 2>&1 || {
  echo PORTAL_BUILD_FAILED; tail -20 /tmp/portal1.log; exit 1; }
echo BUILD_OK
docker rm -f task-portal > /dev/null 2>&1 || true
docker create --name task-portal -p 8106:8106 --restart unless-stopped \
  --network gatehouse \
  -e DATABASE_URL="$DB_URL" \
  -e GATEWAY_URL=http://gatekeeper-gw:8110 \
  -e GATEWAY_KEY="$GK_KEY" \
  task-portal:1 > /dev/null
docker network connect coolify task-portal
docker start task-portal > /dev/null
sleep 5
echo -n "portal healthz: "
docker exec task-portal python -c "
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8106/healthz', timeout=10).read().decode()[:100])
"

echo "== seed templates + demo client =="
docker cp /home/coolify/portal/seed.sql zrqxzbetinodrkqrfd1berbq:/tmp/
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -f /tmp/seed.sql
docker exec zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -t -A -c \
  "SELECT COUNT(*) FROM portal_templates; SELECT COUNT(*) FROM portal_clients"

echo "== tunnel ingress: add portal to the vm-rehearsal config =="
# find where cloudflared runs and its config
CFD=$(ps aux | grep cloudflared | grep -v grep | head -1)
echo "cloudflared process: $CFD"
CONF=""
for d in /home/coolify/.cloudflared /etc/cloudflared; do
  for f in "$d"/*.yml "$d"/*.yaml; do
    [ -f "$f" ] && CONF="$f" && break
  done
done
echo "config file: $CONF"
if [ -n "$CONF" ]; then
  echo "--- current config ---"
  cat "$CONF"
  if ! grep -q portal "$CONF"; then
    echo "--- adding portal ingress ---"
    # insert before the last line (the catch-all or final entry)
    python3 - "$CONF" << 'PYEOF'
import sys
path = sys.argv[1]
text = open(path).read()
entry_hostname = "  - hostname: portal.datagatelodge.com"
entry_service = "    service: http://10.1.2.217:8106"
lines = text.rstrip().split("\n")
if entry_hostname not in text:
    for i in range(len(lines) - 1, -1, -1):
        if "hostname" in lines[i]:
            lines.insert(i, entry_hostname)
            lines.insert(i + 1, entry_service)
            break
    else:
        lines.append(entry_hostname)
        lines.append(entry_service)
    open(path, "w").write("\n".join(lines) + "\n")
    print("ingress added")
else:
    print("already present")
PYEOF
    cat "$CONF"
    # restart cloudflared
    pkill -HUP cloudflared 2>/dev/null || true
    echo "cloudflared reloaded"
  else
    echo "portal ingress already present"
  fi
fi

echo "== containers =="
docker ps --format '{{.Names}} | {{.Image}} | {{.Status}}' | grep -E 'portal|gatekeeper|lodge|zrqx'
"""

stdin, stdout, stderr = cli.exec_command(
    "sudo -S -p '' bash -s", timeout=600)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
print(stdout.read().decode(errors="replace"))
err = stderr.read().decode(errors="replace").strip()
if err:
    print("STDERR:", err[-400:])
cli.close()

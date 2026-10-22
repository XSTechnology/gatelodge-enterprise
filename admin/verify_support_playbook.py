"""Verify the support playbook expansion live (gatekeeper:16 + lodge-office:12).

1. internal-doc leak tests through the email inbound path: questions that
   match the NEW support-internal content must never be served from it
   (excluded doc => low confidence => review), plus two routing controls.
2. Lodge Office KB screen: 13 docs listed, support-internal present with
   the internal badge (checked on the VM via the container's ADMIN_KEY —
   the key is never printed).
"""
import time

import httpx
import paramiko

BASE = "https://chat.datagatelodge.com"
CREDS = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"
key = ""
for line in open(CREDS, encoding="utf-8", errors="replace"):
    if line.strip().startswith("INBOUND_KEY"):
        key = (line.split("=", 1)[1] if "=" in line
               else line.split(":", 1)[1]).strip()
        break

QUESTIONS = [
    # leak tests — the phrasing matches the NEW internal doc content
    ("what is the diagnostic ladder you use for troubleshooting tickets",
     "ladder", "NOT:support-internal"),
    ("what is on your support quick reference card",
     "card", "NOT:support-internal"),
    ("how does your tier 2 support workflow operate",
     "tier2", "NOT:support-internal"),
    # routing controls — public behavior unchanged by the rebuild
    ("how much does it cost to run gatelodge",
     "price", "presales"),
    ("can I export the audit log to our SIEM",
     "export", "evaluation"),
]

ok = fail = 0
for q, tag, want in QUESTIONS:
    try:
        r = httpx.post(
            f"{BASE}/api/email/inbound",
            headers={"Authorization": f"Bearer {key}"},
            json={"from_addr": "playbook-test@example.com", "subject": "q",
                  "text": q,
                  "message_id": f"spb-{tag}-{time.time()}"},
            timeout=180)
        j = r.json()
    except Exception as e:
        print(f"FAIL {tag}: {type(e).__name__}")
        fail += 1
        continue
    doc = j.get("doc") or ""
    good = (doc != want[4:] if want.startswith("NOT:") else doc == want)
    print(f"{'PASS' if good else 'FAIL'} {tag:8} -> "
          f"{doc or j.get('reason', '')[:40]}")
    ok, fail = ok + (1 if good else 0), fail + (0 if good else 1)

print(f"\ninbound: {ok} pass / {fail} fail")

# ── Lodge Office KB screen (on the VM, key never printed) ──────────────────
# SECURITY (review fix): SSH credentials removed from source. And ROTATE
# the server password -- git history still contains the old literal.
import os
HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = os.environ["GK_SSH_PASSWORD"]
cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
SCRIPT = r"""
ADMIN_KEY=$(docker inspect lodge-office --format '{{range .Config.Env}}{{println .}}{{end}}' | grep '^ADMIN_KEY=' | cut -d= -f2-)
curl -s -c /tmp/ck -o /dev/null -d "key=$ADMIN_KEY" http://10.1.2.217:8111/login
KB=$(curl -s -b /tmp/ck http://10.1.2.217:8111/kb)
echo "kb rows:      $(echo "$KB" | grep -c 'kb/edit?name=')"
echo "internal tags: $(echo "$KB" | grep -o '>internal<' | wc -l)"
echo "support-internal row: $(echo "$KB" | grep -c 'name=support-internal')"
echo "ladder on edit page: $(curl -s -b /tmp/ck 'http://10.1.2.217:8111/kb/edit?name=support-internal' | grep -c 'diagnostic ladder')"
rm -f /tmp/ck
"""
stdin, stdout, stderr = cli.exec_command("sudo -S -p '' bash -s", timeout=120)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
print(stdout.read().decode(errors="replace"))
err = stderr.read().decode(errors="replace").strip()
if err:
    print("STDERR:", err[-300:])
cli.close()

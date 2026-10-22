"""Backfill contacts from existing history + live-verify the mail room."""
import httpx

import paramiko

LODGE = "http://10.1.2.217:8111"
CRED = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"
# SECURITY (review fix): SSH credentials removed from source. And ROTATE
# the server password -- git history still contains the old literal.
import os
HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = os.environ["GK_SSH_PASSWORD"]


def cred(name):
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"{name} not found")


ADMIN_KEY = cred("ADMIN_KEY")
INBOUND_KEY = cred("INBOUND_KEY")

BACKFILL = r"""
INSERT INTO contacts (email, name, company, source, first_seen, last_seen, message_count)
SELECT from_addr, '', '', 'inbound',
       MIN(created_at)::text, MAX(created_at)::text, COUNT(*)
FROM inbound_emails WHERE status != 'ignored'
GROUP BY from_addr
ON CONFLICT (email) DO UPDATE SET
  message_count = contacts.message_count + EXCLUDED.message_count,
  last_seen = EXCLUDED.last_seen;

INSERT INTO contacts (email, name, company, source, first_seen, last_seen, message_count)
SELECT email, '', COALESCE(NULLIF(business_name, ''), ''),
       CASE WHEN serial != '' THEN 'purchase' ELSE 'trial' END,
       MIN(created_at)::text, MAX(created_at)::text, COUNT(*)
FROM leads GROUP BY email, business_name, serial
ON CONFLICT (email) DO UPDATE SET
  company = CASE WHEN EXCLUDED.company != '' THEN EXCLUDED.company ELSE contacts.company END,
  source = CASE WHEN contacts.source IN ('', 'manual', 'trial') AND EXCLUDED.source = 'purchase' THEN 'purchase' ELSE contacts.source END,
  message_count = contacts.message_count + EXCLUDED.message_count,
  last_seen = EXCLUDED.last_seen;

INSERT INTO contacts (email, name, company, source, first_seen, last_seen, message_count)
SELECT email, '', COALESCE(NULLIF(business_name, ''), ''), 'purchase',
       MIN(created_at)::text, MAX(created_at)::text, COUNT(*)
FROM licenses GROUP BY email, business_name
ON CONFLICT (email) DO UPDATE SET
  company = CASE WHEN EXCLUDED.company != '' THEN EXCLUDED.company ELSE contacts.company END,
  source = CASE WHEN contacts.source IN ('', 'manual', 'trial') THEN 'purchase' ELSE contacts.source END,
  message_count = contacts.message_count + EXCLUDED.message_count,
  last_seen = EXCLUDED.last_seen;

SELECT email, company, source, message_count FROM contacts ORDER BY last_seen DESC;
"""

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
with sftp.open("/home/coolify/_backfill.sql", "w") as f:
    f.write(BACKFILL)
sftp.close()
stdin, stdout, stderr = cli.exec_command(
    "sudo -S -p '' bash -c 'docker cp /home/coolify/_backfill.sql "
    "zrqxzbetinodrkqrfd1berbq:/tmp/ && docker exec "
    "zrqxzbetinodrkqrfd1berbq psql -U gatelodge -d gatelodge -f "
    "/tmp/_backfill.sql && rm -f /home/coolify/_backfill.sql'", timeout=60)
stdin.write(PW + "\n")
stdin.channel.shutdown_write()
print("== backfill ==")
print(stdout.read().decode(errors="replace"))
err = stderr.read().decode(errors="replace").strip()
if err:
    print("STDERR:", err[-300:])
cli.close()

# ── live checks via the Lodge Office ─────────────────────────────────────
checks = []
with httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": ADMIN_KEY})
    r = c.get("/contacts")
    checks.append(("contacts screen with backfilled book",
                   r.status_code == 200 and "dlangen.wpg@gmail.com" in r.text
                   and "sales@xs.technology" in r.text))
    r = c.get("/compose")
    checks.append(("compose screen", r.status_code == 200
                   and "message" in r.text.lower()))
    r = c.get("/inbox/view", params={"id": "live-test-001"})
    checks.append(("inbound viewer", r.status_code == 200
                   and "Compose follow-up" in r.text))
    r = c.get("/emails")
    checks.append(("email log with subjects", r.status_code == 200
                   and "view" in r.text))

# live contact capture through the real webhook
with httpx.Client(timeout=120) as c:
    r = c.post("https://chat.datagatelodge.com/api/email/inbound",
               headers={"Authorization": f"Bearer {INBOUND_KEY}"},
               json={"message_id": "mailroom-capture-test",
                     "from_addr": "capture-test@external-example.com",
                     "subject": "capture test",
                     "text": "what is the meaning of life?"})
    checks.append(("webhook uncertain -> review",
                   r.json().get("status") == "review", r.text[:150]))

with httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": ADMIN_KEY})
    r = c.get("/contacts", params={"q": "capture-test"})
    checks.append(("live contact captured from inbound",
                   "capture-test@external-example.com" in r.text))

for name, ok, *extra in checks:
    print(("OK  " if ok else "FAIL") + " " + name
          + ("  — " + extra[0] if extra and not ok else ""))
print("\nMAIL ROOM " + ("LIVE VERIFIED" if all(c[1] for c in checks)
                        else "PROBLEMS"))

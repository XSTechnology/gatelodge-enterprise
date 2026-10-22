"""Live verification of the CISO messaging pass (gatekeeper:22 +
lodge-office:18 + site personas + staging)."""
import time

import httpx
import paramiko

SITE = "https://datagatelodge.com"
CHAT = "https://chat.datagatelodge.com"
CREDS = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"
inbound = ""
for line in open(CREDS, encoding="utf-8", errors="replace"):
    if line.strip().startswith("INBOUND_KEY"):
        inbound = (line.split("=", 1)[1] if "=" in line
                   else line.split(":", 1)[1]).strip()
        break

out = []
ok = fail = 0


def check(name, cond, extra=""):
    global ok, fail
    out.append(f"{'PASS' if cond else 'FAIL'} {name}"
               + ("" if cond else f"   {extra}"))
    ok += 1 if cond else 0
    fail += 0 if cond else 1


# 1. live site: buying-committee personas on capabilities
r = httpx.get(f"{SITE}/gatelodge.html", timeout=30)
check("live site: persona row is the buying committee",
      "ceo &#183; business" in r.text or "ceo · business" in r.text)
check("live site: all four roles present",
      all(x in r.text for x in ("cio", "ciso", "audit")))

# 2. routing through the corrected/new evaluation content
QS = [
    ("what evidence do you produce for auditors", "evidence",
     ("evaluation", "governance")),  # both carry the audit-chain content
    ("how do you handle shadow ai", "shadow",
     ("evaluation", "governance")),   # both docs legitimately cover it
    ("do you log the content of prompts", "boundary",
     ("evaluation",)),                # the new section owns this one
]
for q, tag, want in QS:
    try:
        r = httpx.post(
            f"{CHAT}/api/email/inbound",
            headers={"Authorization": f"Bearer {inbound}"},
            json={"from_addr": f"ciso2-{tag}@example.com",
                  "subject": "q", "text": q,
                  "message_id": f"c2-{tag}-{time.time()}"},
            timeout=180)
        j = r.json()
    except Exception as e:
        check(f"gatekeeper: '{tag}' routes", False, type(e).__name__)
        continue
    check(f"gatekeeper: '{tag}' routes", j.get("doc") in want,
          f"doc={j.get('doc')} rule={j.get('rule')}")

# 3. new sections in both serving containers (separate execs — the
# earlier && chain ate the second output)
import os as _os
# SECURITY (review fix): credential removed from source -- env only, and
# ROTATE the server password (git history retains the old literal).
_PW = _os.environ["GK_SSH_PASSWORD"]
cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(_os.environ.get("GK_SSH_HOST", "10.1.2.217"),
            username=_os.environ.get("GK_SSH_USER", "coolify"),
            password=_PW, timeout=20)
for container in ("gatekeeper", "lodge-office"):
    stdin, stdout, stderr = cli.exec_command(
        "sudo -S -p '' docker exec " + container + " grep -c "
        "'Is prompt content logged' /app/kb/evaluation.md", timeout=60)
    stdin.write(_PW + "\n")
    n = stdout.read().decode(errors="replace").strip()
    check(f"{container} serves the content-logging section", n == "1", n)
stdin, stdout, stderr = cli.exec_command(
    "sudo -S -p '' docker exec zrqxzbetinodrkqrfd1berbq psql -U "
    "gatelodge -d gatelodge -t -A -c \"SELECT count(*) FROM kb_intent "
    "WHERE doc='evaluation'\"", timeout=60)
stdin.write(_PW + "\n")
n = stdout.read().decode(errors="replace").strip()
check("evaluation intent map present (10 sections)", n == "10", n)
cli.close()

# 4. staging serves the persona row (Coolify rebuild from earlier push)
r = httpx.get("https://staging.datagatelodge.com/gatelodge.html",
              timeout=60)
check("staging serves the buying committee",
      "ceo &#183; business" in r.text or "ceo · business" in r.text)

out.append(f"\n{ok} pass / {fail} fail")
with open(r"C:\Users\dlang\AppData\Local\Temp\verify_ciso2.txt", "w",
          encoding="utf-8") as f:
    f.write("\n".join(out))
print("done")

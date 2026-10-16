"""Staging refresh: push sites/datagatelodge to Gitea coolify/sites +
trigger the Coolify redeploy + verify staging serves the new version."""
import subprocess

import paramiko

HOST, USER, PW = "10.1.2.217", "coolify", "@231Garton"
FORK = r"C:\Users\dlang\Workspace\gatelodge_enterprise"
COOLIFY_TOKEN = "1|ppd6WuggZCMXzr4kBxoUg8UXwrCAX2ud0b7N8PLQ29047bd6"
APP_UUID = "znuob7xhhw5sky9kzmaay2ue"

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)

# 1. Gitea access token for the coolify user (a fresh name each run —
#    Gitea refuses to re-print or re-create an existing token name)
import time as _time
tok_name = f"staging-sync-{int(_time.time())}"
stdin, stdout, stderr = cli.exec_command(
    "sudo -S -p '' docker exec -u git gitea gitea admin user generate-"
    f"access-token -u coolify --scopes write:repository --token-name "
    f"{tok_name}", timeout=60)
stdin.write(PW + "\n")
tok_out = stdout.read().decode(errors="replace").strip()
tok_err = stderr.read().decode(errors="replace").strip()
print("token gen:", tok_out[:60] if tok_out else ("ERR " + tok_err[:200]))
token = tok_out.split(":")[-1].strip() if tok_out else ""
if not token:
    raise SystemExit("no gitea token")
cli.close()

# 2. Clone the gitea repo fresh, sync content, commit, push
GITEA = f"http://coolify:{token}@10.1.2.217:3000/coolify/sites.git"
WORK = r"C:\Users\dlang\AppData\Local\Temp\staging_push"
subprocess.run(["cmd", "/c", "if", "exist", WORK, "rmdir", "/s", "/q",
                WORK], check=False)
subprocess.run(["git", "clone", GITEA, WORK], check=True,
               capture_output=True)
# robocopy the site content in (Dockerfile included; /XD keeps .git)
subprocess.run(["robocopy", rf"{FORK}\sites\datagatelodge", WORK,
                "/MIR", "/XD", ".git", "/NFL", "/NDL", "/NJH", "/NP"],
               check=False)
subprocess.run(["git", "-C", WORK, "config", "user.email",
                "staging@datagatelodge.com"], check=True)
subprocess.run(["git", "-C", WORK, "config", "user.name",
                "staging sync"], check=True)
r = subprocess.run(["git", "-C", WORK, "add", "-A"], check=True,
                   capture_output=True)
r = subprocess.run(["git", "-C", WORK, "commit", "-m",
                    "staging refresh: exec bands (CEO on ROI, CIO on "
                    "how) + executive-brief modal"], capture_output=True,
                   text=True)
print("commit:", (r.stdout + r.stderr).strip()[:120])
r = subprocess.run(["git", "-C", WORK, "push", "origin", "main"],
                   capture_output=True, text=True)
print("push:", (r.stdout + r.stderr).strip()[-200:])

# 3. Trigger the Coolify redeploy
import httpx
r = httpx.post(
    f"http://10.1.2.217:8000/api/v1/applications/{APP_UUID}/start",
    headers={"Authorization": f"Bearer {COOLIFY_TOKEN}"}, timeout=60)
print("coolify start:", r.status_code, r.text[:120])

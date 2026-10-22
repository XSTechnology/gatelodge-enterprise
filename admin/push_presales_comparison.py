"""Push the updated presales doc (with the feature-comparison section)
live via the Lodge Office, then re-trace the feature question."""
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
FAQ = r"C:\Users\dlang\Workspace\gatelodge_enterprise\helpdesk\kb\presales.md"


def read_key():
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.upper().startswith("ADMIN_KEY"):
            return line.split("=", 1)[-1].strip()
    raise SystemExit("ADMIN_KEY not found")


key = read_key()
content = open(FAQ, encoding="utf-8").read()

# parse the header for the structured save
import re
title = (re.search(r"^title:\s*(.+)$", content, re.M) or [None, "Presales"])[1].strip()
desc = (re.search(r"^description:\s*(.+)$", content, re.M) or [None, ""])[1].strip()
m = re.search(r"^description:.*$", content, re.M)
body = content[m.end():].strip("\n") if m else content

with httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": key})
    r = c.post("/kb/save", data={"name": "presales", "title": title,
                                 "description": desc, "content": body})
    kb_page = c.get("/kb").text
    print("saved (live):", r.status_code == 200 or r.url.path.endswith("/kb/edit"))
    print("presales shows live-edited:",
          "live-edited" in kb_page[kb_page.find("presales"):
                                   kb_page.find("presales") + 400])

TRACE = '''
import asyncio
import sys
sys.path.insert(0, "/app")
import app

Q = "what are feature differences between free and enterprise"


async def main():
    t = await app.triage(Q, [])
    print("rerank: confident =", t["confident"], "| topic =", t["topic"])
    print("picked passages:")
    for c in t["chunks"]:
        print("   -", c["doc"], "/", c["section"])

asyncio.run(main())
'''

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
with sftp.open("/home/coolify/_trace2.py", "w") as f:
    f.write(TRACE)
sftp.close()
stdin, stdout, stderr = cli.exec_command(
    "sudo -S -p '' bash -c 'docker cp /home/coolify/_trace2.py "
    "gatekeeper:/tmp/ && docker exec -w /app gatekeeper python /tmp/_trace2.py"
    " && rm -f /home/coolify/_trace2.py'", timeout=180)
stdin.write(PW + "\n")
stdin.channel.shutdown_write()
print(stdout.read().decode(errors="replace"))
err = stderr.read().decode(errors="replace").strip()
if err:
    print("STDERR:", err[-300:])
cli.close()

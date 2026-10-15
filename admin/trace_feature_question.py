"""Live trace: how does v2 retrieval handle the feature-diff question."""
import pathlib

import paramiko

HOST, USER, PW = "10.1.2.217", "coolify", "@231Garton"
CHECK = '''
import asyncio
import sys
sys.path.insert(0, "/app")
import app

Q = "what are feature differences between free and enterprise"


async def main():
    # stage 1: what retrieval surfaces
    _, cands = app._retrieve(Q)
    print("TF-IDF top candidates:")
    for c in cands:
        print("   -", c["doc"], "/", c["section"])
    # stage 2: what the rerank model picks (real groq call)
    t = await app.triage(Q, [])
    print("rerank: confident =", t["confident"], "| topic =", t["topic"])
    print("picked passages:")
    for c in t["chunks"]:
        print("   -", c["doc"], "/", c["section"],
              "({} chars)".format(len(c["text"])))
    if t["chunks"]:
        print()
        print("--- what the answer model receives ---")
        print(app._passages_block(t["chunks"])[:600])

asyncio.run(main())
'''

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
with sftp.open("/home/coolify/_trace.py", "w") as f:
    f.write(CHECK)
sftp.close()
stdin, stdout, stderr = cli.exec_command(
    "sudo -S -p '' bash -c 'docker cp /home/coolify/_trace.py "
    "gatekeeper:/tmp/ && docker exec -w /app gatekeeper python /tmp/_trace.py"
    " && rm -f /home/coolify/_trace.py'", timeout=180)
stdin.write(PW + "\n")
stdin.channel.shutdown_write()
print(stdout.read().decode(errors="replace"))
err = stderr.read().decode(errors="replace").strip()
if err:
    print("STDERR:", err[-300:])
cli.close()

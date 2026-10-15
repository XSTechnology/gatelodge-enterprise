"""Live rerank proof: run triage() inside the container against the real
gateway + groq models (costs ~a cent of the dogfood budget)."""
import pathlib

import paramiko

HOST, USER, PW = "10.1.2.217", "coolify", "@231Garton"
CHECK = '''
import asyncio
import sys
sys.path.insert(0, "/app")
import app


async def main():
    for q in ["what happens when the 30 day trial ends?",
              "how do your agent services work?",
              "what is the meaning of life?"]:
        t = await app.triage(q, [])
        print("Q:", q)
        print("   confident:", t["confident"], "| legal:", t["legal"],
              "| topic:", t["topic"])
        print("   passages:", [(c["doc"], c["section"])
                               for c in t["chunks"]])

asyncio.run(main())
'''

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
with sftp.open("/home/coolify/_rerank_check.py", "w") as f:
    f.write(CHECK)
sftp.close()
stdin, stdout, stderr = cli.exec_command(
    "sudo -S -p '' bash -c 'docker cp /home/coolify/_rerank_check.py "
    "gatekeeper:/tmp/ && docker exec -w /app gatekeeper "
    "python /tmp/_rerank_check.py && "
    "rm -f /home/coolify/_rerank_check.py'", timeout=180)
stdin.write(PW + "\n")
stdin.channel.shutdown_write()
print(stdout.read().decode(errors="replace"))
err = stderr.read().decode(errors="replace").strip()
if err:
    print("STDERR:", err[-400:])
cli.close()

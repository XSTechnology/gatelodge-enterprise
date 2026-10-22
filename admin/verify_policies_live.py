"""Verify gatekeeper:12 policy engine live: rulesets + gate + triage eval."""
import os

import paramiko

# SECURITY (review fix): credential removed from source -- env only, and
# ROTATE the server password (git history retains the old literal).
PW = os.environ["GK_SSH_PASSWORD"]
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(os.environ.get("GK_SSH_HOST", "10.1.2.217"),
            username=os.environ.get("GK_SSH_USER", "coolify"),
            password=PW, timeout=15)

PYCHK = r'''
import json, os, psycopg
from zen import ZenEngine
conn = psycopg.connect(os.environ["DATABASE_URL"])
rows = conn.execute("SELECT kind, name, version, enabled FROM rulesets ORDER BY kind").fetchall()
for r in rows:
    print("ruleset:", " | ".join(str(x) for x in r))
gate = conn.execute("SELECT name, version, jdm FROM rulesets WHERE kind='task-tool-gate' AND enabled").fetchone()
tri = conn.execute("SELECT name, version, jdm FROM rulesets WHERE kind='email-triage' AND enabled").fetchone()
eng = ZenEngine()
g = eng.create_decision(json.loads(gate[2]))
print("gate web_fetch/notify=false ->", json.dumps(g.evaluate({"tool": "web_fetch", "notify": False})["result"]))
print("gate email_send/notify=false ->", json.dumps(g.evaluate({"tool": "email_send", "notify": False})["result"]))
t = eng.create_decision(json.loads(tri[2]))
print("triage confident+kb ->", json.dumps(t.evaluate({"confident": True, "legal": False, "kb_hits": 2})["result"]))
print("triage not-confident ->", json.dumps(t.evaluate({"confident": False, "legal": False, "kb_hits": 2})["result"]))
'''

SCRIPT = ("docker exec -i gatekeeper python - <<'PYEOF'\n"
          + PYCHK + "\nPYEOF\n")

stdin, stdout, stderr = ssh.exec_command("sudo -S -p '' bash -s", timeout=90)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
print(stdout.read().decode("utf-8", "replace"))
e = stderr.read().decode("utf-8", "replace").strip()
if e:
    print("STDERR:", e[-500:])
ssh.close()

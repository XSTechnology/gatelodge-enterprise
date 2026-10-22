"""Full CRUD loop proof: create via Lodge Office -> live catalog -> remove."""
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
NAME = "verify-crud-test"


def read_key():
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.upper().startswith("ADMIN_KEY"):
            return line.split("=", 1)[-1].strip()
    raise SystemExit("ADMIN_KEY not found")


CHECK = """
import sys
sys.path.insert(0, "/app")
import app
d = app._load_kb_overrides()
print("N_DOCS", len(d))
print("HAS_TEST", "%s" in d)
print("TEST_TITLE", d.get("%s", {}).get("title", "-"))
""" % (NAME, NAME)

key = read_key()
ok = True

with httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": key})
    c.post("/kb/new", data={
        "name": NAME,
        "title": "CRUD verification doc",
        "description": "Temporary doc proving Lodge Office creation "
                       "reaches the live catalog",
        "content": "## Is this live?\n\nYes.",
    })
    listed = NAME in c.get("/kb").text
    print("1. created + listed in Lodge Office:", listed)
    ok &= listed

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
sftp = cli.open_sftp()
with sftp.open("/home/coolify/_crud_check.py", "w") as f:
    f.write(CHECK)
sftp.close()
stdin, stdout, _ = cli.exec_command(
    "sudo -S -p '' bash -c 'docker cp /home/coolify/_crud_check.py "
    "gatekeeper:/tmp/ && docker exec gatekeeper python /tmp/_crud_check.py'",
    timeout=60)
stdin.write(PW + "\n")
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
print("2. live gatekeeper catalog:", " ".join(out.split()))
ok &= "HAS_TEST True" in out and "CRUD verification doc" in out
cli.close()

with httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": key})
    c.get("/kb/reset", params={"name": NAME})
    gone = NAME not in c.get("/kb").text
    print("3. removed after test:", gone)
    ok &= gone

print("\nFULL CRUD LOOP " + ("VERIFIED" if ok else "PROBLEM"))

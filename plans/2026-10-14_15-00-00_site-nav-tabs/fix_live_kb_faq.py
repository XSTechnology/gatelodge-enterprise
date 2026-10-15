"""Fix: copy faq.md into the running gatekeeper + restart, then verify.

Why: _load_kb_overrides() only merges kb_overrides rows whose name is
already in the built-in KB (/app/kb at image build). The faq doc is new,
so the live agent ignores it until the file exists in /app/kb. docker cp
+ restart picks it up now (KB loads at startup); the repo copy ships in
the next gatekeeper image build for permanence.
"""
import time

import paramiko

HOST, USER, PW = "10.1.2.217", "coolify", "@231Garton"
FAQ = r"C:\Users\dlang\Workspace\gatelodge_enterprise\helpdesk\kb\faq.md"
REMOTE_TMP = "/home/coolify/faq.md"

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)


def run(cmd, sudo=True, timeout=120):
    if sudo:
        cmd = "sudo -S -p '' bash -c '" + cmd + "'"
    stdin, stdout, stderr = cli.exec_command(cmd, timeout=timeout)
    if sudo:
        stdin.write(PW + "\n")
        stdin.channel.shutdown_write()
    return (stdout.read().decode(errors="replace").strip(),
            stderr.read().decode(errors="replace").strip())


# 1. upload the doc to the VM
sftp = cli.open_sftp()
sftp.put(FAQ, REMOTE_TMP)
sftp.close()
print("uploaded faq.md to VM")

# 2. copy into the container + fix ownership + restart
out, err = run("docker cp /home/coolify/faq.md gatekeeper:/app/kb/faq.md "
               "&& docker restart gatekeeper")
print("cp+restart:", out or "(ok)", err[:200] if err else "")
time.sleep(6)

# 3. verify: docs on disk, loader catalog, service health
out, _ = run("docker exec gatekeeper ls /app/kb")
print("\n/app/kb now:\n" + out)

out, err = run("docker exec gatekeeper python -c "
               '"import app; print(len(app.KB), sorted(app.KB))"')
print("\nloader catalog:", out, err[:200] if err else "")

out, _ = run("docker ps --filter name=gatekeeper")
print("\n" + out)

run("rm -f " + REMOTE_TMP)
cli.close()
print("\ndone")

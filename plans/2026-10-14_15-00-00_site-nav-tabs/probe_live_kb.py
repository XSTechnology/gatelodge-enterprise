"""Probe the live gatekeeper container: image, baked KB docs, catalog."""
import paramiko

# SECURITY (review fix): SSH credentials removed from source. And ROTATE
# the server password -- git history still contains the old literal.
import os
HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = os.environ["GK_SSH_PASSWORD"]

cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)


def run(cmd, sudo=False):
    if sudo:
        cmd = "sudo -S -p '' bash -c '" + cmd + "'"
    stdin, stdout, stderr = cli.exec_command(cmd, timeout=60)
    if sudo:
        stdin.write(PW + "\n")
        stdin.channel.shutdown_write()
    out = stdout.read().decode(errors="replace").strip()
    err = stderr.read().decode(errors="replace").strip()
    return out, err


out, _ = run("docker ps --filter name=gatekeeper --format '{{.Names}} | {{.Image}} | {{.Status}}'", sudo=True)
print("containers:\n" + out)

out, _ = run("docker exec gatekeeper ls /app/kb", sudo=True)
print("\n/app/kb in running container:\n" + out)

# does the loader see the full catalog (incl. title/desc parsing)?
probe = (
    "docker exec gatekeeper python -c \"import app; "
    "print(len(app.KB), sorted(app.KB)); "
    "print('faq title:', app.KB.get('faq', {}).get('title', 'MISSING'))\""
)
out, err = run(probe, sudo=True)
print("\nloader catalog:\n" + out)
if err:
    print("stderr:", err[:400])
cli.close()

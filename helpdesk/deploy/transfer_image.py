"""Transfer the closed v2.3.0 gateway image to the VM.

docker save -> gzip (streaming) -> sftp -> remote docker load.
Writes DONE/FAILED marker file when complete.
"""
import gzip
import os
import subprocess
import sys
import time

import paramiko

# SECURITY (review fix): SSH credentials removed from source. And ROTATE
# the server password -- git history still contains the old literal.
import os
HOST = os.environ.get("GK_SSH_HOST", "10.1.2.217")
USER = os.environ.get("GK_SSH_USER", "coolify")
PW = os.environ["GK_SSH_PASSWORD"]
LOCAL_GZ = r"C:\Users\dlang\Workspace\gatelodge_enterprise\deploy_tmp\closed-v230.tar.gz"
REMOTE_GZ = "/home/coolify/gatekeeper/closed-v230.tar.gz"
MARKER = r"C:\Users\dlang\AppData\Local\Temp\workshop_833c5d6b-9aa2-4380-a63b-7c740c69a0e5\transfer_done.txt"

t0 = time.time()

# 1. save + gzip (streaming)
print("saving image...", flush=True)
proc = subprocess.Popen(["docker", "save", "gatelodge-enterprise:closed"],
                        stdout=subprocess.PIPE)
n = 0
with gzip.open(LOCAL_GZ, "wb", compresslevel=3) as f:
    while True:
        chunk = proc.stdout.read(4 * 1024 * 1024)
        if not chunk:
            break
        f.write(chunk)
        n += len(chunk)
if proc.wait() != 0:
    print("docker save FAILED")
    Path = None
    open(MARKER, "w").write("FAILED save")
    sys.exit(1)
gz_size = os.path.getsize(LOCAL_GZ)
print(f"saved {n/1e6:.0f} MB raw -> {gz_size/1e6:.0f} MB gz "
      f"({time.time()-t0:.0f}s)", flush=True)

# 2. connect + mkdir + sftp
cli = paramiko.SSHClient()
cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
cli.connect(HOST, username=USER, password=PW, timeout=20)
cli.exec_command("mkdir -p /home/coolify/gatekeeper")[1].channel.recv_exit_status()
print("uploading...", flush=True)
sftp = cli.open_sftp()
sftp.put(LOCAL_GZ, REMOTE_GZ)
sftp.close()
print(f"uploaded ({time.time()-t0:.0f}s total)", flush=True)

# 3. remote docker load
stdin, stdout, stderr = cli.exec_command(
    "sudo -S -p '' bash -c 'gunzip -c " + REMOTE_GZ + " | docker load && "
    "rm -f " + REMOTE_GZ + "'", timeout=600)
stdin.write(PW + "\n")
stdin.channel.shutdown_write()
out = stdout.read().decode(errors="replace")
err = stderr.read().decode(errors="replace")
print(out[-400:])
if err.strip():
    print("STDERR:", err[-200:])
cli.close()

ok = "Loaded image" in out or "gatelodge-enterprise" in out
open(MARKER, "w").write("DONE" if ok else "FAILED load")
print("MARKER:", "DONE" if ok else "FAILED", f"({time.time()-t0:.0f}s total)")

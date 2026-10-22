"""Live verify: trigger the site-health task, watch the engine run it."""
import time

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


def cred(name):
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"{name} not found")


ADMIN_KEY = cred("ADMIN_KEY")

with httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": ADMIN_KEY})
    # screens render
    r = c.get("/tasks")
    print("tasks screen:", r.status_code == 200
          and "Nightly KB gap sweep" in r.text
          and "Site health check" in r.text)
    r = c.get("/tools")
    print("tools screen:", r.status_code == 200
          and "db_query" in r.text and "gap_cluster" in r.text)
    # find the site health task id and queue it
    import re
    m = re.search(r"/tasks/run\?id=(\d+)\">\s*runs.*?Site health check",
                  r.text, re.S)
    # simpler: parse rows
    rows = re.findall(r"href='/tasks/run\?id=(\d+)'>runs</a> "
                      r"<a class='btn ghost'[^>]*href='/tasks/edit\?id=\1'"
                      r">edit</a>", r.text)
    # get ids with names via the tasks page table
    ids = re.findall(r"<tr><td>([^<]+)</td>.*?/tasks/run\?id=(\d+)'",
                     r.text, re.S)
    print("parsed tasks:", ids[:8])

with httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": ADMIN_KEY})
    r = c.get("/tasks")
    tid = None
    for row in re.findall(r"<tr><td>(.*?)</td>.*?/tasks/now\?id=(\d+)'",
                          r.text, re.S):
        if "Site health" in row[0]:
            tid = row[1]
    print("site-health task id:", tid)
    if tid:
        c.get("/tasks/now", params={"id": tid})
        print("queued — waiting for the engine (polls every 30s)…")
        deadline = time.time() + 180
        done = False
        while time.time() < deadline:
            time.sleep(15)
            r = c.get(f"/tasks/run", params={"id": tid})
            if "achieved" in r.text or "failed" in r.text or \
                    "partial" in r.text or "review" in r.text:
                done = True
                break
        print("run finished:", done)
        r = c.get(f"/tasks/run", params={"id": tid})
        # grab the newest run id
        rid = re.search(r"/tasks/runview\?id=(\d+)'", r.text)
        if rid:
            rv = c.get("/tasks/runview", params={"id": rid.group(1)})
            print("run view renders:", rv.status_code == 200)
            txt = rv.text
            for u in ("datagatelodge.com", "get.datagatelodge.com",
                      "chat.datagatelodge.com"):
                print(f"  checked {u}:", u in txt)
            print("  classification tag present:",
                  "achieved" in txt or "partial" in txt
                  or "failed" in txt or "review" in txt)

# also check the owner report email was logged
with httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": ADMIN_KEY})
    r = c.get("/emails")
    print("task-report logged:", "task-report" in r.text)

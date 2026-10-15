"""Live E2E: launch decision task, wait for agent + policy eval, verify viewer."""
import json
import re
import sys
import time
import httpx

BASE = "https://portal.datagatelodge.com"
UA = {"User-Agent": "Mozilla/5.0"}
KEY = "portal-demo-key-2026"

REQUEST_TEXT = (
    "I'm writing to request a refund for my order #8841. We are an "
    "enterprise customer (account: Contoso Ltd, enterprise plan). We "
    "purchased 40 licenses at $28 each on October 2nd — $1,120 total, "
    "that is 13 days ago. Honestly the product works fine, but our "
    "budget was cut and we changed our mind about the expansion. We "
    "did receive one refund earlier this quarter for a duplicate "
    "invoice. Please advise.")

c = httpx.Client(headers=UA, timeout=60, follow_redirects=False)
c.post(f"{BASE}/login", data={"key": KEY})

# find the decision template id
r = c.get(f"{BASE}/")
m = re.search(r"/launch/(\d+)\">[^<]*</a></p></div>" , r.text)
tids = re.findall(r"/launch/(\d+)", r.text)
print("templates on dashboard:", len(tids))

# the decision template is the one whose launch form mentions refund
tid = None
for t in set(tids):
    rf = c.get(f"{BASE}/launch/{t}")
    if "Refund" in rf.text:
        tid = t
        break
print("decision template id:", tid)
assert tid, "decision template not found"

r = c.post(f"{BASE}/launch/{tid}", data={
    "request_text": REQUEST_TEXT,
    "task_name": "Contoso refund decision",
    "schedule": "manual"})
print("launch ->", r.status_code, r.headers.get("location"))

# locate the task
r = c.get(f"{BASE}/")
rows = re.findall(r"<tr><td>(.*?)</td>.*?href='/task/(\d+)'", r.text, re.S)
task_id = None
for nm, i in rows:
    if "Contoso refund decision" in nm:
        task_id = i
        break
print("task id:", task_id)
assert task_id, "task not found on dashboard"

print("polling for run (30s runner + agent + review)...")
run_id = None
for attempt in range(40):
    time.sleep(15)
    r = c.get(f"{BASE}/task/{task_id}")
    m = re.search(r"/run/(\d+)", r.text)
    cur = m.group(1) if m else None
    if cur and cur != run_id:
        run_id = cur
        print(f"  poll {attempt+1}: run #{run_id}")
    if run_id:
        rr = c.get(f"{BASE}/run/{run_id}")
        txt = rr.text
        if ("achieved" in txt or "partial" in txt or "failed" in txt
                or ">review<" in txt):
            print("\n=== RUN COMPLETE ===")
            ok = fail = 0

            def check(name, cond):
                global ok, fail
                print(("PASS " if cond else "FAIL ") + name)
                ok, fail = (ok + 1, fail) if cond else (ok, fail + 1)

            check("policy decision section", "Policy decision" in txt)
            check("decision approved", "approved" in txt)
            check("rule fired shown", "ent-90d" in txt)
            check("facts section", "Extracted facts" in txt)
            check("facts contain tier", "enterprise" in txt)
            check("submit_facts step", "submit_facts" in txt)
            check("policy_check step", "policy_check" in txt)
            check("ruleset named", "Refund eligibility policy" in txt)
            m2 = re.search(r"tag (\w+)'>(\w+)</span>", txt)
            print("classification:", m2.group(2) if m2 else "?")
            print(f"\nRUN VIEWER: {BASE}/run/{run_id}")
            print(f"{ok} pass / {fail} fail")
            sys.exit(1 if fail else 0)
        print(f"    run #{run_id} still working...")
print("TIMEOUT waiting for classification")
sys.exit(1)

"""Phase 2 E2E: support ticket triage decision run (global policy)."""
import re
import sys
import time
import httpx

BASE = "https://portal.datagatelodge.com"
UA = {"User-Agent": "Mozilla/5.0"}
KEY = "portal-demo-key-2026"

# critical severity + enterprise tier → crit-ent → escalate_now, 1h SLA
TICKET = ("URGENT: production gateway completely down since 4am — all "
          "our customer traffic is failing. Contoso Ltd, enterprise "
          "account #ENT-88. This is a critical outage, our SLA gives us "
          "about 3 hours left before breach penalties. First escalation "
          "this week. Please route appropriately.")

c = httpx.Client(headers=UA, timeout=60, follow_redirects=False)
c.post(f"{BASE}/login", data={"key": KEY})
ok = fail = 0

def check(name, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"PASS {name}")
    else:
        fail += 1
        print(f"FAIL {name}  {detail}")

# find the triage template by TITLE (picker lists other policy names)
r = c.get(f"{BASE}/")
tids = set(re.findall(r"/launch/(\d+)", r.text))
tid = None
for t in tids:
    rf = c.get(f"{BASE}/launch/{t}")
    if "Support ticket triage</h2>" in rf.text:
        tid = t
        break
check("triage template found", tid is not None, str(tids))
print("triage template id:", tid)
assert tid

r = c.post(f"{BASE}/launch/{tid}", data={
    "ticket_text": TICKET, "task_name": "Contoso outage ticket",
    "schedule": "manual"})
check("launch 303", r.status_code == 303, r.status_code)

r = c.get(f"{BASE}/")
rows = re.findall(r"<tr><td>(.*?)</td>.*?href='/task/(\d+)'", r.text, re.S)
task_id = next((i for nm, i in rows if "Contoso outage" in nm), None)
print("task id:", task_id)
assert task_id

print("polling for run...")
run_id = None
for attempt in range(40):
    time.sleep(15)
    r = c.get(f"{BASE}/task/{task_id}")
    m = re.search(r"/run/(\d+)", r.text)
    if m and m.group(1) != run_id:
        run_id = m.group(1)
        print(f"  run #{run_id}")
    if run_id:
        rr = c.get(f"{BASE}/run/{run_id}")
        txt = rr.text
        if ("achieved" in txt or "partial" in txt or "failed" in txt
                or ">review<" in txt):
            check("policy decision section", "Policy decision" in txt)
            check("routing escalate_now", "escalate_now" in txt)
            check("rule crit-ent fired", "crit-ent" in txt)
            check("triage policy named", "Support ticket triage policy" in txt)
            check("facts present", "Extracted facts" in txt)
            check("severity extracted", "critical" in txt)
            print(f"RUN: {BASE}/run/{run_id}")
            break
        print("    working...")

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

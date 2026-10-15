"""Phase 1 E2E: client-owned policy CRUD + test + launch + decision run.
Client policy = refund variant where premium changed-mind allows $300
(global allows $200) — a premium/10d/$250/changed_mind case must be
APPROVED by the client policy but would be DENIED by the global one."""
import json
import re
import sys
import time
import httpx

BASE = "https://portal.datagatelodge.com"
UA = {"User-Agent": "Mozilla/5.0"}
KEY = "portal-demo-key-2026"

# refund JDM with premium-mindchange threshold raised to 300
def refund_jdm(premium_cap: int) -> str:
    rules = [
        ("guard-abuse", ">= 3", "", "", "", "", "manual_review"),
        ("ent-90d", "", "== \"enterprise\"", "<= 90", "", "", "approved"),
        ("std-defective", "", "== \"standard\"", "<= 30", "<= 500",
         "== \"defective\"", "approved"),
        ("premium-mindchange", "", "== \"premium\"", "<= 30",
         f"<= {premium_cap}", "== \"changed_mind\"", "approved"),
        ("std-late", "", "== \"standard\"", "<= 14", "",
         "== \"late_delivery\"", "approved"),
        ("fallback-deny", "", "", "", "", "", "denied"),
    ]
    jrules = []
    for name, c1, c2, c3, c4, c5, dec in rules:
        jrules.append({"_id": name, "c1": c1, "c2": c2, "c3": c3, "c4": c4,
                       "c5": c5, "o1": f"\"{dec}\"", "o2": f"\"{name}\""})
    return json.dumps({
        "edges": [{"id": "e1", "sourceId": "n-input", "targetId": "n-table"},
                  {"id": "e2", "sourceId": "n-table", "targetId": "n-output"}],
        "nodes": [
            {"id": "n-input", "type": "inputNode", "name": "Facts"},
            {"id": "n-output", "type": "outputNode", "name": "Decision"},
            {"id": "n-table", "type": "decisionTableNode",
             "name": "Acme refund rules",
             "content": {
                 "hitPolicy": "first",
                 "inputs": [
                     {"field": "prior_refunds_90d", "id": "c1", "name": "Prior refunds 90d", "type": "expression"},
                     {"field": "customer_tier", "id": "c2", "name": "Tier", "type": "expression"},
                     {"field": "days_since_purchase", "id": "c3", "name": "Days", "type": "expression"},
                     {"field": "order_amount_usd", "id": "c4", "name": "Amount", "type": "expression"},
                     {"field": "reason", "id": "c5", "name": "Reason", "type": "expression"}],
                 "outputs": [
                     {"field": "decision", "id": "o1", "name": "Decision", "type": "expression"},
                     {"field": "rule", "id": "o2", "name": "Rule fired", "type": "expression"}],
                 "rules": jrules}}]})


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

# 1. policies page lists the globals
r = c.get(f"{BASE}/policies")
check("policies page 200", r.status_code == 200, r.status_code)
check("refund policy listed", "Refund eligibility policy" in r.text)
check("triage policy listed", "Support ticket triage policy" in r.text)
check("new policy button", "New policy" in r.text)

# 2. create client-owned policy
r = c.post(f"{BASE}/policy/save", data={
    "id": "0", "name": "Acme refund policy",
    "description": "Acme's own refund rules — premium mind-change cap $300",
    "jdm": refund_jdm(300)})
loc = r.headers.get("location", "")
check("policy saved", r.status_code == 303 and "/policy/" in loc and "flash=policy+saved" in loc, loc)
pid = int(re.search(r"/policy/(\d+)", loc).group(1))
print("client policy id:", pid)

# 3. policies page shows it as mine
r = c.get(f"{BASE}/policies")
check("policy listed as mine", "Acme refund policy" in r.text and "mine" in r.text)

# 4. editor renders the model
r = c.get(f"{BASE}/policy/{pid}")
check("editor 200", r.status_code == 200, r.status_code)
check("editor grid", 'id="grid"' in r.text and "premium-mindchange" in r.text)
check("editor test panel", "Run test" in r.text)

# 5. test endpoint: premium $250 changed mind → approved by CLIENT rule
r = c.post(f"{BASE}/policy/{pid}/test", json={"facts": {
    "customer_tier": "premium", "days_since_purchase": 10,
    "order_amount_usd": 250, "reason": "changed_mind",
    "prior_refunds_90d": 0}})
j = r.json()
check("test 200", r.status_code == 200, r.text[:200])
check("test approved (client cap)", j.get("result", {}).get("decision") == "approved", str(j))
check("test rule premium-mindchange", j.get("result", {}).get("rule") == "premium-mindchange", str(j))
check("test policy name mine", j.get("policy") == "Acme refund policy v1", str(j.get("policy")))

# 5b. cross-check: GLOBAL policy denies the same facts
r = c.post(f"{BASE}/policy/1/test", json={"facts": {
    "customer_tier": "premium", "days_since_purchase": 10,
    "order_amount_usd": 250, "reason": "changed_mind",
    "prior_refunds_90d": 0}})
jg = r.json()
check("global denies same facts", jg.get("result", {}).get("decision") == "denied", str(jg))

# 6. launch refund template with the CLIENT policy
#    (match the template by its TITLE — the policy picker on every
#    decision form lists "Refund eligibility policy", so a bare "Refund"
#    match would hit the triage template too)
r = c.get(f"{BASE}/")
tids = set(re.findall(r"/launch/(\d+)", r.text))
tid = None
for t in tids:
    rf = c.get(f"{BASE}/launch/{t}")
    if "Refund eligibility decision</h2>" in rf.text:
        tid = t
        check("launch picker present", "decision policy to apply" in rf.text)
        check("picker lists client policy", "Acme refund policy" in rf.text)
        break
print("refund template id:", tid)
assert tid, "refund template not found by title"

REQUEST = ("Premium plan customer, Delta Inc. Order #7001 placed 10 days "
           "ago for $250. We changed our mind about this purchase — no "
           "fault with the product. No prior refunds this quarter. "
           "Requesting a refund.")
r = c.post(f"{BASE}/launch/{tid}", data={
    "request_text": REQUEST, "task_name": "Delta refund — Acme policy",
    "schedule": "manual", "policy_id": str(pid)})
check("launch 303", r.status_code == 303, r.status_code)

r = c.get(f"{BASE}/")
rows = re.findall(r"<tr><td>(.*?)</td>.*?href='/task/(\d+)'", r.text, re.S)
task_id = next((i for nm, i in rows if "Delta refund" in nm), None)
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
            check("CLIENT policy fired", "Acme refund policy" in txt, "policy name missing")
            check("decision approved (client cap)", "approved" in txt)
            check("rule premium-mindchange", "premium-mindchange" in txt)
            check("facts present", "Extracted facts" in txt)
            print(f"RUN: {BASE}/run/{run_id}")
            break
        print("    working...")

# 7. cleanup: delete the client policy (keep demo tidy)
r = c.post(f"{BASE}/policy/{pid}/delete")
check("policy deleted", r.status_code == 303, r.status_code)

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

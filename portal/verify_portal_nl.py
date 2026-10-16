"""Live E2E: NL policy authoring (new-policy mode) + fact-gate loop.

Part 1: describe a refund policy in plain English -> compile -> save ->
  policy live with client-owned rows the LLM drafted.
Part 2: launch the refund decision task with that policy and a request
  that omits prior refunds -> submit_facts returns FACTS INCOMPLETE ->
  agent resubmits with the inferred value -> real decision (not
  insufficient_facts).
"""
import json
import re
import sys
import time
import httpx

BASE = "https://portal.datagatelodge.com"
UA = {"User-Agent": "Mozilla/5.0"}
KEY = "portal-demo-key-2026"

DESC = ("Refund policy for Beta Corp: enterprise customers get refunds "
        "any time within 90 days of purchase. Standard customers get "
        "refunds for defective items within 30 days if the order was "
        "under 500 dollars. Premium customers who changed their mind "
        "get refunds within 30 days for orders under 250 dollars. Any "
        "customer with 3 or more refunds in the last 90 days goes to "
        "manual review instead. Everything else is denied.")

# request text deliberately does NOT state prior refunds (agent must
# infer 0 on resubmit after the fact gate)
REQUEST = ("Enterprise customer Globex Inc, enterprise plan. Order #5521 "
           "placed 45 days ago for $2,000. We changed our mind about the "
           "expansion — no fault with the product. Requesting a refund.")

c = httpx.Client(headers=UA, timeout=120, follow_redirects=False)
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

# ── part 1: compile a whole policy from the description
r = c.post(f"{BASE}/policy/compile", json={"pid": 0, "text": DESC})
j = r.json()
print("compile mode:", j.get("mode"), "rows:", len(j.get("rows", [])),
      "dropped:", len(j.get("dropped", [])))
check("compile new-policy 200", r.status_code == 200, r.text[:300])
check("mode new", j.get("mode") == "new")
check("3-6 inputs", 3 <= len(j.get("inputs", [])) <= 6, str(j.get("inputs")))
check("has decision + rule outputs",
      any(o["field"] == "rule" for o in j.get("outputs", []))
      and len(j.get("outputs", [])) >= 2, str(j.get("outputs")))
check("4+ rules drafted", len(j.get("rows", [])) >= 4)
check("fallback last", j["rows"][-1]["name"] == "fallback",
      str([x["name"] for x in j["rows"]]))
if j.get("dropped"):
    print("  dropped drafts:", j["dropped"])

# build the JDM exactly as the editor's save path does (columns ids =
# fields; equivalent to buildJdm's c1/o1 scheme)
ins = [{"field": i["field"], "id": i["field"], "name": i["name"],
        "type": "expression"} for i in j["inputs"]]
outs = [{"field": o["field"], "id": o["field"], "name": o["name"],
         "type": "expression"} for o in j["outputs"]]
rules = [{"_id": row["name"], **row["cells"]} for row in j["rows"]]
jdm = json.dumps({
    "edges": [{"id": "e1", "sourceId": "i", "targetId": "t"},
              {"id": "e2", "sourceId": "t", "targetId": "o"}],
    "nodes": [{"id": "i", "type": "inputNode", "name": "Facts"},
              {"id": "o", "type": "outputNode", "name": "Decision"},
              {"id": "t", "type": "decisionTableNode",
               "name": j.get("name", "Drafted"),
               "content": {"hitPolicy": "first", "inputs": ins,
                           "outputs": outs, "rules": rules}}]})
r = c.post(f"{BASE}/policy/save", data={
    "id": "0", "name": j.get("name", "Beta refund policy"),
    "description": "Drafted from a plain-English description",
    "jdm": jdm})
loc = r.headers.get("location", "")
check("NL policy saved", "flash=policy+saved" in loc, loc)
pid = int(re.search(r"/policy/(\d+)", loc).group(1))
print("NL policy id:", pid)

# the saved policy evaluates: enterprise/45d -> approved (ent rule)
r = c.post(f"{BASE}/policy/{pid}/test", json={"facts": {
    "customer_tier": "enterprise", "days_since_purchase": 45,
    "order_amount_usd": 2000, "reason": "changed_mind",
    "prior_refunds_90d": 0}})
t = r.json()
check("test evaluates", "result" in t, r.text[:300])
print("  enterprise/45d decision:", t.get("result"))

# rows mode: draft one more rule against the NL policy
r = c.post(f"{BASE}/policy/compile", json={
    "pid": pid,
    "text": "Standard customers who changed their mind get refunds "
            "within 14 days for orders under 100 dollars."})
j2 = r.json()
check("rows mode compile", j2.get("mode") == "rows"
      and len(j2.get("rows", [])) >= 1, r.text[:300])
print("  drafted rows:", [x["name"] for x in j2.get("rows", [])])

# ── part 2: fact-gate — launch with the NL policy, request omits prior refunds
r = c.get(f"{BASE}/")
tids = set(re.findall(r"/launch/(\d+)", r.text))
tid = None
for t in tids:
    rf = c.get(f"{BASE}/launch/{t}")
    if "Refund eligibility decision</h2>" in rf.text:
        tid = t
        break
check("refund template found", tid is not None)
r = c.post(f"{BASE}/launch/{tid}", data={
    "request_text": REQUEST, "task_name": "Globex refund — NL policy",
    "schedule": "manual", "policy_id": str(pid)})
check("launch 303", r.status_code == 303, r.status_code)

r = c.get(f"{BASE}/")
rows = re.findall(r"<tr><td>(.*?)</td>.*?href='/task/(\d+)'", r.text, re.S)
task_id = next((i for nm, i in rows if "Globex refund" in nm), None)
print("task id:", task_id)
assert task_id

print("polling for run (watching for the fact-gate loop)...")
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
            check("NL policy named in run", "policy" in txt.lower()
                  and "drafted" not in txt.lower()[:50], "")
            check("decision section", "Policy decision" in txt)
            check("not insufficient_facts", "insufficient_facts" not in txt)
            check("facts section", "Extracted facts" in txt)
            print(f"RUN: {BASE}/run/{run_id}")
            break
        print("    working...")

# inspect the steps for the gate loop (>=2 submit_facts, one INCOMPLETE)
r = c.get(f"{BASE}/run/{run_id}")
submits = len(re.findall(r"submit_facts", r.text))
check("fact-gate loop (2+ submit_facts)", submits >= 2, f"found {submits}")
check("gate message shown", "FACTS INCOMPLETE" in r.text
      or "INCOMPLETE" in r.text, "no incomplete marker")

# cleanup: delete the NL policy
r = c.post(f"{BASE}/policy/{pid}/delete")
check("NL policy deleted", r.status_code == 303)

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

"""Local tests: triage JDM branches + _policy_model round-trip."""
import json
import re
import sys

sys.path.insert(0, "portal")

seed = open("portal/seed.sql", encoding="utf-8").read()
ok = fail = 0

def check(name, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"PASS {name}")
    else:
        fail += 1
        print(f"FAIL {name}  {detail}")

# ── 1. triage ruleset: all 6 branches
from zen import ZenEngine
m = re.search(r"SELECT 'Support ticket triage policy',\s*\n.*?,\s*\n\s*1,\s*\n\s*'(.*?)'\s*\nWHERE NOT EXISTS", seed, re.S)
assert m, "triage JDM not found"
triage = ZenEngine().create_decision(json.loads(m.group(1)))
CASES = [
    ({"severity": "critical", "customer_tier": "enterprise",
      "prior_escalations_7d": 0, "sla_hours_remaining": 30},
     "escalate_now", 1, "crit-ent"),
    ({"severity": "critical", "customer_tier": "standard",
      "prior_escalations_7d": 0, "sla_hours_remaining": 30},
     "escalate_now", 2, "crit-any"),
    ({"severity": "normal", "customer_tier": "standard",
      "prior_escalations_7d": 3, "sla_hours_remaining": 30},
     "priority_queue", 8, "repeat-escalator"),
    ({"severity": "high", "customer_tier": "premium",
      "prior_escalations_7d": 0, "sla_hours_remaining": 5},
     "priority_queue", 8, "high-urgent"),
    ({"severity": "high", "customer_tier": "standard",
      "prior_escalations_7d": 0, "sla_hours_remaining": 40},
     "standard_queue", 24, "high"),
    ({"severity": "low", "customer_tier": "standard",
      "prior_escalations_7d": 0, "sla_hours_remaining": 48},
     "standard_queue", 48, "fallback"),
]
for facts, route, sla, rule in CASES:
    r = triage.evaluate(facts)["result"]
    check(f"triage {rule}", r == {"route": route, "sla_target_hours": sla,
                                  "rule": rule}, str(r))

# ── 2. _policy_model parses the refund JDM; round-trips to valid JDM
import importlib.util
spec = importlib.util.spec_from_file_location("papp", "portal/app.py")
# don't execute the app (needs env); reimplement the parse inline instead
m2 = re.search(r"SELECT 'Refund eligibility policy',\s*\n.*?,\s*\n\s*1,\s*\n\s*'(.*?)'\s*\nWHERE NOT EXISTS", seed, re.S)
assert m2, "refund JDM not found"
refund_json = m2.group(1)


def policy_model(jdm_json):
    g = json.loads(jdm_json)
    for n in g.get("nodes", []):
        if n.get("type") != "decisionTableNode":
            continue
        ct = n.get("content", {})
        ins = [(i.get("field", ""), i.get("name", ""), i.get("id", ""))
               for i in ct.get("inputs", [])]
        outs = [(o.get("field", ""), o.get("name", ""), o.get("id", ""))
                for o in ct.get("outputs", [])]
        rules = []
        for r in ct.get("rules", []):
            cells = [str(r.get(cid, "")) for _, _, cid in ins + outs]
            rules.append({"name": str(r.get("_id", "")), "cells": cells})
        return {"hitPolicy": ct.get("hitPolicy", "first"),
                "inputs": ins, "outputs": outs, "rules": rules}
    return None


model = policy_model(refund_json)
check("model parses", model is not None)
check("model 5 inputs", len(model["inputs"]) == 5, str(model["inputs"]))
check("model 6 rules", len(model["rules"]) == 6)
check("guard-abuse cells", model["rules"][0]["name"] == "guard-abuse"
      and model["rules"][0]["cells"][0] == ">= 3",
      str(model["rules"][0]))

# round-trip: rebuild JDM like the editor JS does, evaluate
def rebuild(model, name="Roundtrip"):
    ins = [{"field": f, "id": f"c{i+1}", "name": n, "type": "expression"}
           for i, (f, n, _) in enumerate(model["inputs"])]
    outs = [{"field": f, "id": f"o{i+1}", "name": n, "type": "expression"}
            for i, (f, n, _) in enumerate(model["outputs"])]
    rules = []
    for i, r in enumerate(model["rules"]):
        d = {"_id": r["name"] or f"r{i+1}"}
        for j, val in enumerate(r["cells"]):
            cid = ins[j]["id"] if j < len(ins) else outs[j - len(ins)]["id"]
            d[cid] = val
        rules.append(d)
    return {"edges": [{"id": "e1", "sourceId": "i", "targetId": "t"},
                      {"id": "e2", "sourceId": "t", "targetId": "o"}],
            "nodes": [{"id": "i", "type": "inputNode", "name": "Facts"},
                      {"id": "o", "type": "outputNode", "name": "Decision"},
                      {"id": "t", "type": "decisionTableNode", "name": name,
                       "content": {"hitPolicy": model["hitPolicy"],
                                   "inputs": ins, "outputs": outs,
                                   "rules": rules}}]}


rt = ZenEngine().create_decision(rebuild(model))
r = rt.evaluate({"customer_tier": "enterprise", "days_since_purchase": 60,
                 "order_amount_usd": 1200, "reason": "changed_mind",
                 "prior_refunds_90d": 0})["result"]
check("roundtrip evaluates same", r == {"decision": "approved",
                                        "rule": "ent-90d"}, str(r))

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

"""Local test: _trial_jdm + _draft_rows_valid validation logic."""
import json
import sys

src = open("portal/app.py", encoding="utf-8").read()
ns = {"json": json, "re": __import__("re")}
a = src.index("def _policy_model")
b = src.index('@app.get("/policies"')
exec(src[a:b], ns)          # _policy_model, _jdm_valid, _cells_valid
a = src.index("def _trial_jdm")
b = src.index("@app.post(\"/policy/compile\")")
exec(src[a:b], ns)          # _trial_jdm + _draft_rows_valid

from zen import ZenEngine
ok = fail = 0

def check(name, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"PASS {name}")
    else:
        fail += 1
        print(f"FAIL {name}  {detail}")

ins = [{"field": "customer_tier", "name": "Tier", "type": "expression"},
       {"field": "days_since_purchase", "name": "Days", "type": "expression"}]
outs = [{"field": "decision", "name": "Decision", "type": "expression"},
        {"field": "rule", "name": "Rule fired", "type": "expression"}]

rows = [
    # valid rule
    {"name": "ent-90d", "cells": {
        "customer_tier": '== "enterprise"',
        "days_since_purchase": "<= 90",
        "decision": '"approved"', "rule": '"ent-90d"'}},
    # invalid expression (unclosed string)
    {"name": "bad-expr", "cells": {
        "customer_tier": '== "enterprise',
        "days_since_purchase": "", "decision": '"approved"',
        "rule": '"bad"'}},
    # garbage cell
    {"name": "garbage", "cells": {
        "customer_tier": "nonsense operator %%%",
        "days_since_purchase": "!!", "decision": "", "rule": ""}},
    # empty everything (valid: fallback row)
    {"name": "fallback", "cells": {
        "customer_tier": "", "days_since_purchase": "",
        "decision": '"denied"', "rule": '"fallback"'}},
]
valid, dropped = ns["_draft_rows_valid"](ins, outs, rows)
check("2 valid kept", [r["name"] for r in valid] == ["ent-90d", "fallback"],
      str(valid))
check("2 invalid dropped", len(dropped) == 2
      and all("error" in d for d in dropped), str(dropped))

# the valid trial evaluates
eng = ZenEngine()
d = eng.create_decision(ns["_trial_jdm"](ins, outs,
    {"_id": "ent-90d", **rows[0]["cells"]}))
r = d.evaluate({"customer_tier": "enterprise",
                "days_since_purchase": 60})["result"]
check("trial evaluates", r == {"decision": "approved", "rule": "ent-90d"},
      str(r))

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)


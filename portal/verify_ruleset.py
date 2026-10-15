"""Test the EXACT ruleset JDM from portal/seed.sql: all branches + guards."""
import json
import re
import sys

seed = open("portal/seed.sql", encoding="utf-8").read()
m = re.search(r"SELECT 'Refund eligibility policy',\s*\n.*?,\s*\n\s*1,\s*\n\s*'(.*?)'\s*\nWHERE NOT EXISTS",
              seed, re.S)
assert m, "ruleset JDM not found in seed.sql"
jdm = m.group(1)
print("JDM extracted:", len(jdm), "chars")

from zen import ZenEngine
decision = ZenEngine().create_decision(json.loads(jdm))
print("create_decision: OK")

CASES = [
    ({"customer_tier": "enterprise", "days_since_purchase": 60,
      "order_amount_usd": 1200, "reason": "changed_mind",
      "prior_refunds_90d": 0}, "approved", "ent-90d"),
    ({"customer_tier": "standard", "days_since_purchase": 20,
      "order_amount_usd": 300, "reason": "defective",
      "prior_refunds_90d": 0}, "approved", "std-defective"),
    ({"customer_tier": "enterprise", "days_since_purchase": 5,
      "order_amount_usd": 50, "reason": "defective",
      "prior_refunds_90d": 4}, "manual_review", "guard-abuse"),
    ({"customer_tier": "premium", "days_since_purchase": 10,
      "order_amount_usd": 150, "reason": "changed_mind",
      "prior_refunds_90d": 0}, "approved", "premium-mindchange"),
    ({"customer_tier": "standard", "days_since_purchase": 10,
      "order_amount_usd": 800, "reason": "late_delivery",
      "prior_refunds_90d": 0}, "approved", "std-late"),
    ({"customer_tier": "standard", "days_since_purchase": 90,
      "order_amount_usd": 800, "reason": "changed_mind",
      "prior_refunds_90d": 0}, "denied", "fallback-deny"),
]
ok = fail = 0
for facts, want_d, want_r in CASES:
    r = decision.evaluate(facts)
    got = r["result"]
    good = got.get("decision") == want_d and got.get("rule") == want_r
    ok, fail = (ok + 1, fail) if good else (ok, fail + 1)
    print(f"{'PASS' if good else 'FAIL'} {want_d:14} -> {got} "
          f"({r.get('performance')})")

# missing-fields helper (mirrors _missing_fields in app.py)
g = json.loads(jdm)
need = [i.get("field", "") for n in g.get("nodes", [])
        if n.get("type") == "decisionTableNode"
        for i in n.get("content", {}).get("inputs", [])]
print("expected fields:", need)
missing = [f for f in need if f not in CASES[0][0]]
print("missing for case 0:", missing, "(expect [])")
assert not missing

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

"""Local test: the two DEFAULT_RULESETS policies evaluate correctly."""
import json
import re
import sys

src = open("helpdesk/app.py", encoding="utf-8").read()
ok = fail = 0

def check(name, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"PASS {name}")
    else:
        fail += 1
        print(f"FAIL {name}  {detail}")

# extract _table_jdm + DEFAULT_RULESETS from app.py without running the app
ns = {"json": json}
tree_start = src.index("def _table_jdm")
tree_end = src.index("def _sync_rulesets")
code = src[tree_start:tree_end]
exec(code, ns)  # defines _table_jdm + DEFAULT_RULESETS in ns

from zen import ZenEngine
eng = ZenEngine()

gate_specs = next(r for r in ns["DEFAULT_RULESETS"] if r[1] == "task-tool-gate")
triage_specs = next(r for r in ns["DEFAULT_RULESETS"] if r[1] == "email-triage")

gate = eng.create_decision(json.loads(
    ns["_table_jdm"](gate_specs[0], gate_specs[3], gate_specs[4],
                     gate_specs[5])))
tri = eng.create_decision(json.loads(
    ns["_table_jdm"](triage_specs[0], triage_specs[3], triage_specs[4],
                     triage_specs[5])))

# gate: email_send blocked when notify=false
r = gate.evaluate({"tool": "email_send", "notify": False})["result"]
check("gate blocks email_send/notify=false",
      r == {"allow": False, "rule": "no-notify-email"}, str(r))
# gate: email_send allowed when notify=true
r = gate.evaluate({"tool": "email_send", "notify": True})["result"]
check("gate allows email_send/notify=true",
      r == {"allow": True, "rule": "allow-all"}, str(r))
# gate: any other tool allowed regardless
r = gate.evaluate({"tool": "web_fetch", "notify": False})["result"]
check("gate allows web_fetch",
      r == {"allow": True, "rule": "allow-all"}, str(r))
r = gate.evaluate({"tool": "db_query", "notify": True})["result"]
check("gate allows db_query", r == {"allow": True, "rule": "allow-all"}, str(r))

# triage: legal → inbox
r = tri.evaluate({"confident": True, "legal": True, "kb_hits": 3})["result"]
check("triage legal → inbox",
      r == {"action": "inbox", "rule": "legal-queue"}, str(r))
# triage: no kb → inbox even if confident
r = tri.evaluate({"confident": True, "legal": False, "kb_hits": 0})["result"]
check("triage no-kb → inbox",
      r == {"action": "inbox", "rule": "no-kb"}, str(r))
# triage: confident + kb → auto_reply
r = tri.evaluate({"confident": True, "legal": False, "kb_hits": 2})["result"]
check("triage confident → auto_reply",
      r == {"action": "auto_reply", "rule": "confident-faq"}, str(r))
# triage: not confident → inbox
r = tri.evaluate({"confident": False, "legal": False, "kb_hits": 2})["result"]
check("triage not-confident → inbox",
      r == {"action": "inbox", "rule": "not-confident"}, str(r))

# URL-count regex sanity (deterministic check logic)
goal = ("Fetch each of these URLs with web_fetch and report: "
        "https://a.com https://b.com, https://c.com)\n"
        "and https://d.com")
urls = re.findall(r"https?://[^\s,)\"]+", goal)
check("url count regex = 4", len(urls) == 4, str(urls))

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

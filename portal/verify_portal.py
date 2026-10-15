"""Verify run viewer + task page + run-again as the client."""
import re
import sys
import httpx

BASE = "https://portal.datagatelodge.com"
UA = {"User-Agent": "Mozilla/5.0"}
KEY = "portal-demo-key-2026"

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

# dashboard shows the task with its outcome
r = c.get(f"{BASE}/")
check("dashboard lists task", "Estate health check" in r.text, "missing")
check("dashboard shows achieved", "achieved" in r.text, "no classification")

# task page
r = c.get(f"{BASE}/task/2")
check("task page 200", r.status_code == 200, r.status_code)
check("task page shows run", "#1" in r.text and "achieved" in r.text, "no run row")
check("run-again button", "Run again now" in r.text, "missing button")
check("run link present", "/run/1" in r.text, "no run link")

# run viewer
r = c.get(f"{BASE}/run/1")
check("run viewer 200", r.status_code == 200, r.status_code)
check("classification shown", "achieved" in r.text, "missing")
check("steps listed", "web_fetch" in r.text, "no steps")
check("report content", "Task Report" in r.text, "no report")
check("URL statuses in report", "datagatelodge.com" in r.text, "report missing URLs")

# templates gallery page
r = c.get(f"{BASE}/templates")
check("templates page 200", r.status_code == 200, r.status_code)
check("gallery has 6 templates", r.text.count("Use this template") == 6, r.text.count("Use this template"))
check("gallery not stub", "coming soon" not in r.text, "still stub")
check("decision template present", "Refund eligibility" in r.text, "missing")
check("triage template present", "Support ticket triage" in r.text, "missing")

# policies page (per-client rulesets)
r = c.get(f"{BASE}/policies")
check("policies page 200", r.status_code == 200, r.status_code)
check("global policies listed", "Refund eligibility policy" in r.text
      and "Support ticket triage policy" in r.text, "missing")
check("new policy button", "New policy" in r.text, "missing")

# decision run viewer (run 3 = Contoso refund decision, ent-90d)
r = c.get(f"{BASE}/run/3")
check("decision run viewer 200", r.status_code == 200, r.status_code)
check("policy decision section", "Policy decision" in r.text, "missing")
check("rule fired shown", "ent-90d" in r.text, "no rule id")
check("facts section", "Extracted facts" in r.text, "missing")
check("submit_facts step", "submit_facts" in r.text, "missing")
check("policy_check step", "policy_check" in r.text, "missing")
check("deterministic tag", "deterministic" in r.text, "missing")

# logout
r = c.get(f"{BASE}/logout")
check("logout", r.status_code == 303, r.status_code)

print(f"\n{ok} pass / {fail} fail")
sys.exit(1 if fail else 0)

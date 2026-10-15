"""Full E2E: launch template with real params, wait for run, verify viewer."""
import re
import sys
import time
import httpx

BASE = "https://portal.datagatelodge.com"
UA = {"User-Agent": "Mozilla/5.0"}
KEY = "portal-demo-key-2026"
URLS = ("https://datagatelodge.com\nhttps://get.datagatelodge.com\n"
        "https://chat.datagatelodge.com\nhttps://portal.datagatelodge.com")

c = httpx.Client(headers=UA, timeout=60, follow_redirects=False)
c.post(f"{BASE}/login", data={"key": KEY})

# find the Website health monitor template id on the gallery
r = c.get(f"{BASE}/")
tids = re.findall(r"/launch/(\d+)", r.text)
print("template ids on dashboard:", tids)

# launch template 1 with real URLs (template order: health monitor is first)
r = c.post(f"{BASE}/launch/1", data={
    "urls": URLS, "task_name": "Estate health check",
    "schedule": "manual"})
print("launch ->", r.status_code, r.headers.get("location"))

# find our task on the dashboard
r = c.get(f"{BASE}/")
rows = re.findall(r"<tr><td>(.*?)</td>.*?/task/(\d+)\">view</a>", r.text, re.S)
tid = None
for name, i in rows:
    if "Estate health check" in name:
        tid = i
        break
print("task id:", tid)
if not tid:
    sys.exit(1)

print("polling for run (runner 30s loop + agent execution) ...")
run_id = None
for attempt in range(40):
    time.sleep(15)
    r = c.get(f"{BASE}/task/{tid}")
    m = re.search(r"/run/(\d+)", r.text)
    cur = m.group(1) if m else None
    if cur != run_id:
        run_id = cur
        print(f"  poll {attempt+1}: run #{run_id} appeared")
    if run_id:
        rr = c.get(f"{BASE}/run/{run_id}")
        txt = rr.text
        if "achieved" in txt or "partial" in txt or "failed" in txt or ">review<" in txt:
            print("\n=== RUN COMPLETE ===")
            print(f"RUN VIEWER: {BASE}/run/{run_id}")
            cl = re.search(r"tag (\w+)'>(\w+)</span>", txt)
            print("classification:", cl.group(2) if cl else "?")
            print("has steps:", "web_fetch" in txt or "tool" in txt.lower())
            print("has report:", "Report" in txt)
            # pull the report text
            mrep = re.search(r"Report</h2>(.*?)</div>", txt, re.S)
            if mrep:
                rep = re.sub(r"<[^>]+>", " ", mrep.group(1))
                print("report excerpt:", " ".join(rep.split())[:400])
            break
        else:
            print(f"    run #{run_id} still executing ...")

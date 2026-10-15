"""Watch the Lodge Office inbox for a live email arriving through CF."""
import re
import time

import httpx

LODGE = "http://10.1.2.217:8111"
CRED = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"


def cred(name):
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"{name} not found")


ADMIN_KEY = cred("ADMIN_KEY")

print("watching for inbound mail (send your test email to "
      "support@datagatelodge.com now)...", flush=True)
deadline = time.time() + 150
seen = None
while time.time() < deadline:
    try:
        with httpx.Client(base_url=LODGE, timeout=20,
                          follow_redirects=True) as c:
            c.post("/login", data={"key": ADMIN_KEY})
            r = c.get("/inbox")
            txt = r.text
        # any inbound row created in the last 5 minutes?
        stamps = re.findall(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})", txt)
        recent = [s for s in stamps if s >= time.strftime(
            "%Y-%m-%d %H:%M", time.gmtime(time.time() - 300))]
        fresh_rows = len(recent)
        if fresh_rows and ("live test" in txt.lower()
                           or "answered" in txt):
            seen = txt
            break
    except Exception as e:
        print("poll error:", repr(e)[:100], flush=True)
    time.sleep(10)

if not seen:
    print("\nnothing arrived in the window — send the email and "
          "I'll check again")
    raise SystemExit(0)

# report what we saw
pending = re.findall(r"needs review", seen)
print("\nMAIL ARRIVED — current inbox state:")
plain = re.sub(r"<[^>]+>", " ", seen)
plain = " ".join(plain.split())
for m in re.finditer(r"(recently handled|needs review)", plain, re.I):
    pass
# summarize rows
rows = re.findall(r"<tr><td class='pill'>([^<]+)</td><td>([^<]*)</td>"
                  r"<td>(?:<span class=tag[^>]*>)?(\w+)", seen)
for frm, subject, status in rows[:6]:
    print(f"   {frm:32} {subject[:40]:42} {status}")
cards = len(re.findall(r"needs review", seen))
print(f"pending review cards: {cards}")

# was a reply sent?
with httpx.Client(base_url=LODGE, timeout=20, follow_redirects=True) as c:
    c.post("/login", data={"key": ADMIN_KEY})
    r = c.get("/emails")
    if "gatekeeper-email-reply" in r.text:
        print("reply send logged (gatekeeper-email-reply in email log)")
print("\nLIVE LOOP " + ("COMPLETE" if not cards else
                        "PARTIAL — mail queued for review (check the "
                        "question; confident ones auto-reply)"))

"""Check whether the owner's live test email made it through the chain."""
import re

import httpx

LODGE = "http://10.1.2.217:8111"
CRED = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"


def cred(name):
    for line in open(CRED, encoding="utf-8", errors="replace"):
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip()
    raise SystemExit(f"{name} not found")


ADMIN_KEY = cred("ADMIN_KEY")

with httpx.Client(base_url=LODGE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": ADMIN_KEY})

    r = c.get("/inbox")
    txt = r.text

    # pending review cards (from · date · topic + body)
    cards = re.findall(
        r"<span class=\"tag warn\">needs review</span>\s*"
        r"<span class=\"mono note\">([^<]+)</span>", txt)
    print("pending review cards:", len(cards))
    for card in cards:
        print("   ", card)

    bodies = re.findall(r"<p style=\"margin:.6rem 0[^\"]*\">([^<]{10,200})",
                        txt)
    for b in bodies[:5]:
        print("   body:", b[:120])

    # recently handled table rows
    rows = re.findall(
        r"<tr><td class='pill'>([^<]+)</td><td>([^<]*)</td>"
        r"<td>(?:<span class=tag[^>]*>)?(\w+)(?:</span>)?</td>"
        r"<td class='pill'>([^<]*)</td>"
        r"<td class='note'>([^<]*)</td></tr>", txt)
    print("\nrecently handled:")
    for frm, subject, status, doc, at in rows:
        print(f"   {frm:34} {subject[:38]:40} {status:10} {doc or '-':12} {at}")

    # reply send log
    r2 = c.get("/emails")
    sends = re.findall(r"gatekeeper-email-reply", r2.text)
    print(f"\ngatekeeper-email-reply sends logged: {len(sends)}")

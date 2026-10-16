"""Live verification of the governance pass (lodge-office:13 +
gatekeeper:18). Drives the real Lodge Office over the LAN with the
ADMIN_KEY; policy/KB writes use identical content (no behavior change).
"""
import html as _html
import re

import httpx

BASE = "http://10.1.2.217:8111"
CREDS = r"C:\Users\dlang\Workspace\gateway_secrets\gatekeeper-credentials.txt"
key = ""
for line in open(CREDS, encoding="utf-8", errors="replace"):
    if line.strip().startswith("ADMIN_KEY"):
        key = (line.split("=", 1)[1] if "=" in line
               else line.split(":", 1)[1]).strip()
        break

out = []
ok = fail = 0


def check(name, cond, extra=""):
    global ok, fail
    out.append(f"{'PASS' if cond else 'FAIL'} {name}"
               + ("" if cond else f"   {extra}"))
    ok, fail = ok + (1 if cond else 0), fail + (0 if cond else 1)


with httpx.Client(base_url=BASE, timeout=30, follow_redirects=True) as c:
    c.post("/login", data={"key": key})

    # 1. knowledge screen: security column + prompts section + badges
    r = c.get("/kb")
    check("kb: security column", "security" in r.text)
    check("kb: prompt templates section",
          "prompt templates" in r.text.lower()
          and "rerank" in r.text)
    check("kb: docs lint clean (no high findings)",
          " high</span>" not in r.text and "clean" in r.text,
          [m for m in re.findall(r"<td>(\d+ high|.{0,30}warn)", r.text)][:5])

    # 2. policies screen: the three system rulesets
    r = c.get("/policies")
    check("policies: system rulesets listed",
          "Task tool gate" in r.text
          and "Inbound email triage" in r.text
          and "Visitor refund eligibility" in r.text
          and "task-tool-gate" in r.text
          and "email-triage" in r.text
          and "chat-decision" in r.text)
    check("policies: danger framing present",
          "fail-open" in r.text)

    # 3. editor + test runner for the tool gate
    rid = re.search(r"/policy/(\d+)", r.text)
    rid = rid.group(1) if rid else ""
    r = c.get(f"/policy/{rid}")
    check("policy editor: grid renders",
          "no-notify-email" in r.text and 'value="tool"' in r.text
          and "allow-all" in r.text, f"rid={rid}")
    r = c.post(f"/policy/{rid}/test",
               json={"facts": {"tool": "email_send", "notify": False}})
    j = r.json() if r.status_code == 200 else {}
    check("policy test: gate blocks no-notify email",
          j.get("result", {}).get("rule") == "no-notify-email"
          and j.get("result", {}).get("allow") is False, str(j)[:120])
    r = c.post(f"/policy/{rid}/test",
               json={"facts": {"tool": "email_send", "notify": True}})
    j = r.json() if r.status_code == 200 else {}
    check("policy test: gate allows notify",
          j.get("result", {}).get("rule") == "allow-all", str(j)[:120])

    # 4. inbox: fired-rule column
    r = c.get("/inbox")
    check("inbox: fired-rule tags shown",
          "rule" in r.text.lower()
          and any(t in r.text for t in ("no-kb", "confident-faq",
                                        "not-confident", "legal-queue")))

    # 5. edit page: lint + provenance boxes
    r = c.get("/kb/edit", params={"name": "presales"})
    check("edit page: lint + history boxes",
          "Security lint" in r.text and "Provenance" in r.text)

    # 6. live provenance round-trip (identical content, no behavior change)
    m_t = re.search(r'name="title" value="([^"]*)"', r.text)
    m_d = re.search(r'name="description" value="([^"]*)"', r.text)
    m_c = re.search(r'<textarea name="content">(.*?)</textarea>',
                    r.text, re.S)
    title = _html.unescape(m_t.group(1)) if m_t else ""
    desc = _html.unescape(m_d.group(1)) if m_d else ""
    content = _html.unescape(m_c.group(1)) if m_c else ""
    r = c.post("/kb/save", data={"name": "presales", "title": title,
                                 "description": desc,
                                 "content": content})
    r = c.get("/kb/edit", params={"name": "presales"})
    vids = re.findall(r"/kb/version\?id=(\d+)", r.text)
    check("provenance: save recorded history", bool(vids), str(vids)[:80])
    if vids:
        r = c.get("/kb/version", params={"id": vids[0]})
        check("provenance: version view renders",
              r.status_code == 200 and "state after this action" in r.text)
        r = c.post("/kb/restore", data={"id": vids[0]})
        r = c.get("/kb/edit", params={"name": "presales"})
        check("provenance: restore recorded",
              "restore" in r.text)

    # 7. internal doc: lint skips internal-mention, history box present
    r = c.get("/kb/edit", params={"name": "support-internal"})
    check("internal doc: lint + history render",
          "Security lint" in r.text and "Provenance" in r.text)

out.append(f"\n{ok} pass / {fail} fail")
with open(r"C:\Users\dlang\AppData\Local\Temp\verify_gov.txt", "w",
          encoding="utf-8") as f:
    f.write("\n".join(out))
print("done")

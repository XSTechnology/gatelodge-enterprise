"""Local rehearsal for the email-delivery wiring (no real sends).

Run from licensing/:
    ..\\.venv\\Scripts\\python.exe test_email_flow.py

Covers:
  1. trial request  -> lead row + email_sent False (emailer disabled)
  2. forged signed webhook -> key minted, JWT claims verified
  3. same event retried -> SAME serial, no second license (idempotency)
  4. email outage (monkeypatched transport) -> webhook + trial still 200,
     email_log rows ok=0 (failure isolation)
  5. both templates rendered to preview files
"""
import hashlib
import hmac
import json
import os
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
TD = Path(tempfile.mkdtemp(prefix="lic_email_"))
os.environ["DATA_DIR"] = str(TD)
os.environ["STRIPE_WEBHOOK_SECRET"] = "whsec_test"
os.environ["BINARY_PATH"] = ""          # download 404s; not under test
os.environ.pop("RESEND_API_KEY", None)
os.environ.pop("EMAIL_PROVIDER", None)
os.environ.pop("TURNSTILE_SECRET", None)   # dev mode: verification skipped

# throwaway RSA signing key (same shape as the real license key)
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_key_path = TD / "test_key.pem"
_key_path.write_bytes(_key.private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8,
    serialization.NoEncryption()))
os.environ["LICENSE_PRIVATE_KEY"] = str(_key_path)

import app                                  # noqa: E402 (env set first)
import emailer                              # noqa: E402
import jwt as pyjwt                         # noqa: E402
from fastapi.testclient import TestClient   # noqa: E402

fails = []


def check(name, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + name +
          ("" if cond else f"   {extra}"))
    if not cond:
        fails.append(name)


client = TestClient(app.app)

# ── 1. trial request ──────────────────────────────────────────────────────
r = client.post("/api/trial-request",
                json={"email": "buyer@example.com",
                      "business_name": "Acme"})
check("trial 200", r.status_code == 200, r.text)
check("trial email_sent false (disabled)", r.json().get("email_sent") is False)
with app.db() as c:
    check("lead row written",
          c.execute("SELECT COUNT(*) FROM leads").fetchone()[0] == 1)

# ── 2. forged signed webhook -> mint ──────────────────────────────────────


def post_event(event_id: str, buyer: str):
    ev = {"id": event_id, "type": "checkout.session.completed",
          "data": {"object": {"customer_details": {"email": buyer}}}}
    payload = json.dumps(ev).encode()
    t = str(int(time.time()))
    sig = hmac.new(b"whsec_test", f"{t}.".encode() + payload,
                   hashlib.sha256).hexdigest()
    return client.post("/api/stripe/webhook", content=payload,
                       headers={"stripe-signature": f"t={t},v1={sig}"})


r = post_event("evt_test_001", "buyer@example.com")
check("webhook 200", r.status_code == 200, r.text)
rec = r.json()
check("serial GL- prefixed", rec.get("serial", "").startswith("GL-"), rec)
check("license_json returned",
      rec.get("license_json", "").startswith('{\n  "license"'), rec)
check("email echoed", rec.get("email") == "buyer@example.com")

tok = json.loads(rec["license_json"])["license"]
claims = pyjwt.decode(tok, _key.public_key(), algorithms=["RS256"])
check("jwt serial matches", claims["serial"] == rec["serial"])
check("jwt providers = 9", len(claims["providers"]) == 9)
check("jwt perpetual", claims["exp"] - claims["iat"] > 99 * 365 * 86400)

# ── 3. retried event -> idempotent ────────────────────────────────────────
r2 = post_event("evt_test_001", "buyer@example.com")
check("retry 200", r2.status_code == 200, r2.text)
check("retry same serial", r2.json()["serial"] == rec["serial"])
with app.db() as c:
    check("exactly one license row",
          c.execute("SELECT COUNT(*) FROM licenses").fetchone()[0] == 1)
    row = c.execute("SELECT license_json FROM licenses").fetchone()
    check("license_json persisted", bool(row[0]))

# ── 4. email outage -> graceful failure ───────────────────────────────────
emailer.RESEND_API_KEY = "dummy"
emailer.EMAIL_PROVIDER = "resend"

_orig_post = emailer.httpx.AsyncClient.post


async def _post_fail(self, url, **kwargs):
    raise OSError("network down (simulated)")


emailer.httpx.AsyncClient.post = _post_fail

r3 = post_event("evt_test_002", "buyer2@example.com")
check("webhook 200 despite email outage", r3.status_code == 200, r3.text)
check("second key minted", r3.json()["serial"] != rec["serial"])

r4 = client.post("/api/trial-request",
                 json={"email": "trial2@example.com"})
check("trial 200 despite email outage", r4.status_code == 200, r4.text)
check("trial email_sent false on outage",
      r4.json().get("email_sent") is False)

emailer.httpx.AsyncClient.post = _orig_post

with app.db() as c:
    rows = c.execute("SELECT kind, to_addr, serial, ok, detail "
                     "FROM email_log ORDER BY id").fetchall()
check("email_log has failed purchase row",
      any(x[0] == "purchase" and x[3] == 0 for x in rows), str(rows))
check("email_log has failed trial row",
      any(x[0] == "trial" and x[3] == 0 for x in rows), str(rows))
check("failure detail captured",
      all("network down" in (x[4] or "") for x in rows), str(rows))

# ── 5. render previews ────────────────────────────────────────────────────
out = TD / "previews"
out.mkdir()
s1, h1, txt1 = emailer.render_purchase(rec["serial"], rec["license_json"])
(out / "purchase.html").write_text(h1, encoding="utf-8")
(out / "purchase.txt").write_text(txt1, encoding="utf-8")
s2, h2, txt2 = emailer.render_trial()
(out / "trial.html").write_text(h2, encoding="utf-8")
(out / "trial.txt").write_text(txt2, encoding="utf-8")

check("purchase subject carries serial", rec["serial"] in s1)
check("templates substantial", len(h1) > 2000 and len(h2) > 2000)
for name, doc in (("purchase", h1), ("trial", h2)):
    check(f"{name} tables balanced",
          doc.count("<table") == doc.count("</table>"))
    check(f"{name} serial/ticket present",
          ("GATE PASS" in doc) or ("TRIAL PASS" in doc) or
          ("DOWNLOAD GATELODGE" in doc))
    check(f"{name} legal footer", "XS Technologies Inc." in doc)
    check(f"{name} eula link", "/eula" in doc)
check("purchase license block present", "license.json" in h1)
check("trial download link", "/download/community" in h2)

print()
print("previews :", out)
print("db       :", TD / "leads.db")
if fails:
    print("FAILED:", ", ".join(fails))
    sys.exit(1)
print("ALL PASS")

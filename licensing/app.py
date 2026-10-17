"""Gatelodge licensing service — trial download + (phase 2) paid-key issuance.

Endpoints:
  GET  /healthz               — liveness
  GET  /api/config            — public config for the site (payment link etc.)
  POST /api/trial-request     — email capture (Turnstile-verified) -> lead
                                record -> download grant (+ emailed
                                download link when Resend configured)
  GET  /download/community    — streams the community binary (rate limited)
  POST /api/license/recovery — lost-key re-send (Turnstile-verified,
                                anti-enumeration: same response whether
                                or not the address holds a key)
  POST /api/stripe/webhook    — phase 2: payment -> perpetual license key
                                + delivery email (activates when
                                STRIPE_WEBHOOK_SECRET set)

Env:
  TURNSTILE_SECRET     — Cloudflare Turnstile secret key (unset = dev mode,
                         verification skipped with a warning)
  ALLOWED_ORIGINS      — comma-separated CORS origins
  BINARY_PATH          — path to the community .exe served for download
  DATA_DIR             — writable dir for the sqlite lead store
  PAYMENT_LINK         — Stripe payment-link URL shown by the site ("" = none)
  STRIPE_WEBHOOK_SECRET— Stripe signing secret (phase 2; unset = 503)
  LICENSE_PRIVATE_KEY  — path to the RS256 private key PEM (phase 2)
  DATABASE_URL         — Postgres (phase 3; unset = sqlite in DATA_DIR)
  ADMIN_KEY            — bearer for POST /api/admin/issue (manual keys)
  EMAIL_PROVIDER       — "resend" to enable transactional email (see
                         emailer.py for RESEND_API_KEY, EMAIL_FROM,
                         EMAIL_REPLY_TO, PUBLIC_BASE_URL, SITE_URL;
                         unset = email disabled, everything else works)
  PORT                 — default 8100
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from pathlib import Path

import httpx
import jwt

import emailer
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

# ── config ────────────────────────────────────────────────────────────────

TURNSTILE_SECRET = os.environ.get("TURNSTILE_SECRET", "")
ALLOWED_ORIGINS = [o.strip() for o in
                   os.environ.get("ALLOWED_ORIGINS",
                                  "https://datagatelodge.com").split(",")
                   if o.strip()]
BINARY_PATH = os.environ.get("BINARY_PATH", "")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
PAYMENT_LINK = os.environ.get("PAYMENT_LINK", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
LICENSE_PRIVATE_KEY = os.environ.get("LICENSE_PRIVATE_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")     # unset = sqlite
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")           # /api/admin/issue
PORT = int(os.environ.get("PORT", "8100"))

DATA_DIR.mkdir(parents=True, exist_ok=True)
DB = DATA_DIR / "leads.db"

if DATABASE_URL:
    import psycopg

    class _Pg:
        """sqlite3-compatible facade over psycopg: translates '?'
        placeholders, commits + closes on context exit."""

        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, params=()):
            if "INSERT OR IGNORE" in sql:
                sql = (sql.replace("INSERT OR IGNORE INTO",
                                   "INSERT INTO")
                       + " ON CONFLICT DO NOTHING")
            return self._conn.execute(sql.replace("?", "%s"),
                                      params or ())

        def __enter__(self):
            return self

        def __exit__(self, et, ev, tb):
            try:
                if et is None:
                    self._conn.commit()
            finally:
                self._conn.close()

ALL_CLOUD_PROVIDERS = ["openai", "anthropic", "gemini", "responses",
                       "azure", "vertex", "bedrock", "runpod",
                       "runpod-public"]

DURATIONS = {"30d": 30, "60d": 60, "90d": 90, "1y": 365, "2y": 730,
             "3y": 1095, "perpetual": 100 * 365}

app = FastAPI(title="Gatelodge Licensing", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── store: Postgres when DATABASE_URL is set, sqlite otherwise ────────────


def db():
    if DATABASE_URL:
        return _Pg(psycopg.connect(DATABASE_URL))
    c = sqlite3.connect(DB)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def _contact_upsert(c, email: str, name: str = "", company: str = "",
                    source: str = "") -> None:
    """Add/refresh an address-book contact (auto-capture). Works on both
    sqlite and Postgres (ON CONFLICT upsert)."""
    import datetime
    email = (email or "").strip().lower()[:200]
    if not email or "@" not in email:
        return
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(
        timespec="seconds")
    c.execute(
        "INSERT INTO contacts (email, name, company, source, first_seen,"
        " last_seen, message_count) VALUES (?, ?, ?, ?, ?, ?, 1) "
        "ON CONFLICT(email) DO UPDATE SET "
        "name = CASE WHEN EXCLUDED.name != '' THEN EXCLUDED.name "
        "ELSE contacts.name END, "
        "company = CASE WHEN EXCLUDED.company != '' THEN EXCLUDED.company "
        "ELSE contacts.company END, "
        "source = CASE WHEN contacts.source IN ('', 'manual') AND "
        "EXCLUDED.source != '' THEN EXCLUDED.source ELSE contacts.source "
        "END, "
        "last_seen = EXCLUDED.last_seen, "
        "message_count = contacts.message_count + 1",
        (email, (name or "").strip()[:120], (company or "").strip()[:160],
         source, now, now))


_CONTACTS_PG = """CREATE TABLE IF NOT EXISTS contacts (
    id BIGSERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT DEFAULT '',
    company TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    tags TEXT DEFAULT '',
    source TEXT DEFAULT 'manual',
    first_seen TEXT DEFAULT '',
    last_seen TEXT DEFAULT '',
    message_count INTEGER DEFAULT 0
)"""
_CONTACTS_SQLITE = """CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    name TEXT DEFAULT '',
    company TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    tags TEXT DEFAULT '',
    source TEXT DEFAULT 'manual',
    first_seen TEXT DEFAULT '',
    last_seen TEXT DEFAULT '',
    message_count INTEGER DEFAULT 0
)"""


def init_db() -> None:
    if DATABASE_URL:
        with db() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS leads (
                id BIGSERIAL PRIMARY KEY,
                email TEXT NOT NULL,
                business_name TEXT DEFAULT '',
                serial TEXT DEFAULT '',
                edition TEXT DEFAULT 'community-trial',
                source TEXT DEFAULT 'website',
                ip TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT now()
            )""")
            c.execute("""CREATE TABLE IF NOT EXISTS licenses (
                id BIGSERIAL PRIMARY KEY,
                serial TEXT UNIQUE NOT NULL,
                email TEXT NOT NULL,
                edition TEXT NOT NULL,
                kind TEXT NOT NULL,
                stripe_event TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT now(),
                license_json TEXT DEFAULT '',
                business_name TEXT DEFAULT '',
                revoked BOOLEAN DEFAULT FALSE
            )""")
            c.execute("""CREATE TABLE IF NOT EXISTS email_log (
                id BIGSERIAL PRIMARY KEY,
                kind TEXT NOT NULL,
                to_addr TEXT NOT NULL,
                serial TEXT DEFAULT '',
                ok INTEGER DEFAULT 0,
                detail TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT now(),
                subject TEXT DEFAULT '',
                body TEXT DEFAULT ''
            )""")
            c.execute(_CONTACTS_PG)
            c.execute("ALTER TABLE email_log "
                      "ADD COLUMN IF NOT EXISTS subject TEXT DEFAULT ''")
            c.execute("ALTER TABLE email_log "
                      "ADD COLUMN IF NOT EXISTS body TEXT DEFAULT ''")
        return
    with db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            business_name TEXT DEFAULT '',
            serial TEXT DEFAULT '',
            edition TEXT DEFAULT 'community-trial',
            source TEXT DEFAULT 'website',
            ip TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS licenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            serial TEXT UNIQUE NOT NULL,
            email TEXT NOT NULL,
            edition TEXT NOT NULL,
            kind TEXT NOT NULL,
            stripe_event TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS email_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            to_addr TEXT NOT NULL,
            serial TEXT DEFAULT '',
            ok INTEGER DEFAULT 0,
            detail TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        )""")
        c.execute(_CONTACTS_SQLITE)
        for stmt in ("ALTER TABLE licenses "
                     "ADD COLUMN license_json TEXT DEFAULT ''",
                     "ALTER TABLE licenses "
                     "ADD COLUMN business_name TEXT DEFAULT ''",
                     "ALTER TABLE licenses "
                     "ADD COLUMN revoked BOOLEAN DEFAULT 0",
                     "ALTER TABLE email_log "
                     "ADD COLUMN subject TEXT DEFAULT ''",
                     "ALTER TABLE email_log "
                     "ADD COLUMN body TEXT DEFAULT ''"):
            try:
                c.execute(stmt)
            except sqlite3.OperationalError:
                pass


init_db()

# ── naive rate limiting (per-IP, in-memory; fronted by Cloudflare anyway) ─

_HITS: dict[str, list[float]] = {}


def rate_ok(ip: str, limit: int, window_s: int = 3600,
            bucket: str = "") -> bool:
    now = time.time()
    key = f"{ip}|{bucket}"
    hits = [t for t in _HITS.get(key, []) if now - t < window_s]
    if len(hits) >= limit:
        _HITS[key] = hits
        return False
    hits.append(now)
    _HITS[key] = hits
    return True


def client_ip(request: Request) -> str:
    return (request.headers.get("cf-connecting-ip")
            or request.client.host if request.client else "?")


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# ── turnstile ─────────────────────────────────────────────────────────────


async def turnstile_ok(token: str, ip: str) -> bool:
    if not TURNSTILE_SECRET:
        print("WARNING: TURNSTILE_SECRET unset — verification SKIPPED "
              "(dev mode)")
        return True
    try:
        r = await _client().post(
            "https://challenges.cloudflare.com/turnstile/v0/siteverify",
            data={"secret": TURNSTILE_SECRET, "response": token,
                  "remoteip": ip})
        return bool(r.json().get("success"))
    except Exception:
        return False


def _client() -> httpx.AsyncClient:
    # lazy per-request client (module-level client blocks test teardown)
    return httpx.AsyncClient(timeout=15)


# ── endpoints ─────────────────────────────────────────────────────────────


class TrialRequest(BaseModel):
    email: str = ""
    business_name: str = ""
    turnstile_token: str = ""


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "gatelodge-licensing"}


@app.get("/", include_in_schema=False)
async def root():
    """get.datagatelodge.com lands on the site's download section —
    the download itself lives here, the pitch lives there."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(
        "https://datagatelodge.com/gatelodge.html#download",
        status_code=302)


@app.get("/api/config")
async def config():
    return {"payment_link": PAYMENT_LINK,
            "turnstile_sitekey": os.environ.get("TURNSTILE_SITEKEY", "")}


@app.post("/api/trial-request")
async def trial_request(body: TrialRequest, request: Request):
    ip = client_ip(request)
    if not rate_ok(ip, limit=5, bucket="trial"):
        raise HTTPException(429, "too many requests — try again later")
    email = body.email.strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(400, "a valid email address is required")
    if not await turnstile_ok(body.turnstile_token, ip):
        raise HTTPException(403, "verification failed — please retry")

    with db() as c:
        c.execute("INSERT INTO leads (email, business_name, ip) "
                  "VALUES (?, ?, ?)",
                  (email, body.business_name.strip()[:120], ip))
        _contact_upsert(c, email, company=body.business_name,
                        source="trial")
    email_sent = False
    if emailer.configured():
        subject, html_body, text_body = emailer.render_trial()
        email_sent, detail = await emailer.send(email, subject, html_body,
                                                text_body)
        print(f"[email] trial -> {email}: "
              f"{'OK ' + detail if email_sent else 'FAIL ' + detail}")
        with db() as c:
            c.execute("INSERT INTO email_log "
                      "(kind, to_addr, serial, ok, detail, subject, body) "
                      "VALUES ('trial', ?, '', ?, ?, ?, ?)",
                      (email, int(email_sent), detail[:300], subject,
                       text_body[:20000]))
    return {
        "ok": True,
        "download": "/download/community",
        "email_sent": email_sent,
        "note": ("The Community binary runs a full-feature 30-day trial "
                 "from first boot — no license file needed."),
    }


@app.get("/download/community")
async def download_community(request: Request):
    ip = client_ip(request)
    if not rate_ok(ip, limit=10, bucket="dl"):
        raise HTTPException(429, "too many downloads — try again later")
    if not BINARY_PATH or not os.path.exists(BINARY_PATH):
        raise HTTPException(404, "binary not configured")
    name = os.path.basename(BINARY_PATH)
    # the shipped artifact is the zipped dist folder (standalone build:
    # the exe needs its DLL/.pyd siblings — a bare exe cannot run)
    media = ("application/zip" if name.endswith(".zip")
             else "application/octet-stream")
    return FileResponse(BINARY_PATH, media_type=media, filename=name)


# ── lost-key recovery ─────────────────────────────────────────────────────


class RecoveryRequest(BaseModel):
    email: str = ""
    turnstile_token: str = ""


@app.post("/api/license/recovery")
async def license_recovery(body: RecoveryRequest, request: Request):
    """Re-send a buyer's license key by email.

    Anti-enumeration by design: the response is identical whether or not
    the address holds a key — only a real match sends mail."""
    ip = client_ip(request)
    if not rate_ok(ip, limit=3, bucket="recovery"):
        raise HTTPException(429, "too many requests — try again later")
    email = body.email.strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(400, "a valid email address is required")
    if not await turnstile_ok(body.turnstile_token, ip):
        raise HTTPException(403, "verification failed — please retry")
    if emailer.configured():
        with db() as c:
            row = c.execute(
                "SELECT serial, license_json FROM licenses "
                "WHERE email = ? AND license_json != '' "
                "ORDER BY id DESC LIMIT 1", (email,)).fetchone()
        if row:
            subject, html_body, text_body = emailer.render_purchase(
                row[0], row[1])
            ok, detail = await emailer.send(email, subject, html_body,
                                            text_body)
            print(f"[email] recovery {row[0]} -> {email}: "
                  f"{'OK ' + detail if ok else 'FAIL ' + detail}")
            with db() as c:
                c.execute("INSERT INTO email_log "
                          "(kind, to_addr, serial, ok, detail, subject, body)"
                          " VALUES ('recovery', ?, ?, ?, ?, ?, ?)",
                          (email, row[0], int(ok), detail[:300], subject,
                           text_body[:20000]))
                _contact_upsert(c, email, source="purchase")
    return {"ok": True}


# ── phase 2: stripe webhook -> perpetual community license key ────────────


def _mint(email: str, edition: str = "community",
          business_name: str = "", providers: list | None = None,
          duration_days: int = 100 * 365, kind: str = "paid-key",
          stripe_event: str = "") -> dict:
    """Mint a signed license and store it. Shared by the Stripe webhook
    and the admin-issue endpoint. Claims mirror scripts/license_tool.py:
    RS256, kid = sha16 of the public key DER."""
    if not LICENSE_PRIVATE_KEY or not os.path.exists(LICENSE_PRIVATE_KEY):
        raise HTTPException(503, "license issuance not configured")
    import hashlib
    import secrets

    from cryptography.hazmat.primitives import serialization

    with open(LICENSE_PRIVATE_KEY, "rb") as f:
        priv = serialization.load_pem_private_key(f.read(), password=None)
    pub_der = priv.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo)
    kid = hashlib.sha256(pub_der).hexdigest()[:16]

    now = int(time.time())
    serial = "GL-" + secrets.token_hex(6).upper()
    claims = {
        "product": "gatelodge",
        "edition": edition,
        "serial": serial,
        "business_name": business_name,
        "contract_email": email,
        "vendor": "-",
        "support": False,
        "providers": providers if providers is not None
        else ALL_CLOUD_PROVIDERS,
        "grace_hours": 0,
        "iat": now,
        "nbf": now,
        "exp": now + duration_days * 24 * 3600,
    }
    token = jwt.encode(claims, priv, algorithm="RS256",
                       headers={"kid": kid})
    license_json = json.dumps({"license": token}, indent=2)
    with db() as c:
        c.execute("INSERT OR IGNORE INTO licenses "
                  "(serial, email, edition, kind, stripe_event, "
                  "license_json, business_name) "
                  "VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (serial, email, edition, kind, stripe_event,
                   license_json, business_name))
    return {"serial": serial, "license_json": license_json,
            "email": email}


def issue_paid_key(email: str, stripe_event: str = "") -> dict:
    """Mint a perpetual community License Key (all cloud providers named).

    Idempotent per Stripe event — a retried webhook delivery returns the
    key minted for that event instead of issuing a second one."""
    if stripe_event:
        with db() as c:
            row = c.execute(
                "SELECT serial, license_json FROM licenses "
                "WHERE stripe_event = ? AND kind = 'paid-key'",
                (stripe_event,)).fetchone()
        if row and row[1]:
            return {"serial": row[0], "license_json": row[1],
                    "email": email}
    return _mint(email, kind="paid-key", stripe_event=stripe_event)


async def deliver_purchase(rec: dict) -> None:
    """Email the license key. Never raises — a failed email must not fail
    the webhook (the key stays in the store for a later resend)."""
    if not emailer.configured():
        print(f"[email] purchase {rec['serial']} -> {rec['email']}: "
              "SKIPPED (emailer disabled)")
        return
    with db() as c:
        sent = c.execute(
            "SELECT 1 FROM email_log "
            "WHERE serial = ? AND kind = 'purchase' AND ok = 1",
            (rec["serial"],)).fetchone()
    if sent:
        return                        # already delivered for this key
    subject, html_body, text_body = emailer.render_purchase(
        rec["serial"], rec["license_json"])
    ok, detail = await emailer.send(rec["email"], subject, html_body,
                                    text_body)
    print(f"[email] purchase {rec['serial']} -> {rec['email']}: "
          f"{'OK ' + detail if ok else 'FAIL ' + detail}")
    with db() as c:
        c.execute("INSERT INTO email_log "
                  "(kind, to_addr, serial, ok, detail, subject, body) "
                  "VALUES ('purchase', ?, ?, ?, ?, ?, ?)",
                  (rec["email"], rec["serial"], int(ok), detail[:300],
                   subject, text_body[:20000]))
        _contact_upsert(c, rec["email"], company=rec.get("business_name", ""),
                        source="purchase")


@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(503, "stripe not configured")
    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")

    # verify signature (Stripe's t=...,v1=... scheme)
    import hashlib
    import hmac
    try:
        parts = dict(p.split("=", 1) for p in sig.split(","))
        t, v1 = parts["t"], parts["v1"]
    except Exception:
        raise HTTPException(400, "bad signature header")
    signed = f"{t}.".encode() + payload
    expect = hmac.new(STRIPE_WEBHOOK_SECRET.encode(), signed,
                      hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expect, v1):
        raise HTTPException(400, "signature mismatch")

    event = json.loads(payload)
    if event.get("type") == "checkout.session.completed":
        session = event.get("data", {}).get("object", {})
        email = (session.get("customer_details") or {}).get("email", "")
        if email:
            rec = issue_paid_key(email, event.get("id", ""))
            await deliver_purchase(rec)
            return rec
    return {"received": True}


# ── admin: manual license issue (LAN-side admin service only) ─────────────


class AdminIssueRequest(BaseModel):
    email: str = ""
    edition: str = "community"          # community | enterprise
    business_name: str = ""
    duration: str = "perpetual"         # 30d/60d/90d/1y/2y/3y/perpetual
    providers: list[str] = []           # backend names or cloud types


def _require_admin(request: Request) -> None:
    if not ADMIN_KEY:
        raise HTTPException(503, "admin not configured")
    auth = request.headers.get("authorization", "")
    if auth != f"Bearer {ADMIN_KEY}":
        raise HTTPException(401, "invalid admin key")


@app.post("/api/admin/issue")
async def admin_issue(body: AdminIssueRequest, request: Request):
    _require_admin(request)
    if not rate_ok(client_ip(request), limit=30, bucket="admin"):
        raise HTTPException(429, "too many requests")
    email = body.email.strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(400, "a valid email address is required")
    if body.edition not in ("community", "enterprise"):
        raise HTTPException(400, "edition must be community or enterprise")
    if body.duration not in DURATIONS:
        raise HTTPException(400, f"duration must be one of "
                                 f"{', '.join(DURATIONS)}")
    providers = [p.strip() for p in body.providers if p.strip()] or None
    rec = _mint(email, edition=body.edition,
                business_name=body.business_name.strip()[:120],
                providers=providers,
                duration_days=DURATIONS[body.duration],
                kind="manual")
    if emailer.configured():
        subject, html_body, text_body = emailer.render_purchase(
            rec["serial"], rec["license_json"])
        ok, detail = await emailer.send(rec["email"], subject, html_body,
                                        text_body)
        print(f"[email] manual {rec['serial']} -> {rec['email']}: "
              f"{'OK ' + detail if ok else 'FAIL ' + detail}")
        with db() as c:
            c.execute("INSERT INTO email_log "
                      "(kind, to_addr, serial, ok, detail, subject, body) "
                      "VALUES ('manual-issue', ?, ?, ?, ?, ?, ?)",
                      (rec["email"], rec["serial"], int(ok),
                       detail[:300], subject, text_body[:20000]))
            _contact_upsert(c, rec["email"],
                            company=rec.get("business_name", ""),
                            source="purchase")
    return {"ok": True, "serial": rec["serial"],
            "license_json": rec["license_json"]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

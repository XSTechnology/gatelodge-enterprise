"""Gatelodge licensing service — transactional email (Resend).

Delivers the purchase receipt (perpetual license key) and the trial
download link. Provider: Resend (https://resend.com) via plain httpx —
no SDK. Sends NEVER raise: a failed email must not fail the webhook or
the trial request; results are returned as (ok, detail) and logged by
the caller.

Env:
  EMAIL_PROVIDER   — "resend" to enable; unset/anything else = disabled
                     (auto-selects "resend" when RESEND_API_KEY is set)
  RESEND_API_KEY   — API key from the Resend dashboard
  EMAIL_FROM       — default "Gatelodge Licensing <license@datagatelodge.com>"
  SUPPORT_EMAIL    — published support address used for Reply-To and body
                     text; default "support@datagatelodge.com" (receives
                     via Cloudflare Email Routing -> sales@xs.technology)
  EMAIL_REPLY_TO   — default derives from SUPPORT_EMAIL
  PUBLIC_BASE_URL  — public origin of this service (download links),
                     default "https://get.datagatelodge.com"
  SITE_URL         — marketing site origin (EULA links),
                     default "https://datagatelodge.com"
"""
from __future__ import annotations

import html as _html
import os
import time

import httpx

# ── config ────────────────────────────────────────────────────────────────

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
EMAIL_PROVIDER = os.environ.get("EMAIL_PROVIDER",
                                "resend" if RESEND_API_KEY else "")
EMAIL_FROM = os.environ.get(
    "EMAIL_FROM", "Gatelodge Licensing <license@datagatelodge.com>")
SUPPORT_EMAIL = os.environ.get("SUPPORT_EMAIL",
                               "support@datagatelodge.com")
EMAIL_REPLY_TO = os.environ.get("EMAIL_REPLY_TO", SUPPORT_EMAIL)
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL",
                                 "https://get.datagatelodge.com").rstrip("/")
SITE_URL = os.environ.get("SITE_URL",
                          "https://datagatelodge.com").rstrip("/")

# ── palette (gatehouse aesthetic; email clients strip web fonts, so
#    Georgia stands in for Fraunces and Courier for IBM Plex Mono) ─────────

_INK = "#0e1411"
_BRASS = "#d4af37"
_PAPER = "#f5f0e6"
_MUTE = "#5f6d66"
_RULE = "#d8d0bd"
_TICKET = "#fffdf4"

# ── client ────────────────────────────────────────────────────────────────


def configured() -> bool:
    return EMAIL_PROVIDER.lower() == "resend" and bool(RESEND_API_KEY)


async def send(to: str, subject: str, html: str,
               text: str) -> tuple[bool, str]:
    """Deliver one message via Resend. Returns (ok, detail); never raises."""
    if not configured():
        return False, ("email disabled (EMAIL_PROVIDER unset or "
                       "RESEND_API_KEY missing)")
    payload = {"from": EMAIL_FROM, "reply_to": EMAIL_REPLY_TO,
               "to": [to], "subject": subject, "html": html, "text": text}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(
                "https://api.resend.com/emails",
                json=payload,
                headers={"Authorization": f"Bearer {RESEND_API_KEY}"})
        if r.status_code in (200, 201):
            return True, r.json().get("id", "")
        return False, f"resend {r.status_code}: {r.text[:200]}"
    except Exception as e:                       # noqa: BLE001
        return False, f"send error: {e!r}"


# ── shared chrome ─────────────────────────────────────────────────────────


def _shell(preheader: str, body: str) -> str:
    esc = _html.escape
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DataGateLodge</title></head>
<body style="margin:0;padding:0;background:#e9e3d3;">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;">{esc(preheader)}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#e9e3d3;">
<tr><td align="center" style="padding:28px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="width:600px;max-width:100%;background:{_PAPER};border:1px solid {_INK};">
<tr><td style="background:{_INK};padding:26px 32px;border-bottom:3px solid {_BRASS};">
  <div style="font-family:Georgia,'Times New Roman',serif;font-size:21px;letter-spacing:3px;color:{_PAPER};">DATAGATELODGE</div>
  <div style="font-family:'Courier New',Courier,monospace;font-size:11px;letter-spacing:4px;color:{_BRASS};padding-top:7px;">LICENSE DELIVERY</div>
</td></tr>
<tr><td style="padding:34px 34px 6px;">{body}</td></tr>
<tr><td style="padding:18px 34px 30px;">
  <div style="border-top:1px solid {_RULE};padding-top:16px;font-family:Arial,Helvetica,sans-serif;font-size:11px;line-height:18px;color:{_MUTE};">
    <span style="font-family:'Courier New',Courier,monospace;color:{_INK};letter-spacing:1px;">datagatelodge.solutions</span><br>
    a division of XS Technologies Inc., a Delaware corporation<br>
    &copy; 2026 XS Technologies Inc. &nbsp;&middot;&nbsp; <a href="{SITE_URL}/eula" style="color:{_MUTE};">EULA</a> &nbsp;&middot;&nbsp; {SUPPORT_EMAIL}
  </div>
</td></tr>
</table>
</td></tr></table>
</body></html>"""


def _ledger(rows: list[tuple[str, str]]) -> str:
    """Two-column receipt rows on ledger rules."""
    out = [f'<table role="presentation" width="100%" cellpadding="0" '
           f'cellspacing="0" border="0" style="margin-top:26px;">']
    for label, value in rows:
        out.append(f"""
<tr>
  <td style="font-family:'Courier New',Courier,monospace;font-size:11px;letter-spacing:2px;color:{_MUTE};padding:9px 0;border-top:1px solid {_RULE};width:110px;">{label}</td>
  <td align="right" style="font-family:Arial,Helvetica,sans-serif;font-size:12px;color:{_INK};padding:9px 0;border-top:1px solid {_RULE};">{value}</td>
</tr>""")
    out.append("</table>")
    return "".join(out)


# ── purchase receipt ──────────────────────────────────────────────────────


def render_purchase(serial: str, license_json: str) -> tuple[str, str, str]:
    """Subject, HTML and plain-text for the paid-key delivery email."""
    esc = _html.escape
    dl = f"{PUBLIC_BASE_URL}/download/community"
    today = time.strftime("%Y-%m-%d")

    body = f"""
<p style="font-family:Georgia,'Times New Roman',serif;font-size:19px;line-height:28px;color:{_INK};margin:0 0 12px;">Thank you &mdash; your pass is issued.</p>
<p style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:22px;color:{_MUTE};margin:0 0 26px;">Your Gatelodge Community perpetual license is confirmed. This email is the only place your key is delivered &mdash; keep it somewhere safe.</p>

<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
<td style="border:2px dashed {_BRASS};background:{_TICKET};padding:26px 20px;text-align:center;">
  <div style="font-family:'Courier New',Courier,monospace;font-size:11px;letter-spacing:4px;color:{_MUTE};">GATE PASS &middot; PERPETUAL</div>
  <div style="font-family:Georgia,'Times New Roman',serif;font-size:27px;letter-spacing:2px;color:{_INK};padding:9px 0 11px;">{esc(serial)}</div>
  <div style="display:inline-block;border:2px solid {_BRASS};color:{_BRASS};font-family:Arial,Helvetica,sans-serif;font-size:10px;font-weight:bold;letter-spacing:5px;padding:4px 8px 4px 13px;">CLEARED</div>
</td>
</tr></table>

<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:28px;">
<tr>
  <td style="width:34px;vertical-align:top;font-family:'Courier New',Courier,monospace;font-size:13px;color:{_BRASS};padding:0 10px 12px 0;">01</td>
  <td style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:22px;color:{_INK};padding-bottom:12px;">Download the binary &mdash; <a href="{dl}" style="color:{_INK};text-decoration:underline;">gatelodge-community.zip</a>. Unzip it, then run <span style="font-family:'Courier New',Courier,monospace;font-size:13px;">gatelodge-community.exe</span> from the unzipped folder.</td>
</tr>
<tr>
  <td style="width:34px;vertical-align:top;font-family:'Courier New',Courier,monospace;font-size:13px;color:{_BRASS};padding:0 10px 12px 0;">02</td>
  <td style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:22px;color:{_INK};padding-bottom:12px;">Save the license block below as <span style="font-family:'Courier New',Courier,monospace;font-size:13px;">license.json</span> in Gatelodge&rsquo;s data folder (the <span style="font-family:'Courier New',Courier,monospace;font-size:13px;">data</span> directory beside the executable &mdash; create it if it doesn&rsquo;t exist).</td>
</tr>
<tr>
  <td style="width:34px;vertical-align:top;font-family:'Courier New',Courier,monospace;font-size:13px;color:{_BRASS};padding:0 10px 0 0;">03</td>
  <td style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:22px;color:{_INK};">Start (or restart) Gatelodge. The console License tile shows your serial and expiry.</td>
</tr>
</table>

<div style="font-family:'Courier New',Courier,monospace;font-size:11px;letter-spacing:3px;color:{_MUTE};padding:28px 0 10px;">LICENSE FILE &mdash; SAVE AS license.json</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
<td style="background:{_INK};padding:16px 18px;">
<code style="font-family:'Courier New',Courier,monospace;font-size:12px;line-height:19px;color:{_RULE};word-break:break-all;white-space:pre-wrap;display:block;">{esc(license_json)}</code>
</td>
</tr></table>
""" + _ledger([
        ("ITEM", "Gatelodge Community &mdash; perpetual license key"),
        ("AMOUNT", "US$99.00 &middot; one-time"),
        ("DATE", esc(today)),
        ("SERIAL", esc(serial)),
    ])

    text = f"""DATAGATELODGE — LICENSE DELIVERY

Thank you — your pass is issued.

GATE PASS · PERPETUAL
Serial: {serial}  [CLEARED]

1) Download the binary:
   {dl}

2) Save the license block below as "license.json" in Gatelodge's
   data folder (the "data" directory beside the executable —
   create it if it doesn't exist).

3) Start (or restart) Gatelodge. The console License tile shows
   your serial and expiry.

------------------------- license.json -------------------------
{license_json}
------------------------------------------------------------------

Questions? {SUPPORT_EMAIL}

datagatelodge.solutions
a division of XS Technologies Inc., a Delaware corporation
(c) 2026 XS Technologies Inc.
"""
    subject = f"Your Gatelodge license key — {serial}"
    return subject, _shell(
        "Your perpetual license key is enclosed — keep this email.",
        body), text


# ── trial download ────────────────────────────────────────────────────────


def render_trial() -> tuple[str, str, str]:
    """Subject, HTML and plain-text for the trial download email."""
    dl = f"{PUBLIC_BASE_URL}/download/community"
    body = f"""
<p style="font-family:Georgia,'Times New Roman',serif;font-size:19px;line-height:28px;color:{_INK};margin:0 0 12px;">Thank you &mdash; your trial pass is ready.</p>
<p style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:22px;color:{_MUTE};margin:0 0 8px;">The Gatelodge Community binary runs the <b style="color:{_INK};">full feature set as a 30-day trial</b> from first boot. No license file needed &mdash; download and run.</p>

<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:24px 0 10px;"><tr>
<td style="background:{_INK};">
<a href="{dl}" style="display:inline-block;padding:15px 34px;font-family:Arial,Helvetica,sans-serif;font-size:13px;font-weight:bold;letter-spacing:3px;color:{_BRASS};text-decoration:none;">DOWNLOAD GATELODGE</a>
</td></tr></table>

<p style="font-family:'Courier New',Courier,monospace;font-size:12px;line-height:19px;color:{_MUTE};margin:0 0 22px;">or paste into a browser:<br>{dl}</p>

<p style="font-family:Arial,Helvetica,sans-serif;font-size:13px;line-height:20px;color:{_MUTE};margin:0;">Keep this email &mdash; the link re-downloads the same binary whenever you need it. When the trial earns its keep, a perpetual key is US$99 from the site.</p>
""" + _ledger([
        ("EDITION", "Gatelodge Community"),
        ("TRIAL", "30 days &middot; full feature set"),
        ("PLATFORM", "Windows &middot; 64-bit"),
    ])

    text = f"""DATAGATELODGE — YOUR TRIAL DOWNLOAD

Thank you — your trial pass is ready.

The Gatelodge Community binary runs the full feature set as a
30-day trial from first boot. No license file needed.

Download:
{dl}

Keep this email — the link re-downloads the same binary whenever
you need it. When the trial earns its keep, a perpetual key is
US$99 from the site.

Questions? {SUPPORT_EMAIL}

datagatelodge.solutions
a division of XS Technologies Inc., a Delaware corporation
(c) 2026 XS Technologies Inc.
"""
    subject = "Your Gatelodge Community trial — download link"
    return subject, _shell("Your 30-day trial download link is inside.",
                           body), text

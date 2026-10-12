"""Gatelodge binary licensing — signed license files + community trial.

v1.5 "closed distribution": both editions ship as COMPILED binaries
(Nuitka — machine code, no shipped source or bytecode). Source is
disclosed only under contract / external-audit conditions. The
licensing FRAMEWORK is core (both editions enforce it); the commercial
POLICY lives in the entry points:

    community binary (gateway.__main__.main):
        bootstrap(edition="community") — no license → 30-day trial
        anchored to first boot (clock-rollback watched). An expired
        trial keeps the process up but the middleware answers 403
        license_expired on everything except /healthz, so operators
        can see WHY serving stopped. A valid license (community or
        enterprise claims) removes the limit and carries entitlements
        (feature keys, vendor/support tie-in — e.g. a channel build
        distributed with "runpod support enabled").

    enterprise binary (the fork's enterprise_main):
        bootstrap(edition="enterprise") + enterprise_boot_gate() —
        refuses to boot unless a valid ENTERPRISE-edition license is
        active. No trial. Feature claims gate tier_d / scim /
        session_budgets.

TOKEN FORMAT (owner decision 2026-10-11): the license file is
PLAINTEXT/readable claims — operators can see their own entitlements
without tooling. Authenticity = RSA signature: the SHA-256 hash of
the contents is signed with the vendor PRIVATE key ("encrypted with
the private key") and verified with the embedded PUBLIC key — a
plain RS256 JWT (PyJWT). Tampering with any claim breaks the
signature; forging requires the vendor private key (offline
custody).

CLAIMS: product, edition, business_name (v1: customer),
contract_email, vendor?, support, features {code: Y/N} over
FEATURE_CODES (v1 list = all Y), providers [backend names] —
PER-BACKEND cloud entitlement: embedded + llama-server are FREE
(local compute); openai / runpod / runpod-public backends serve only
when their NAME is in the claim (provider_allowed). exp? (absent =
perpetual; the tool issues 30d/60d/90d/1y/2y/3y presets), nbf, iat,
node? (node-lock fingerprint).

The signing keypair comes from ``scripts/license_tool.py keygen``;
the PRIVATE half lives in vendor_keys/ (gitignored — offline
custody); this module embeds the PUBLIC half. Tests swap the key via
set_license_public_key() — they never need the vendor private key.

Trial-anchor honesty: an offline trial cannot be made tamper-proof
(the box is the attacker's). data/.trial_anchor.json records
first_boot + max_seen; a wall-clock rollback beyond the 5-minute
tolerance flips the binary to expired. Deleting the anchor resets
the trial — accepted risk; the compiled binary is the real wall.

Running from source (dev/tests) never bootstraps → the middleware
passes through ("unmanaged") — same doctrine as the other seams:
behavior is injected at boot by the entry point, not at import.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import jwt

from .config import PROJECT_ROOT

logger = logging.getLogger(__name__)

PRODUCT = "gatelodge"
EDITIONS = ("community", "enterprise")
TRIAL_DAYS_DEFAULT = 30
ROLLBACK_TOLERANCE_S = 300        # wall clock may drift; 5 min is hostile
_RECHECK_S = 60.0                 # middleware re-evaluates at most 1x/min

# Feature-code registry — the codes a license may switch Y/N. One
# list shared by the tool (issue-time validation) and the gateway
# (status surfaces the catalog). New codes ride a license-tool +
# licensing.py bump together.
FEATURE_CODES = ("tier_d", "scim", "session_budgets", "mcp", "teams")

# Provider entitlement (v2.2): local compute is FREE; cloud provider
# backends serve only under their per-backend license claim.
LOCAL_PROVIDER_TYPES = frozenset({"embedded", "llama-server"})
CLOUD_PROVIDER_TYPES = frozenset({"openai", "runpod", "runpod-public"})

# The vendor's PUBLIC license-signing key (scripts/license_tool.py
# keygen prints it; the private half never leaves vendor_keys/).
_EMBEDDED_PUBLIC_KEY_PEM = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAn4cO+k/7CCmRhWRr/Lv+
jlB3dmDaNDkpukjaMdpHY8OEU1hlf2RooiK0b3QDhoayzxRfQqAfi4coz1KDQ1oH
aIis3gbQ3O2gLshg4RCuZvXJMnvs23eTM4iS+v0y8u+m6D+LB9uQgEmxYyvArl30
WZ88iGUdPoeHCyoKJig/1uCMsIXQZhNgREL0ZskjlW2Fz0Ns3XEW+QBiasnS3yqx
odqdeqxMx629j8zLjd0I8QGUKHHBDWULuejldzrgHJ01Rd1EYdBznXA1YJrR3PA2
Fcff0UYGSVlouEKDOW+dZzqUtoRuoeH2KST54QsPXNZR6KAtdr8xIZaN0bm/bv+C
FwIDAQAB
-----END PUBLIC KEY-----"""

# ── module state (seam-style: injected at boot, swappable in tests) ────────

_PUBLIC_KEY_PEM: str = _EMBEDDED_PUBLIC_KEY_PEM
_STATE: dict[str, Any] | None = None    # None = unmanaged (source/tests)
_CACHE: tuple[float, tuple[bool, str]] | None = None


def _default_license_path() -> Path:
    return PROJECT_ROOT / "data" / "license.json"


def _default_anchor_path() -> Path:
    return PROJECT_ROOT / "data" / ".trial_anchor.json"


def node_fingerprint() -> str:
    """Stable-per-host identity for node-locked licenses (sha16).
    Containers with volatile hostnames pin it via
    GATELODGE_NODE_FINGERPRINT."""
    override = (os.environ.get("GATELODGE_NODE_FINGERPRINT") or "").strip()
    if override:
        return override.lower()
    import platform
    import uuid
    raw = f"{platform.node()}|{uuid.getnode():012x}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# ── license loading + verification ─────────────────────────────────────────

def _read_license_file(p: Path) -> tuple[str | None, str]:
    """data/license.json holds {"license": "<jwt>"} (a bare JWT text
    file is accepted too). Returns (token, detail)."""
    try:
        text = p.read_text(encoding="utf-8").strip()
    except OSError as e:
        return None, f"unreadable license file ({e.__class__.__name__})"
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except ValueError:
            return None, "license file is not valid JSON"
        tok = (data.get("license") or data.get("token") or "").strip()
        return (tok or None), ("" if tok else "license file has no license field")
    return (text or None), ""


def _load_license_token(explicit_path: Path | None
                        ) -> tuple[str | None, str | None, str]:
    """Resolve the license token: explicit path → GATELODGE_LICENSE
    (an existing file path OR an inline JWT — container-friendly) →
    data/license.json. Returns (token, source, detail)."""
    if explicit_path is not None:
        tok, detail = _read_license_file(explicit_path)
        return tok, "file", detail
    env = (os.environ.get("GATELODGE_LICENSE") or "").strip()
    if env:
        p = Path(env)
        if p.is_file():
            tok, detail = _read_license_file(p)
            return tok, "env:file", detail
        return env, "env:inline", ""
    default = _default_license_path()
    if default.is_file():
        tok, detail = _read_license_file(default)
        return tok, "file", detail
    return None, None, "no license present"


def _verify_token(token: str, now: datetime) -> tuple[dict | None, str]:
    """RS256 verification against the ACTIVE public key + claim sanity:
    the license is plaintext claims; the SHA-256 hash of the contents
    is signed with the vendor private key and verified here with the
    embedded public key. PyJWT enforces exp/nbf; alg confusion is
    impossible (algorithms is pinned). Returns (claims, '') or
    (None, reason)."""
    try:
        claims = jwt.decode(token, _PUBLIC_KEY_PEM, algorithms=["RS256"])
    except jwt.ExpiredSignatureError:
        return None, "license expired"
    except jwt.InvalidTokenError as e:
        return None, f"invalid license ({e.__class__.__name__})"
    if claims.get("product") != PRODUCT:
        return None, "license is for a different product"
    if claims.get("edition") not in EDITIONS:
        return None, "license carries no recognized edition"
    node = claims.get("node")
    if node and str(node).lower() != node_fingerprint():
        return None, "license is node-locked to another deployment"
    return claims, ""


# ── trial anchor ───────────────────────────────────────────────────────────

def _parse_iso(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _write_anchor(p: Path, first: datetime, max_seen: datetime) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({
            "first_boot": first.isoformat(),
            "max_seen": max_seen.isoformat()}), encoding="utf-8")
    except OSError:
        logger.debug("trial anchor not writable (%s) — rollback watch "
                     "degraded", p, exc_info=True)


def _trial_state(anchor: Path, trial_days: int, now: datetime
                 ) -> tuple[dict[str, Any], str]:
    """Evaluate/refresh the trial anchor. Returns (trial_info, mode)
    where mode is 'trial' or 'expired' — a wall-clock rollback beyond
    the tolerance is treated as expiry (tamper)."""
    data: dict[str, Any] = {}
    try:
        data = json.loads(anchor.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        data = {}
    first = _parse_iso(data.get("first_boot"))
    max_seen = _parse_iso(data.get("max_seen"))
    if first is None:
        first, max_seen, mode = now, now, "trial"
    else:
        if max_seen and now < max_seen - timedelta(seconds=ROLLBACK_TOLERANCE_S):
            mode = "expired"        # the clock ran backwards — hostile
            max_seen = max(max_seen, now) if max_seen else now
        else:
            mode = "trial" if (now - first) < timedelta(days=trial_days) \
                else "expired"
            max_seen = max(max_seen or first, now)
    _write_anchor(anchor, first, max_seen)
    left = math.ceil(((first + timedelta(days=trial_days)) - now)
                     .total_seconds() / 86400)
    return ({"trial_days": trial_days, "first_boot": first.isoformat(),
             "days_left": max(0, min(left, trial_days))}, mode)


# ── boot + runtime evaluation ──────────────────────────────────────────────

def _trial_days_from(trial_days: int | None) -> int:
    if trial_days is not None:
        return max(1, int(trial_days))
    raw = (os.environ.get("GATELODGE_TRIAL_DAYS") or "").strip()
    try:
        return max(1, int(raw)) if raw else TRIAL_DAYS_DEFAULT
    except ValueError:
        return TRIAL_DAYS_DEFAULT


def bootstrap(*, edition: str = "community",
              license_path: Path | None = None,
              anchor_path: Path | None = None,
              trial_days: int | None = None,
              now: datetime | None = None) -> dict[str, Any]:
    """Load + verify the license (or evaluate the community trial) and
    install the process-wide licensing state. Called ONCE by the
    binary's entry point before serving. Returns the public status
    (see status()). Never raises — a broken license is a state, not a
    crash."""
    global _STATE, _CACHE
    now = now or datetime.now(timezone.utc)
    edition = edition if edition in EDITIONS else "community"
    days = _trial_days_from(trial_days)
    anchor = anchor_path or _default_anchor_path()

    token, source, detail = _load_license_token(license_path)
    claims: dict | None = None
    if token:
        claims, detail = _verify_token(token, now)
    if claims is not None:
        exp = claims.get("exp")
        _STATE = {"edition": edition, "mode": "licensed", "license": claims,
                  "source": source, "detail": "", "trial": None}
        logger.info(
            "License: %s-edition license active (customer=%r vendor=%r "
            "support=%s expires=%s)", claims.get("edition"),
            claims.get("customer"), claims.get("vendor") or "-",
            bool(claims.get("support")),
            "perpetual" if exp is None
            else datetime.fromtimestamp(exp, tz=timezone.utc).isoformat())
    elif edition == "enterprise":
        # enterprise binaries never trial — the entry point's boot gate
        # turns this state into a refusal to serve at all.
        _STATE = {"edition": edition, "mode": "unlicensed",
                  "license": None, "source": source,
                  "detail": detail or "no valid license", "trial": None}
    else:
        trial, mode = _trial_state(anchor, days, now)
        _STATE = {"edition": edition, "mode": mode, "license": None,
                  "source": None, "detail": detail or "", "trial": trial,
                  "anchor_path": str(anchor)}
        if token and detail:
            logger.warning("License: ignoring invalid license (%s) — "
                           "community trial applies", detail)
        if mode == "trial":
            logger.info("License: no license — community trial, %d day(s) "
                        "left (first boot %s)", trial["days_left"],
                        trial["first_boot"])
        else:
            logger.warning("License: TRIAL EXPIRED — /healthz stays up, "
                           "every other route answers 403 license_expired")
    _CACHE = None
    return status()


def ensure_bootstrapped(*, edition: str = "community") -> dict[str, Any]:
    """bootstrap(), but idempotent — the entry-point CHAIN (the fork's
    enterprise_main → core __main__.main) must not clobber an already-
    installed state with community semantics. Direct bootstrap() calls
    always re-run (tests, re-issuing); entry points use this."""
    if _STATE is None:
        return bootstrap(edition=edition)
    return status()


def _recheck(state: dict[str, Any], now: datetime) -> tuple[bool, str]:
    """Runtime (re)evaluation — license expiry mid-run, trial window
    and clock-rollback while serving. Well-defined failures only;
    unexpected errors fail OPEN (a licensing bug must not brick a
    paying deployment) and are logged."""
    try:
        mode = state.get("mode")
        if mode == "licensed":
            exp = (state.get("license") or {}).get("exp")
            if exp is not None and now.timestamp() >= float(exp):
                logger.warning("License: EXPIRED while running — serving "
                               "disabled (403 license_expired)")
                return False, "expired"
            return True, "licensed"
        if mode == "trial":
            trial, m = _trial_state(Path(state["anchor_path"]),
                                    state["trial"]["trial_days"], now)
            state["trial"] = trial
            state["mode"] = m
            if m != "trial":
                logger.warning("License: TRIAL EXPIRED while running — "
                               "serving disabled (403 license_expired)")
                return False, m
            return True, "trial"
        return False, str(mode or "unlicensed")
    except Exception:  # noqa: BLE001 — fail open, log once per cache miss
        logger.exception("license recheck failed — allowing (fail-open)")
        return True, "licensed"


def request_allowed() -> tuple[bool, str]:
    """The middleware's cheap gate: (allowed, mode), re-evaluated at
    most once per _RECHECK_S. Unmanaged (source/tests, pre-bootstrap)
    always allows — the entry point installs the state before serving.
    """
    global _CACHE
    state = _STATE
    if state is None:
        return True, "unmanaged"
    mono = time.monotonic()
    if _CACHE is not None and mono - _CACHE[0] < _RECHECK_S:
        return _CACHE[1]
    result = _recheck(state, datetime.now(timezone.utc))
    _CACHE = (mono, result)
    return result


def _features_map(claims: dict[str, Any]) -> dict[str, bool]:
    """Normalize the features claim: v2 {code: Y/N} map (dropping
    unknown/false codes is the CALLER's choice — this returns the raw
    map) or the v1 list (present = Y)."""
    features = claims.get("features")
    if isinstance(features, dict):
        return {str(k): bool(v) for k, v in features.items()}
    if isinstance(features, list):
        return {str(c): True for c in features}
    return {}


def allows(feature: str) -> bool:
    """Feature-claim gate (community Pro extras; enterprise modules).
    True only under an ACTIVE license whose features carry the code as
    Y — v2 map {code: Y/N} or v1 list (present = Y). Trial/unlicensed/
    expired grant nothing."""
    state = _STATE
    if not state or state.get("mode") != "licensed":
        return False
    return _features_map(state.get("license") or {}).get(feature, False)


def provider_allowed(name: str, ptype: str) -> bool:
    """Per-backend provider entitlement (v2.2):

    embedded + llama-server (LOCAL compute) are FREE — always allowed;

    openai / runpod / runpod-public (CLOUD providers) serve only when
    the ACTIVE license's ``providers`` claim names this backend.

    Posture: unmanaged (source/tests) → no gate; TRIAL → the full
    product (evaluation); expired/unlicensed → nothing (the license
    middleware stops serving anyway — this returns False as defense
    in depth)."""
    state = _STATE
    if state is None:
        return True                    # unmanaged — source/tests
    mode = state.get("mode")
    if mode == "trial":
        return True                    # trial = full evaluation
    if mode != "licensed":
        return False                   # expired/unlicensed
    if ptype in LOCAL_PROVIDER_TYPES:
        return True                    # local compute: free
    providers = (state.get("license") or {}).get("providers") or []
    return (isinstance(providers, list)
            and str(name) in [str(p) for p in providers])


def status() -> dict[str, Any]:
    """Public licensing status (GET /admin/license). Claims are safe to
    surface here — the endpoint sits behind the admin pipeline + RBAC
    like every /admin GET."""
    state = _STATE
    if state is None:
        return {"mode": "unmanaged", "edition": None, "customer": None,
                "detail": "not bootstrapped (running from source)"}
    lic = state.get("license") or {}
    exp = lic.get("exp")
    trial = state.get("trial") or {}
    providers = [str(p) for p in (lic.get("providers") or [])
                 if str(p).strip()]
    business = lic.get("business_name") or lic.get("customer")
    return {
        "mode": state.get("mode"),
        "edition": state.get("edition"),
        "license_edition": lic.get("edition"),
        "business_name": business,
        "contract_email": lic.get("contract_email"),
        "customer": business,           # legacy alias
        "vendor": lic.get("vendor"),
        "support": bool(lic.get("support")),
        "features": _features_map(lic),
        "feature_codes": list(FEATURE_CODES),
        "providers": providers,
        "local_provider_types": sorted(LOCAL_PROVIDER_TYPES),
        "cloud_provider_types": sorted(CLOUD_PROVIDER_TYPES),
        "expires_at": datetime.fromtimestamp(exp, tz=timezone.utc).isoformat()
        if exp is not None else None,
        "perpetual": bool(lic) and exp is None,
        "node_locked": bool(lic.get("node")),
        "trial_days_left": trial.get("days_left"),
        "source": state.get("source"),
        "detail": state.get("detail") or None,
    }


def healthz_license() -> dict[str, Any]:
    """/healthz-safe subset — NO customer/vendor names (healthz is
    unauthenticated)."""
    state = _STATE
    if state is None:
        return {"mode": "unmanaged"}
    out: dict[str, Any] = {"mode": state.get("mode")}
    trial = state.get("trial")
    if trial:
        out["trial_days_left"] = trial.get("days_left")
    return out


def enterprise_boot_gate(state: dict[str, Any] | None = None) -> None:
    """Refuse to boot unless a valid ENTERPRISE-edition license is
    active. Invoked by the commercial entry point (the fork's
    enterprise_main) — enterprise binaries never run trial mode.
    Raises SystemExit(2) with a clear log line."""
    st = state or status()
    if st.get("mode") == "licensed" and st.get("license_edition") == "enterprise":
        return
    logger.critical(
        "Gatelodge Enterprise requires an enterprise license — no trial "
        "mode. Install a signed license at data/license.json (or set "
        "GATELODGE_LICENSE to a file path / inline JWT) and restart. "
        "[mode=%s license_edition=%s%s]",
        st.get("mode"), st.get("license_edition"),
        f" detail={st['detail']}" if st.get("detail") else "")
    raise SystemExit(2)


# ── test seams (same doctrine as set_storage / set_auth_pipeline) ──────────

def set_license_public_key(pem: str) -> None:
    """Swap the verification key (tests mint licenses with their OWN
    throwaway keypair — the vendor private key never touches a test)."""
    global _PUBLIC_KEY_PEM
    _PUBLIC_KEY_PEM = pem


def set_license_state(state: dict[str, Any] | None) -> None:
    """Install a state dict directly (app-level tests: expired/trial/
    licensed serving behavior without touching files)."""
    global _STATE, _CACHE
    _STATE = state
    _CACHE = None


def reset_licensing() -> None:
    """Restore the embedded key + unmanaged state (test teardown)."""
    global _PUBLIC_KEY_PEM, _STATE, _CACHE
    _PUBLIC_KEY_PEM = _EMBEDDED_PUBLIC_KEY_PEM
    _STATE = None
    _CACHE = None

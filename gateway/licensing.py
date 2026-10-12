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
when their NAME is in the claim (provider_allowed). caps {keys,
instances, teams, gpus} — scale limits (absent = unlimited; trial =
unlimited). serial — unique issuance id (GL-…; vendor ledger).
exp? (absent = perpetual; the tool issues 30d/60d/90d/1y/2y/3y
presets), nbf, iat, node? (node-lock fingerprint). The JWT HEADER
carries kid — the signing key's id (sha16 of its public DER); the
gateway trusts a LIST of keys and selects by kid (rotation without
bricking deployed binaries).

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
CLOUD_PROVIDER_TYPES = frozenset({"openai", "runpod", "runpod-public",
                                  "anthropic", "gemini", "responses",
                                  "azure", "vertex", "bedrock"})

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
# Key rotation (v2.2): ALL trusted verification keys, newest first.
# A license's ``kid`` claim (sha16 of the signing public key) selects
# its key; kid-less tokens (v1 and early v2) try keys in order. A
# rotation = keygen a new pair, embed BOTH publics, re-issue with kid.
_TRUSTED_KEYS: list[str] = [_EMBEDDED_PUBLIC_KEY_PEM]
_STATE: dict[str, Any] | None = None    # None = unmanaged (source/tests)
_CACHE: tuple[float, tuple[bool, str]] | None = None
_LICENSE_MTIME: float | None = None     # hot-reload watch (file mtime)

# Signed revocation list (#6): data/revocations.json = {"crl": <jwt>}
# with claims {crl: true, revoked: [serials], version, iat} signed by
# the vendor (kid, same trusted keys). Offline honesty: the box
# holder can delete the file — same accepted risk as the trial
# anchor; revocations ALSO ship embedded in later binary builds.
_CRL: dict[str, Any] | None = None      # {"revoked": set, "version": n}
_CRL_MTIME: float | None = None


def _crl_path() -> Path:
    return PROJECT_ROOT / "data" / "revocations.json"


def _crl_is_revoked(serial: str) -> bool:
    """Active CRL check (inert when no CRL is loaded)."""
    return bool(_CRL and serial in _CRL.get("revoked", set()))


def load_revocations(path: Path | None = None) -> bool:
    """Load + verify the CRL file. A missing file = no revocations.
    A BROKEN/tampered file is ignored with a warning (fail-open for
    the FILE — a corrupt CRL must not brick serving; authenticity is
    enforced for whatever it does contain). True when a valid CRL is
    active. Boot + maybe_reload_crl both come through here."""
    global _CRL, _CRL_MTIME
    p = path or _crl_path()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        token = raw.get("crl") if isinstance(raw, dict) else None
        if not token:
            return False
        claims, err = _decode_jwt(str(token))
        if claims is None:
            logger.warning("Revocation list ignored (%s) — unsigned or "
                           "tampered", err)
            return False
        if not claims.get("crl"):
            logger.warning("Revocation list ignored — token is not a CRL")
            return False
        revoked = {str(s) for s in (claims.get("revoked") or [])
                   if str(s).strip()}
        _CRL = {"revoked": revoked,
                "version": int(claims.get("version") or 1)}
        _CRL_MTIME = p.stat().st_mtime
        if revoked:
            logger.info("Revocation list active (v%s): %d serial(s)",
                        _CRL["version"], len(revoked))
        # a just-loaded CRL may cover the ALREADY-ACTIVE license —
        # flip immediately so the next request_allowed sees it
        state = _STATE
        if (state and state.get("mode") in ("licensed", "grace")
                and _crl_is_revoked(
                    (state.get("license") or {}).get("serial") or "")):
            logger.error("License: REVOKED by CRL (serial %s) — serving "
                         "disabled",
                         (state.get("license") or {}).get("serial"))
            state["mode"] = "revoked"
            global _CACHE
            _CACHE = None
        return True
    except OSError:
        return False                     # no file — nothing revoked
    except Exception as e:               # noqa: BLE001 — never brick on CRL
        logger.warning("Revocation list ignored (%s: %s)",
                       e.__class__.__name__, e)
        return False


def maybe_reload_crl() -> bool:
    """mtime watch — a dropped/updated revocations.json takes effect
    live (newly-revoked serials flip the active state to revoked).
    A broken file is re-read only when it changes again (the mtime is
    recorded on failure too — no per-request re-parse of garbage).
    True when a valid CRL (re)loaded."""
    global _CRL_MTIME
    p = _crl_path()
    try:
        mtime = p.stat().st_mtime
    except OSError:
        return False
    if mtime == _CRL_MTIME and _CRL is not None:
        return False
    loaded = load_revocations(p)
    if not loaded and _CRL_MTIME != mtime:
        try:
            _CRL_MTIME = mtime         # don't retry until it changes
        except OSError:
            pass
    return loaded


def key_id(pem: str) -> str:
    """A signing key's id: sha16 of the public key DER (the ``kid``
    claim; license_tool keygen prints it)."""
    from cryptography.hazmat.primitives import serialization
    pub = serialization.load_pem_public_key(pem.encode("ascii"))
    der = pub.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo)
    return hashlib.sha256(der).hexdigest()[:16]


def set_license_public_keys(pems: list[str]) -> None:
    """Replace the trusted-key LIST (rotation: old + new side by
    side; newest first for kid-less fallback)."""
    global _PUBLIC_KEY_PEM, _TRUSTED_KEYS
    _TRUSTED_KEYS = [p for p in pems if p and p.strip()]
    _PUBLIC_KEY_PEM = _TRUSTED_KEYS[0] if _TRUSTED_KEYS else ""


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


def _resolved_license_path(explicit: Path | None = None) -> Path | None:
    """The FILE the active license came from (hot-reload watch), by
    the same precedence as _load_license_token: explicit → env file →
    default. None for env-inline / no license."""
    if explicit is not None:
        return explicit
    env = (os.environ.get("GATELODGE_LICENSE") or "").strip()
    if env:
        p = Path(env)
        return p if p.is_file() else None
    default = _default_license_path()
    return default if default.is_file() else None


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


def _decode_jwt(token: str) -> tuple[dict | None, str]:
    """kid-aware RS256 decode against the TRUSTED keys — shared by the
    license and CRL verification paths. Key selection: the ``kid``
    header (sha16 of the signing public key) picks the exact key;
    kid-less tokens try keys newest-first. PyJWT enforces exp/nbf; alg
    confusion is impossible (algorithms is pinned). Returns (claims,
    '') or (None, reason)."""
    kid: str | None = None
    try:
        import base64 as _b64
        h = token.split(".")[0]
        kid = (json.loads(_b64.urlsafe_b64decode(h + "=="))
               .get("kid") or None)
    except Exception:                                      # noqa: BLE001
        kid = None
    candidates: list[str]
    if kid:
        candidates = [k for k in _TRUSTED_KEYS if key_id(k) == kid]
        if not candidates:
            return None, f"unknown signing key (kid {kid!r})"
    else:
        candidates = list(_TRUSTED_KEYS)
    claims = None
    last_err = "invalid token (InvalidTokenError)"
    for pem in candidates:
        try:
            claims = jwt.decode(token, pem, algorithms=["RS256"])
            break
        except jwt.ExpiredSignatureError:
            return None, "token expired"
        except jwt.InvalidTokenError as e:
            last_err = f"invalid token ({e.__class__.__name__})"
            claims = None
    if claims is None:
        return None, last_err
    return claims, ""


def _verify_token(token: str, now: datetime) -> tuple[dict | None, str]:
    """License verification: plaintext claims, the SHA-256 hash of
    the contents signed with the vendor private key, verified here
    against an embedded public key (via _decode_jwt) + claim sanity.
    Returns (claims, '') or (None, reason)."""
    claims, err = _decode_jwt(token)
    if claims is None:
        return None, err
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
        # #6 revocation: checked BEFORE installing the licensed state
        # — a revoked serial never serves (an enterprise binary's boot
        # gate then refuses to start; community flips to revoked mode)
        load_revocations()
        if _crl_is_revoked(str(claims.get("serial") or "")):
            _STATE = {"edition": edition, "mode": "revoked",
                      "license": claims, "source": source,
                      "detail": f"serial {claims.get('serial')} is "
                      "revoked (CRL)", "trial": None}
            logger.error("License: REVOKED (serial %s, CRL v%s) — this "
                         "deployment may not serve",
                         claims.get("serial"),
                         (_CRL or {}).get("version", "?"))
            _CACHE = None
            return status()
        exp = claims.get("exp")
        _STATE = {"edition": edition, "mode": "licensed", "license": claims,
                  "source": source, "detail": "", "trial": None}
        logger.info(
            "License: %s-edition license active (serial=%s customer=%r "
            "vendor=%r support=%s expires=%s)",
            claims.get("edition"), claims.get("serial") or "-",
            claims.get("business_name") or claims.get("customer"),
            claims.get("vendor") or "-",
            bool(claims.get("support")),
            "perpetual" if exp is None
            else datetime.fromtimestamp(exp, tz=timezone.utc).isoformat())
        # hot-reload watch: remember the FILE behind this license (env
        # inline tokens have none — no hot reload for those)
        lic_path = (license_path if license_path is not None
                    else _resolved_license_path())
        if lic_path is not None:
            _STATE["path"] = str(lic_path)
            try:
                global _LICENSE_MTIME
                _LICENSE_MTIME = lic_path.stat().st_mtime
            except OSError:
                pass
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


def _grace_hours(claims: dict[str, Any]) -> int:
    """Post-expiry grace window in hours (#7): the license claim
    ``grace_hours`` overrides; absent = the 72h default; 0/negative
    disables (immediate stop at exp). Perpetual licenses never see
    this path."""
    raw = claims.get("grace_hours")
    if raw is None:
        return 72
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 72


def _recheck(state: dict[str, Any], now: datetime) -> tuple[bool, str]:
    """Runtime (re)evaluation — license expiry mid-run (with the
    post-expiry GRACE window: serving continues, everything warns),
    revocation (CRL), trial window and clock-rollback while serving.
    Well-defined failures only; unexpected errors fail OPEN (a
    licensing bug must not brick a paying deployment) and are
    logged."""
    try:
        mode = state.get("mode")
        if mode == "licensed":
            lic = state.get("license") or {}
            serial = lic.get("serial")
            if serial and _crl_is_revoked(serial):
                logger.error("License: REVOKED (serial %s, CRL v%s) — "
                             "serving disabled", serial,
                             (_CRL or {}).get("version", "?"))
                state["mode"] = "revoked"
                return False, "revoked"
            exp = lic.get("exp")
            if exp is not None and now.timestamp() >= float(exp):
                gh = _grace_hours(lic)
                grace_end = float(exp) + gh * 3600.0
                if gh > 0 and now.timestamp() < grace_end:
                    state["grace"] = {
                        "grace_hours": gh,
                        "hours_left": round(
                            (grace_end - now.timestamp()) / 3600.0, 1)}
                    return True, "grace"
                state.pop("grace", None)
                logger.warning("License: EXPIRED while running — serving "
                               "disabled (403 license_expired)")
                return False, "expired"
            state.pop("grace", None)
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


# ── scale caps (v2.2 — tiered deployments; absent claim = unlimited) ──────

CAP_KINDS = ("keys", "instances", "teams", "gpus")


def caps() -> dict[str, int]:
    """ACTIVE scale caps from the license ``caps`` claim, normalized:
    {"keys": 50, "instances": 4, ...}. Unmanaged/trial → {} (the full
    product); invalid values drop (that cap = unlimited)."""
    state = _STATE
    if not state or state.get("mode") != "licensed":
        return {}
    raw = (state.get("license") or {}).get("caps")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for kind in CAP_KINDS:
        try:
            v = int(raw.get(kind))
        except (TypeError, ValueError):
            continue
        if v > 0:
            out[kind] = v
    return out


def cap_allows(kind: str, current: int) -> tuple[bool, int | None]:
    """(allowed, limit) for creating ONE more of ``kind`` given the
    CURRENT count. No cap → (True, None). Trial/unmanaged → always
    allowed (the full product)."""
    limit = caps().get(kind)
    if limit is None:
        return True, None
    return (int(current) < int(limit), limit)


# ── expiry early-warning (30/7/1 days) ────────────────────────────────────

def expiry_warning() -> dict[str, Any] | None:
    """The current license-expiry warning level, or None. {"level":
    30|7|1, "days_left": float} pre-expiry; {"level": 0, "grace":
    True, "hours_left": float} INSIDE the post-expiry grace window
    (serving continues — level 0 rides the same on-change event/log
    path). Surfaced by the app on level CHANGE (event ring + log);
    the console banner rides /admin/license."""
    state = _STATE
    if not state or state.get("mode") not in ("licensed", "grace"):
        return None
    lic = state.get("license") or {}
    exp = lic.get("exp")
    if exp is None:
        return None                     # perpetual
    try:
        days = (float(exp) - time.time()) / 86400.0
    except (TypeError, ValueError):
        return None
    if days < 0:
        # expired — inside the grace window? (level 0)
        gh = _grace_hours(lic)
        grace_end = float(exp) + gh * 3600.0
        if gh > 0 and time.time() < grace_end:
            return {"level": 0, "grace": True,
                    "hours_left": round(
                        (grace_end - time.time()) / 3600.0, 1)}
        return None                     # past grace — the gate handles it
    for level in (1, 7, 30):
        if days <= level:
            return {"level": level, "days_left": round(days, 1)}
    return None


# ── license hot-reload (drop-in renewal, no restart) ──────────────────────

def maybe_reload_license() -> bool:
    """Pick up a RENEWED license file in place (mtime watch). Swaps
    the state ONLY when the new token verifies AND satisfies the
    binary's edition requirement (an enterprise binary stays
    enterprise-gated); a broken/partial file keeps the OLD state and
    warns — a botched renewal must never make serving worse. Env-
    inline licenses have no file: no-op. True when the state changed."""
    global _STATE, _CACHE, _LICENSE_MTIME
    state = _STATE
    if not state or state.get("mode") != "licensed":
        return False
    path_raw = state.get("path")
    if not path_raw:
        return False                    # env-inline (no file)
    p = Path(path_raw)
    try:
        mtime = p.stat().st_mtime
    except OSError:
        return False
    if mtime == _LICENSE_MTIME:
        return False
    old_state = dict(state)
    edition = state.get("edition") or "community"
    try:
        new = bootstrap(edition=edition, license_path=p)
    except Exception as e:                               # noqa: BLE001
        logger.warning("License hot-reload failed (%s) — keeping the "
                       "current license", e)
        _STATE = old_state
        _CACHE = None
        _LICENSE_MTIME = mtime           # don't re-try until it changes
        return False
    ok = (new.get("mode") == "licensed"
          and (edition != "enterprise"
               or new.get("license_edition") == "enterprise"))
    if not ok:
        logger.warning("License hot-reload rejected (mode=%s "
                       "license_edition=%s detail=%s) — keeping the "
                       "current license", new.get("mode"),
                       new.get("license_edition"), new.get("detail"))
        _STATE = old_state
        _CACHE = None
        _LICENSE_MTIME = mtime           # don't re-try until it changes
        return False
    logger.info("License hot-reloaded (serial=%s expires=%s)",
                (new.get("serial") or "-"),
                new.get("expires_at") or "perpetual")
    return True


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
    warn = expiry_warning()
    grace = state.get("grace")          # set by _recheck inside the window
    return {
        "mode": state.get("mode"),
        "edition": state.get("edition"),
        "license_edition": lic.get("edition"),
        "serial": lic.get("serial"),
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
        "caps": caps(),
        "cap_kinds": list(CAP_KINDS),
        "grace_hours": _grace_hours(lic) if exp is not None else None,
        "grace": grace,
        "expires_at": datetime.fromtimestamp(exp, tz=timezone.utc).isoformat()
        if exp is not None else None,
        "perpetual": bool(lic) and exp is None,
        "days_left": (warn or {}).get("days_left")
        if exp is not None else None,
        "expiry_warning": (warn or {}).get("level") if warn else None,
        "node_locked": bool(lic.get("node")),
        "trial_days_left": trial.get("days_left"),
        "revoked_serials": len((_CRL or {}).get("revoked", set())),
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
    grace = state.get("grace")         # post-expiry soft window (#7)
    if grace:
        out["grace_hours_left"] = grace.get("hours_left")
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
    throwaway keypair — the vendor private key never touches a test).
    Replaces the whole trusted list with this one key."""
    global _PUBLIC_KEY_PEM, _TRUSTED_KEYS
    _PUBLIC_KEY_PEM = pem
    _TRUSTED_KEYS = [pem]


def set_license_state(state: dict[str, Any] | None) -> None:
    """Install a state dict directly (app-level tests: expired/trial/
    licensed serving behavior without touching files)."""
    global _STATE, _CACHE
    _STATE = state
    _CACHE = None


def reset_licensing() -> None:
    """Restore the embedded key + unmanaged state (test teardown)."""
    global _PUBLIC_KEY_PEM, _TRUSTED_KEYS, _STATE, _CACHE, _LICENSE_MTIME
    global _CRL, _CRL_MTIME
    _PUBLIC_KEY_PEM = _EMBEDDED_PUBLIC_KEY_PEM
    _TRUSTED_KEYS = [_EMBEDDED_PUBLIC_KEY_PEM]
    _STATE = None
    _CACHE = None
    _LICENSE_MTIME = None
    _CRL = None
    _CRL_MTIME = None

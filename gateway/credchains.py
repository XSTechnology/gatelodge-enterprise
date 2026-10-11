"""Cloud credential chains (v1.4): backends with ``auth: sigv4`` or
``auth: adc`` authenticate with the PLATFORM credential chain instead
of a static api key — no provider secrets at rest (the SOC2 story).

    "backends": {
      "bedrock": {"type": "openai",
                  "auth": "sigv4", "region": "us-east-1",
                  "base_url": "https://bedrock-runtime.us-east-1.amazonaws.com/openai/v1"},
      "vertex":  {"type": "openai",
                  "auth": "adc", "region": "us-central1",
                  "base_url": "https://aiplatform.googleapis.com/v1/projects/P/locations/us-central1/endpoints/openapi"}
    }

- ``auth: sigv4`` — AWS Signature V4 per request, credentials from the
  STANDARD env-var chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
  AWS_SESSION_TOKEN, region from ``region`` or AWS_REGION). The
  httpx.Auth signer signs every request; no botocore dependency.
- ``auth: adc`` — Google Application Default Credentials (lazy
  google-auth import): the access token is fetched and cached until
  ~expiry, refreshed per request when stale. Needs the google-auth
  package (uv add google-auth) — absent → clear error at request time.

Both ride httpx.Auth on the AsyncOpenAI client's http_client, so the
per-request refresh is invisible to the OpenAI SDK.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import hmac
import logging
import os
import time
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)


class CloudCredentialError(Exception):
    """The platform credential chain is unavailable/misconfigured."""


# ── AWS SigV4 (env-var chain, no botocore) ─────────────────────────────────

_ALGO = "AWS4-HMAC-SHA256"


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _sigv4_sign(access_key: str, secret_key: str, session_token: str,
                region: str, service: str, method: str, url: str,
                body: bytes, amz_date: str) -> tuple[str, dict[str, str]]:
    """Sign one request → (authorization_header, signed_headers_dict).
    Follows the SigV4 canonical-request flow (singlechunk, unsigned
    payload hashed as x-amz-content-sha256 — the Bedrock-compatible
    form). Pure function: deterministic for fixed inputs (test-pinned)."""
    p = urlparse(url)
    host = p.hostname or ""
    canonical_uri = p.path or "/"
    # canonical query: sorted k=v pairs, URL-encoded
    from urllib.parse import parse_qsl, urlencode
    q = urlencode(sorted(parse_qsl(p.query)))
    payload_hash = hashlib.sha256(body).hexdigest()
    headers = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if session_token:
        headers["x-amz-security-token"] = session_token
    signed_names = ";".join(sorted(headers))
    canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))
    canonical = (f"{method}\n{canonical_uri}\n{q}\n{canonical_headers}\n"
                 f"{signed_names}\n{payload_hash}")
    scope = f"{amz_date[:8]}/{region}/{service}/aws4_request"
    sts = (f"{_ALGO} Credential={access_key}/{scope}, "
           f"SignedHeaders={signed_names}, ")
    to_sign = (f"{_ALGO}\n{amz_date}\n{scope}\n"
               + hashlib.sha256(canonical.encode()).hexdigest())
    date_key = _hmac_sha256(b"AWS4" + secret_key.encode(), amz_date[:8])
    region_key = _hmac_sha256(date_key, region)
    service_key = _hmac_sha256(region_key, service)
    signing_key = _hmac_sha256(service_key, "aws4_request")
    sig = hmac.new(signing_key, to_sign.encode(), hashlib.sha256).hexdigest()
    return sts + f"Signature={sig}", headers


class SigV4Auth(httpx.Auth):
    """Per-request AWS SigV4 signer over the env-var credential chain."""

    def __init__(self, region: str = "", service: str = "bedrock"):
        self.region = region or os.environ.get("AWS_REGION", "us-east-1")
        self.service = service

    def _creds(self) -> tuple[str, str, str]:
        ak = os.environ.get("AWS_ACCESS_KEY_ID", "")
        sk = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        st = os.environ.get("AWS_SESSION_TOKEN", "")
        if not ak or not sk:
            raise CloudCredentialError(
                "auth: sigv4 requires AWS_ACCESS_KEY_ID and "
                "AWS_SECRET_ACCESS_KEY in the environment (the env-var "
                "chain; instance profiles need a sidecar token fetcher)")
        return ak, sk, st

    def auth_flow(self, request: httpx.Request):
        body = request.content or b""
        ak, sk, st = self._creds()
        amz_date = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        authz, headers = _sigv4_sign(ak, sk, st, self.region, self.service,
                                     request.method, str(request.url), body,
                                     amz_date)
        request.headers["Authorization"] = authz
        for k, v in headers.items():
            if k != "host":
                request.headers[k] = v
        yield request


# ── Google ADC (lazy google-auth, cached access token) ─────────────────────

def _adc_fetch_token() -> tuple[str, float]:
    """Application Default Credentials → (access_token, expires_at_epoch).
    Raises CloudCredentialError when google-auth is missing or no
    credentials are found."""
    try:
        import google.auth
        import google.auth.transport.requests as gtr
    except ImportError as e:
        raise CloudCredentialError(
            "auth: adc requires the google-auth package (uv add "
            "google-auth) — not installed") from e
    try:
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        if not creds.valid:
            creds.refresh(gtr.Request())
        token = creds.token or ""
        expiry = getattr(creds, "expiry", None)
        exp_epoch = (expiry.replace(tzinfo=None).timestamp()
                     if isinstance(expiry, _dt.datetime) else time.time() + 3600)
        if not token:
            raise CloudCredentialError("adc: no token after refresh")
        return token, exp_epoch
    except CloudCredentialError:
        raise
    except Exception as e:  # noqa: BLE001 — any chain failure
        raise CloudCredentialError(f"adc: {e}") from e


class ADCAuth(httpx.Auth):
    """Per-request Bearer token from ADC, cached until ~expiry (refresh
    60s early); the OpenAI SDK never sees the refresh."""

    _refresh_skew_s = 60.0

    def __init__(self, fetcher=None):
        self._fetcher = fetcher or _adc_fetch_token
        self._token: str = ""
        self._expires: float = 0.0

    def _current(self) -> str:
        if not self._token or time.time() >= self._expires - self._refresh_skew_s:
            self._token, self._expires = self._fetcher()
            logger.debug("adc token refreshed (expires %s)",
                         time.strftime("%H:%M:%S", time.localtime(self._expires)))
        return self._token

    def auth_flow(self, request: httpx.Request):
        request.headers["Authorization"] = f"Bearer {self._current()}"
        yield request

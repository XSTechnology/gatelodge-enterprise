"""Content Vault — key material, wrapping, and the keyfile.

Two-key design (specs/content-vault-spec.md):

  Key D — data keys: the CUSTOMER'S public keys (RSA, OAEP-SHA256).
  Every record's fresh AES-256-GCM DEK is multi-wrapped to EVERY key
  in the active wrap set (primary + escrow) — losing any single key
  never loses data. The gateway holds only publics: it can WRITE the
  vault but never READ it back.

  Key S — search key (32 random bytes): keys the blind indexes
  (HMAC-SHA256 over the consuming gateway key id / model / team), so
  records are searchable and extractable by user WITHOUT decrypting
  content. Escrowed in the keyfile, multi-wrapped to the same wrap
  set.

Keyfile (vault.keys, beside the part files): the registry mirror plus
Key S under multi-wrap — any private-key holder can recover it.

Algorithms (v1): RSA-OAEP(SHA-256, 3072+ recommended) for wraps,
AES-256-GCM for content, HMAC-SHA256 for indexes. X25519/HPKE is a
future wrap algorithm — the record format keys on alg strings.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_B64 = lambda b: base64.b64encode(b).decode("ascii")          # noqa: E731
_UNB64 = base64.b64decode


# ── registry ───────────────────────────────────────────────────────────────

def normalize_registry(entries: list[dict]) -> list[dict]:
    """Validate + sort the wrap set: [{v, role, alg, pub}] with v
    unique, role primary|escrow, exactly one primary. Raises ValueError
    with a clear message on any problem (config errors fail the boot)."""
    if not entries:
        raise ValueError("vault.keys: the wrap set is empty — at least "
                         "one primary key is required")
    out, seen, primaries = [], set(), 0
    for e in entries:
        v = int(e.get("v") or 0)
        role = str(e.get("role") or "").strip()
        pub = str(e.get("pub") or "").strip()
        if v <= 0 or v in seen:
            raise ValueError(f"vault.keys: bad or duplicate version {v!r}")
        if role not in ("primary", "escrow"):
            raise ValueError(f"vault.keys v{v}: role must be primary|escrow")
        if role == "primary":
            primaries += 1
        if "BEGIN PUBLIC KEY" not in pub and "BEGIN RSA PUBLIC KEY" not in pub:
            raise ValueError(f"vault.keys v{v}: pub must be a PEM public key")
        load_public_key(pub)          # parse check — raises on garbage
        seen.add(v)
        out.append({"v": v, "role": role, "alg": "RSA-OAEP",
                    "pub": pub})
    if primaries != 1:
        raise ValueError("vault.keys: exactly one primary key is required")
    return sorted(out, key=lambda e: e["v"])


def load_public_key(pem: str) -> rsa.RSAPublicKey:
    key = serialization.load_pem_public_key(pem.encode("ascii"))
    if not isinstance(key, rsa.RSAPublicKey):
        raise ValueError("vault keys must be RSA public keys (v1)")
    if key.key_size < 2048:
        raise ValueError(f"vault RSA key too small ({key.key_size} bits; "
                         "2048 minimum, 3072 recommended)")
    return key


def load_private_key(pem: str) -> rsa.RSAPrivateKey:
    key = serialization.load_pem_private_key(pem.encode("ascii"),
                                             password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ValueError("expected an RSA private key")
    return key


# ── DEK multi-wrap ─────────────────────────────────────────────────────────

def wrap_dek(dek: bytes, registry: list[dict]) -> list[dict]:
    """Wrap the DEK to EVERY key in the wrap set (escrow by
    construction). One list entry per key version."""
    wraps = []
    for e in registry:
        pub = load_public_key(e["pub"])
        w = pub.encrypt(dek, padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(), label=None))
        wraps.append({"v": e["v"], "alg": "RSA-OAEP", "wrap": _B64(w)})
    return wraps


def unwrap_dek(wraps: list[dict], private_pem: str) -> bytes:
    """Open a record with ANY private key from its wrap set. Raises
    KeyError when this key's version is not in the set, ValueError on
    ciphertext damage."""
    priv = load_private_key(private_pem)
    for w in wraps:
        try:
            return priv.decrypt(_UNB64(w["wrap"]), padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(), label=None))
        except Exception:      # noqa: BLE001 — wrong version, keep trying
            continue
    raise KeyError("this private key opens none of the record's wraps")


# ── content seal ───────────────────────────────────────────────────────────

def seal(dek: bytes, plaintext: bytes) -> tuple[str, str]:
    """AES-256-GCM. Returns (nonce_b64, ct_b64) — the tag rides the
    ciphertext tail (AESGCM convention)."""
    nonce = os.urandom(12)
    return _B64(nonce), _B64(AESGCM(dek).encrypt(nonce, plaintext, None))


def open_(dek: bytes, nonce_b64: str, ct_b64: str) -> bytes:
    return AESGCM(dek).decrypt(_UNB64(nonce_b64), _UNB64(ct_b64), None)


# ── Key S (blind index) ────────────────────────────────────────────────────

def new_search_key() -> bytes:
    return os.urandom(32)


def blind_index(search_key: bytes, value: Any) -> str:
    """Deterministic keyed digest — equality search over ciphertext.
    None and empty string canonicalize to '-' (open-mode requests carry
    an empty key id; they index and search as anonymous)."""
    v = str(value) if value not in (None, "") else "-"
    return hmac.new(search_key, v.encode(), hashlib.sha256).hexdigest()


# ── the keyfile ────────────────────────────────────────────────────────────

def write_keyfile(path, vault_id: str, registry: list[dict],
                  search_key: bytes) -> None:
    """Key S under multi-wrap to the same wrap set: any private-key
    holder can recover the search key from this file. Write-once —
    regenerating requires re-indexing (documented in the spec)."""
    kf = {"v": 1, "vault_id": vault_id, "created": _now_iso(),
          "keys": [{k: e[k] for k in ("v", "role", "alg", "pub")}
                   for e in registry],
          "search_key": wrap_dek(search_key, registry)}
    path.write_text(json.dumps(kf, indent=2), encoding="utf-8")


def read_keyfile(path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def recover_search_key(keyfile: dict, private_pem: str) -> bytes:
    """Recover Key S from the keyfile with any wrap-set private key —
    the escrow path for the search side."""
    return unwrap_dek(keyfile["search_key"], private_pem)


def _now_iso() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

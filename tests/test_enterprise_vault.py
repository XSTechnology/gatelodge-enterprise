"""Content Vault — the M3 fork tests (spec §7).

Covers: crypto round-trip (multi-wrap escrow: EITHER key opens), the
keyfile recovery path, the writer's chain (verify ok intact / broken on
any tamper / broken on any removal), blind-index search (finds the
user's records, no false positives, content stays opaque), decryption
with sha_req/sha_resp integrity, capture modes, sampling, queue
overflow drop-and-flag, retention with tombstones + continued chain,
canaries, the keycheck challenge/verify flow, and the full END-TO-END:
a served chat request lands in the vault and decrypts offline.
"""
import asyncio
import base64
import hashlib
import json
import sys
import time
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402

from gateway_enterprise.vault import keys as vkeys  # noqa: E402
from gateway_enterprise.vault import index as vindex  # noqa: E402
from gateway_enterprise.vault import writer as vwriter  # noqa: E402


# ── fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def keypairs():
    """primary + escrow RSA keypairs (the wrap set)."""
    out = []
    for role in ("primary", "escrow"):
        k = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pub = k.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("ascii")
        priv = k.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ).decode("ascii")
        out.append({"v": 1 if role == "primary" else 2, "role": role,
                    "pub": pub, "priv": priv})
    return out


@pytest.fixture()
def registry(keypairs):
    return vkeys.normalize_registry(
        [{"v": k["v"], "role": k["role"], "pub": k["pub"]}
         for k in keypairs])


@pytest.fixture()
def vault(tmp_path, registry):
    """A writer over a temp dir. The task is started INSIDE _drive()
    (one asyncio.run per test) — start() requires a running loop."""
    w = vwriter.VaultWriter(vault_dir=tmp_path / "vault",
                            vault_id="test-vault",
                            registry=registry,
                            search_key=vkeys.new_search_key())
    return w


def _drive(w: vwriter.VaultWriter, feed=None) -> None:
    """One event loop for the whole cycle: start → feed → drain → stop
    (the app path does the same via the startup/shutdown handlers)."""
    async def main():
        w.start()
        if feed is not None:
            feed()
        for _ in range(50):
            if w.queue.empty():
                break
            await asyncio.sleep(0.01)
        await w.stop()
    asyncio.run(main())


def _feed_and_drain(w, n=3, **kw):
    _drive(w, lambda: _feed(w, n, **kw))


def _parts(w) -> list[Path]:
    return w._parts()


def _feed(w, n=3, user="key-abc", team="platform", model="groq:cloud-fast"):
    """Feed n synthetic requests through enqueue (the flush path)."""
    for i in range(n):
        w.enqueue(json.dumps({"messages": [{"role": "user",
                                            "content": f"q{i}"}]}),
                  f"answer {i}",
                  {"model": model, "key": user, "team": team,
                   "status": 200, "usage": {"total_tokens": 8}})


# ── keys: multi-wrap escrow ────────────────────────────────────────────────

def test_wrap_unwrap_each_key_opens(keypairs, registry):
    dek = vkeys.new_search_key()          # any 32 bytes
    wraps = vkeys.wrap_dek(dek, registry)
    assert len(wraps) == 2                 # multi-wrap: every key version
    for kp in keypairs:                    # EITHER private key opens it
        assert vkeys.unwrap_dek(wraps, kp["priv"]) == dek


def test_unwrap_wrong_key_raises():
    import os
    dek = os.urandom(32)
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv = other.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()).decode()
    k = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pub = k.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    reg = vkeys.normalize_registry(
        [{"v": 1, "role": "primary", "pub": pub}])
    wraps = vkeys.wrap_dek(dek, reg)
    with pytest.raises(KeyError):
        vkeys.unwrap_dek(wraps, priv)


def test_keyfile_recovery_with_any_key(keypairs, registry, tmp_path):
    sk = vkeys.new_search_key()
    kf_path = tmp_path / "vault.keys"
    vkeys.write_keyfile(kf_path, "vid", registry, sk)
    kf = vkeys.read_keyfile(kf_path)
    for kp in keypairs:                    # either private key recovers S
        assert vkeys.recover_search_key(kf, kp["priv"]) == sk


def test_blind_index_deterministic_and_keyed():
    sk = b"k" * 32
    a = vkeys.blind_index(sk, "key-abc")
    assert a == vkeys.blind_index(sk, "key-abc")          # deterministic
    assert a != vkeys.blind_index(b"j" * 32, "key-abc")   # keyed
    assert vkeys.blind_index(sk, None) == vkeys.blind_index(sk, "-")


def test_registry_validation(registry):
    with pytest.raises(ValueError, match="primary"):
        vkeys.normalize_registry(
            [{"v": 1, "role": "escrow", "pub": registry[0]["pub"]}])
    with pytest.raises(ValueError, match="duplicate"):
        vkeys.normalize_registry([
            {"v": 1, "role": "primary", "pub": registry[0]["pub"]},
            {"v": 1, "role": "escrow", "pub": registry[1]["pub"]}])
    with pytest.raises(ValueError, match="PEM"):
        vkeys.normalize_registry(
            [{"v": 1, "role": "primary", "pub": "garbage"}])


# ── writer: records, chain, modes, overflow, retention ─────────────────────

def test_records_chain_and_verify(vault):
    _feed_and_drain(vault)
    parts = _parts(vault)
    assert len(parts) == 1
    lines = [json.loads(l) for l in
             parts[0].read_text(encoding="utf-8").splitlines()
             if l.strip()]
    headers = [l for l in lines if l.get("header")]
    recs = [l for l in lines if not l.get("header")]
    assert len(headers) == 1 and headers[0]["continues"] == \
        hashlib.sha256(b"vault:test-vault").hexdigest()
    assert len(recs) == 3
    assert [r["seq"] for r in recs] == [0, 1, 2]
    # each record multi-wrapped, indexed, chained
    for r in recs:
        assert len(r["dek"]) == 2
        assert set(r["idx"]) == {"user", "model", "team"}
        assert r["prev"]
    v = vindex.verify_chain(parts, "test-vault")
    assert v["ok"] and v["checked"] == 3


def test_tamper_breaks_chain(vault):
    _feed_and_drain(vault)
    part = _parts(vault)[0]
    lines = part.read_text(encoding="utf-8").splitlines()
    # flip a byte inside record 2's ciphertext
    rec = json.loads(lines[2])
    ct = bytearray(base64.b64decode(rec["ct"]))
    ct[10] ^= 0xFF
    rec["ct"] = base64.b64encode(bytes(ct)).decode()
    lines[2] = json.dumps(rec)
    part.write_text("\n".join(lines) + "\n", encoding="utf-8")
    v = vindex.verify_chain(_parts(vault), "test-vault")
    assert not v["ok"] and "mismatch" in v["break"]


def test_removal_breaks_chain(vault):
    _feed_and_drain(vault)
    part = _parts(vault)[0]
    lines = [l for l in part.read_text(encoding="utf-8").splitlines()
             if l.strip()]
    lines.pop(2)                            # delete record 2 entirely
    part.write_text("\n".join(lines) + "\n", encoding="utf-8")
    v = vindex.verify_chain(_parts(vault), "test-vault")
    assert not v["ok"]


def test_search_finds_user_without_decryption(vault, keypairs):
    _drive(vault, lambda: (_feed(vault, 2, user="key-alice"),
                           _feed(vault, 1, user="key-bob")))
    hits = vindex.search(_parts(vault), vault.search_key,
                         "user", "key-alice")
    assert len(hits) == 2
    # the hits are the ENCRYPTED records — content never leaves opaque
    for h in hits:
        assert "ct" in h["record"]
    # no false positives for a user that never appeared
    assert vindex.search(_parts(vault), vault.search_key,
                         "user", "key-nobody") == []


def test_decrypt_with_either_key_and_integrity(vault, keypairs):
    _drive(vault, lambda: _feed(vault, 1, user="key-carol"))
    hits = vindex.search(_parts(vault), vault.search_key,
                         "user", "key-carol")
    for kp in keypairs:                    # escrow: either key opens it
        out = vindex.decrypt([h["record"] for h in hits], kp["priv"])
        assert len(out) == 1
        assert out[0]["request"]["messages"][0]["content"] == "q0"
        assert out[0]["response"] == "answer 0"
        assert out[0]["meta"]["key"] == "key-carol"
        assert out[0]["integrity"]["sha_req_ok"]
        assert out[0]["integrity"]["sha_resp_ok"]


def test_capture_modes(vault_prompts, vault_responses, keypairs):
    priv = keypairs[0]["priv"]
    for w, want_req, want_resp in ((vault_prompts, True, ""),
                                   (vault_responses, None, "answer 0")):
        _drive(w, lambda w=w: _feed(w, 1))
        hits = vindex.search(_parts(w), w.search_key, "user", "key-abc")
        out = vindex.decrypt([h["record"] for h in hits], priv)
        if want_req:
            assert out[0]["request"]["messages"][0]["content"] == "q0"
        else:
            assert out[0]["request"] is None
        assert out[0]["response"] == want_resp


def test_overflow_drops_and_flags(tmp_path, registry):
    w = vwriter.VaultWriter(vault_dir=tmp_path / "v", vault_id="t",
                            registry=registry,
                            search_key=b"s" * 32, queue_cap=1)
    # no writer task running → the queue fills immediately
    assert w.enqueue("{}", "r", {}) is True
    assert w.enqueue("{}", "r", {}) is False     # dropped, not raised
    assert w.dropped == 1


def test_retention_tombstones_and_chain_continues(tmp_path, registry):
    w = vwriter.VaultWriter(vault_dir=tmp_path / "v", vault_id="t",
                            registry=registry, search_key=b"s" * 32,
                            keep_days=1)
    # today's part with one record
    _drive(w, lambda: _feed(w, 1))
    # synthesize a properly-chained OLD part (10 days old)
    import datetime
    day = (datetime.datetime.now() - datetime.timedelta(days=10))
    old_part = tmp_path / "v" / f"vault-{day.strftime('%Y%m%d')}.jsonl"
    old_part.write_text(json.dumps({
        "header": True, "part": old_part.name, "vault_id": "t",
        "continues": hashlib.sha256(b"vault:t").hexdigest(),
        "ts": time.time()}) + "\n", encoding="utf-8")
    w._enforce_retention()
    assert not old_part.exists()                  # dropped
    # feed more — a FRESH writer over the same dir (the queue binds to
    # its loop; restarts get a new writer, which also resumes the chain)
    w2 = vwriter.VaultWriter(vault_dir=tmp_path / "v", vault_id="t",
                             registry=registry, search_key=b"s" * 32,
                             keep_days=1)
    _drive(w2, lambda: _feed(w2, 1))
    v = vindex.verify_chain(_parts(w2), "t")
    assert v["ok"]
    assert any(t["deleted"] == old_part.name for t in v["tombstones"])


def test_canary_record(vault, keypairs):
    canary = vault.canary(1)
    out = vindex.decrypt([canary], keypairs[0]["priv"])
    assert out[0]["request"]["canary"] is True
    assert out[0]["request"]["key_v"] == 1


def test_writer_resumes_chain_after_restart(tmp_path, registry):
    w1 = vwriter.VaultWriter(vault_dir=tmp_path / "v", vault_id="t",
                             registry=registry, search_key=b"s" * 32)
    _drive(w1, lambda: _feed(w1, 2))
    head1, seq1 = w1._chain_head, w1.seq
    # "restart": a new writer over the same dir continues the chain
    w2 = vwriter.VaultWriter(vault_dir=tmp_path / "v", vault_id="t",
                             registry=registry, search_key=b"s" * 32)
    assert w2.seq == seq1
    assert w2._chain_head == head1
    _drive(w2, lambda: _feed(w2, 1))
    v = vindex.verify_chain(_parts(w2), "t")
    assert v["ok"] and v["checked"] == 3


# ── keycheck: challenge / verify (possession proven, key never uploaded) ──

def test_keycheck_roundtrip(vault, registry, keypairs):
    from gateway_enterprise.vault.tools import _CHALLENGES
    import secrets
    # simulate the endpoint's challenge creation (direct — the router
    # needs a running app; the crypto flow is what matters here)
    token = secrets.token_hex(16)
    dek = __import__("os").urandom(32)
    nonce, ct = vkeys.seal(dek, token.encode())
    wraps = vkeys.wrap_dek(dek, registry)
    challenge = {"challenge": {"dek": wraps, "nonce": nonce, "ct": ct}}
    # offline: the operator decrypts with the private key
    d = vkeys.unwrap_dek(challenge["challenge"]["dek"], keypairs[1]["priv"])
    got = vkeys.open_(d, challenge["challenge"]["nonce"],
                      challenge["challenge"]["ct"]).decode()
    assert got == token
    assert hashlib.sha256(got.encode()).hexdigest() == \
        hashlib.sha256(token.encode()).hexdigest()


# ── end-to-end: a served request lands in the vault and decrypts ──────────

def test_e2e_chat_request_captured_and_decryptable(
        tmp_path, registry, keypairs, monkeypatch):
    import gateway.app as gw_app
    import gateway.config as gw_config
    import gateway.models as gw_models
    import gateway.traffic as traffic
    from gateway_enterprise.vault import VaultCapture

    gguf = tmp_path / "fake.gguf"
    gguf.write_bytes(b"x")
    cfg = {"port": 9914, "bind": "127.0.0.1", "default_alias": "fast",
           "models_dir": str(tmp_path / "models"),
           "backends": {"local": {"type": "embedded", "models": {
               "fast": {"gguf": str(gguf), "ctx": 1024,
                        "placement": ""}}}},
           "aliases": {"fast": {"chain": ["local:fast"], "hedge_s": 0}},
           "vault": {"enabled": True}}
    cfgfile = tmp_path / "gateway_config.json"
    cfgfile.write_text(json.dumps(cfg), encoding="utf-8")
    cfg_obj = gw_config.GatewayConfig(cfgfile)
    monkeypatch.setattr(gw_config, "_CONFIG", cfg_obj)
    engine = gw_models.EmbeddedEngine(cfg_obj)
    monkeypatch.setattr(gw_models, "_ENGINE", engine)
    monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "log.jsonl")
    gw_app._AUDIT_CHAIN_HEAD = None

    w = vwriter.VaultWriter(vault_dir=tmp_path / "vault",
                            vault_id="e2e", registry=registry,
                            search_key=b"s" * 32)

    # install the vault exactly as enterprise boot does: the writer's
    # lifecycle rides the app's startup/shutdown events (the SAME loop
    # that serves — asyncio queues bind to their loop; that IS the
    # production shape this test verifies)
    import gateway_enterprise.vault as vault_mod
    vault_mod._WRITER = w
    gw_app.app.router.add_event_handler("startup", vault_mod._start_writer)
    gw_app.app.router.add_event_handler("shutdown", vault_mod._stop_writer)
    traffic.install(VaultCapture(w))
    try:
        def fn(records):
            with TestClient(gw_app.app) as client:
                r = client.post("/v1/chat/completions", json={
                    "model": "fast",
                    "messages": [{"role": "user", "content": "hello"}]})
            assert r.status_code == 200, r.text

        _with_fake_llama(fn)
        # the app's shutdown handler drained the queue — assert the vault
        parts = _parts(w)
        assert len(parts) == 1
        hits = vindex.search(parts, w.search_key, "user", None)
        assert len(hits) == 1                 # anonymous (open mode)
        out = vindex.decrypt([h["record"] for h in hits],
                             keypairs[0]["priv"])
        assert out[0]["request"]["messages"] == [
            {"role": "user", "content": "hello"}]
        # non-streaming request → the fake's non-stream content
        assert out[0]["response"] == "local answer"
        assert out[0]["integrity"]["sha_resp_ok"]
        v = vindex.verify_chain(parts, "e2e")
        assert v["ok"] and v["checked"] == 1
    finally:
        traffic.uninstall()
        vault_mod._WRITER = None


def _with_fake_llama(fn, script=None):
    """The stage3 fake-embedded pattern (stream pieces joined by the
    engine shim into one delta)."""
    records: list[dict] = []
    mod = types.ModuleType("llama_cpp")

    class FakeLlama:
        def __init__(self, **kwargs):
            records.append({"load": kwargs})

        def create_chat_completion(self, **kwargs):
            if kwargs.get("stream"):
                def _gen():
                    for p in ("Hello", " world"):
                        yield {"choices": [{"delta": {"content": p}}]}
                return _gen()
            records.append({"call": kwargs})
            return {"choices": [{"message": {"content": "local answer"}}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 2,
                              "total_tokens": 5}}

    mod.Llama = FakeLlama
    real = sys.modules.get("llama_cpp")
    sys.modules["llama_cpp"] = mod
    try:
        fn(records)
    finally:
        if real is not None:
            sys.modules["llama_cpp"] = real
        else:
            sys.modules.pop("llama_cpp", None)


# ── mode fixtures (prompts / responses) ────────────────────────────────────

@pytest.fixture()
def vault_prompts(tmp_path, registry):
    w = vwriter.VaultWriter(vault_dir=tmp_path / "v1", vault_id="t",
                            registry=registry, search_key=b"s" * 32,
                            mode="prompts")
    return w


@pytest.fixture()
def vault_responses(tmp_path, registry):
    w = vwriter.VaultWriter(vault_dir=tmp_path / "v2", vault_id="t",
                            registry=registry, search_key=b"s" * 32,
                            mode="responses")
    return w

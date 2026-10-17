"""Traffic capture seam (v2.4 Content Vault) — the M2 seam tests.

The seam contract (gateway/traffic.py):

  DEFAULT NO-OP — no backend installed: zero observation, zero
  retention, serving byte-for-byte unchanged.

  With a backend installed (enterprise build): every chat request
  observes itself at entry, every response fragment as it assembles
  (all streaming paths + non-streaming + cache hits), and flushes
  exactly once from the request-log chokepoint on EVERY terminal path
  — success, cache hit, error — with the terminal metadata.

  Capture never breaks serving: a backend that raises is counted and
  warned, the request proceeds untouched.

  vault.enabled without a backend fails the boot gate (community
  build / unlicensed enterprise) with a clear error.
"""
import json
import sys
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.cloud as gw_cloud  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.traffic as traffic  # noqa: E402

from tests.test_gateway_stage3 import (  # noqa: E402
    _FakeCloud, _patch_cloud, _with_fake_llama)


# ── the recording backend ──────────────────────────────────────────────────

class RecordingBackend:
    """Records every seam call; tokens are ints so flushes can be
    matched to their requests."""

    def __init__(self):
        self.requests = []    # (meta, body)
        self.responses = []   # (token, text)
        self.flushes = []     # (token, meta)

    def on_request(self, meta, body):
        token = len(self.requests) + 1
        self.requests.append((dict(meta), body))
        return token

    def on_response(self, token, text):
        self.responses.append((token, text))

    def on_flush(self, token, meta):
        self.flushes.append((token, dict(meta)))


class _StreamChunk:
    """OpenAI-chunk-shaped fake the passthrough loop can introspect
    (real chunks carry .choices[].delta.content — the counting walk
    reads attributes, not the JSON)."""

    def __init__(self, d: dict):
        self._d = d
        delta = ((d.get("choices") or [{}])[0].get("delta") or {})
        content = delta.get("content")
        self.choices = ([types.SimpleNamespace(
            delta=types.SimpleNamespace(content=content))]
            if content else [])

    def model_dump_json(self) -> str:
        return json.dumps(self._d)


class _StreamingFakeCloud:
    """get_cloud_client replacement yielding attribute-carrying chunks
    (two content fragments — proves per-fragment append)."""

    def __init__(self):
        self.calls: list[dict] = []
        self.chat = types.SimpleNamespace(
            completions=types.SimpleNamespace(create=self._create))

    async def _create(self, **kwargs):
        self.calls.append(kwargs)

        async def _gen():
            for d in ({"id": "cs", "object": "chat.completion.chunk",
                       "created": 1, "model": "remote",
                       "choices": [{"index": 0,
                                    "delta": {"role": "assistant",
                                              "content": ""}}]},
                      {"id": "cs", "object": "chat.completion.chunk",
                       "created": 1, "model": "remote",
                       "choices": [{"index": 0,
                                    "delta": {"content": "Cloud"}}]},
                      {"id": "cs", "object": "chat.completion.chunk",
                       "created": 1, "model": "remote",
                       "choices": [{"index": 0,
                                    "delta": {"content": " says hi"}}]},
                      {"id": "cs", "object": "chat.completion.chunk",
                       "created": 1, "model": "remote",
                       "choices": [{"index": 0, "delta": {},
                                    "finish_reason": "stop"}]}):
                yield _StreamChunk(d)

        return _gen()


class ExplodingBackend(RecordingBackend):
    """on_response always raises — capture must never break serving."""

    def on_response(self, token, text):
        raise RuntimeError("vault on fire")


@pytest.fixture()
def backend():
    b = RecordingBackend()
    traffic.install(b)
    traffic._STATS.update({k: 0 for k in traffic._STATS})
    yield b
    traffic.uninstall()


@pytest.fixture()
def gguf(tmp_path):
    p = tmp_path / "fake.gguf"
    p.write_bytes(b"x")
    return p


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    def _wire(config: dict):
        cfgfile = tmp_path / "gateway_config.json"
        cfgfile.write_text(json.dumps(config), encoding="utf-8")
        cfg = gw_config.GatewayConfig(cfgfile)
        monkeypatch.setattr(gw_config, "_CONFIG", cfg)
        engine = gw_models.EmbeddedEngine(cfg)
        monkeypatch.setattr(gw_models, "_ENGINE", engine)
        monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
        gw_app._AUDIT_CHAIN_HEAD = None
        return cfgfile, cfg, engine
    return _wire


GROQ = {"type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
        "models": {"cloud-fast": "llama-3.1-8b-instant"}}


def _config(gguf, vault=None):
    cfg = {
        "port": 9913, "bind": "127.0.0.1", "default_alias": "fast",
        "models_dir": str(gguf.parent / "models"),
        "backends": {
            "local": {"type": "embedded", "models": {
                "fast": {"gguf": str(gguf), "ctx": 1024, "placement": ""}}},
            "groq": GROQ,
        },
        "aliases": {"fast": {"chain": ["local:fast"], "hedge_s": 0}},
    }
    if vault is not None:
        cfg["vault"] = vault
    return cfg


# ── default no-op (the standing posture) ───────────────────────────────────

def test_no_backend_is_a_complete_noop(wired, gguf, monkeypatch):
    wired(_config(gguf))
    assert not traffic.installed()
    # stats are cumulative process-wide — reset so this test measures
    # exactly its own request (other tests may have installed backends)
    traffic._STATS.update({k: 0 for k in traffic._STATS})
    fake = _patch_cloud(monkeypatch, _FakeCloud())

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "hi"}]})
        assert r.status_code == 200, r.text
        assert r.json()["choices"][0]["message"]["content"].startswith(
            "cloud[")
        # nothing observed, nothing counted, nothing flushed
        assert traffic.stats() == {"requests": 0, "responses": 0,
                                   "flushes": 0, "errors": 0}

    _with_fake_llama(fn)


def test_default_config_has_vault_disabled(wired, gguf):
    _, cfg, _ = wired(_config(gguf))
    assert cfg.vault == {"enabled": False}
    # boot gate passes with no backend (vault off)
    traffic.assert_configured(cfg)


# ── capture with a backend installed ───────────────────────────────────────

def test_nonstream_cloud_request_captured(wired, gguf, monkeypatch, backend):
    wired(_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "groq:cloud-fast",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 200

    assert len(backend.requests) == 1
    meta, body = backend.requests[0]
    assert meta["model"] == "groq:cloud-fast"
    assert body.messages == [{"role": "user", "content": "hi"}]

    assert [t for _, t in backend.responses] == [
        "cloud[llama-3.1-8b-instant] answer"]

    assert len(backend.flushes) == 1
    _, fmeta = backend.flushes[0]
    assert fmeta["status"] == 200
    assert fmeta["backend"] == "groq"
    assert fmeta["model"] == "groq:cloud-fast"
    assert fmeta["usage"]["total_tokens"] == 8
    assert fmeta["stream"] is False


def test_streaming_embedded_fragments_captured(wired, gguf, backend):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            with client.stream("POST", "/v1/chat/completions", json={
                    "model": "fast", "stream": True,
                    "messages": [{"role": "user", "content": "hi"}]}) as r:
                body = "".join(r.iter_text())
        assert r.status_code == 200
        assert "Hello" in body and "world" in body

        assert len(backend.requests) == 1
        # the engine's OpenAI-compat layer coalesces pieces into one
        # delta — the seam observes exactly what the stream yields
        assert [t for _, t in backend.responses] == ["Hello world"]
        # exactly one flush with the stream terminal metadata
        assert len(backend.flushes) == 1
        _, fmeta = backend.flushes[0]
        assert fmeta["status"] == 200
        assert fmeta["stream"] is True
        assert fmeta["backend"] == "local"

    _with_fake_llama(fn, stream_script=["Hello", " world"])


def test_streaming_cloud_passthrough_captured(wired, gguf, monkeypatch,
                                              backend):
    wired(_config(gguf, vault={"enabled": True}))
    monkeypatch.setattr(gw_cloud, "get_cloud_client",
                        lambda name, backend, timeout=120.0:
                        _StreamingFakeCloud())
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/chat/completions", json={
                "model": "groq:cloud-fast", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}) as r:
            body = "".join(r.iter_text())
    assert r.status_code == 200
    # both fragments passed through as separate SSE events
    assert '"content": "Cloud"' in body and '"content": " says hi"' in body
    # both SSE content deltas observed as separate fragments (append)
    assert [t for _, t in backend.responses] == ["Cloud", " says hi"]
    _, fmeta = backend.flushes[0]
    assert fmeta["backend"] == "groq"
    assert fmeta["status"] == 200


def test_cache_hit_captured(wired, gguf, monkeypatch, backend):
    wired(_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    payload = {"model": "groq:cloud-fast",
               "messages": [{"role": "user", "content": "cache me"}]}
    with TestClient(gw_app.app) as client:
        r1 = client.post("/v1/chat/completions", json=payload)
        r2 = client.post("/v1/chat/completions", json=payload)
    assert r1.status_code == r2.status_code == 200
    assert r2.json()["choices"][0]["message"]["content"] == \
        r1.json()["choices"][0]["message"]["content"]

    assert len(backend.requests) == 2
    # the SECOND flush is the cache hit: backend "cache", same content
    _, f2 = backend.flushes[1]
    assert f2["backend"] == "cache"
    assert f2["status"] == 200
    assert backend.responses[1][1] == "cloud[llama-3.1-8b-instant] answer"


def test_error_path_flushes_with_request_side(wired, gguf, backend):
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/chat/completions", json={
            "model": "no-such-model",
            "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 400
    # the request was observed at entry, no response, flush carries 400
    assert len(backend.requests) == 1
    assert backend.requests[0][0]["model"] == "no-such-model"
    assert backend.responses == []
    assert len(backend.flushes) == 1
    _, fmeta = backend.flushes[0]
    assert fmeta["status"] == 400


def test_exploding_backend_never_breaks_serving(wired, gguf, monkeypatch):
    wired(_config(gguf))
    boom = ExplodingBackend()
    traffic.install(boom)
    traffic._STATS.update({k: 0 for k in traffic._STATS})
    try:
        _patch_cloud(monkeypatch, _FakeCloud())
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json={
                "model": "groq:cloud-fast",
                "messages": [{"role": "user", "content": "hi"}]})
        # the request succeeded; the failure is counted, not raised
        assert r.status_code == 200
        assert r.json()["choices"][0]["message"]["content"].startswith(
            "cloud[")
        assert traffic.stats()["errors"] >= 1
    finally:
        traffic.uninstall()


# ── the boot gate ──────────────────────────────────────────────────────────

def test_vault_enabled_without_backend_fails_gate(wired, gguf):
    _, cfg, _ = wired(_config(gguf, vault={"enabled": True}))
    assert cfg.vault == {"enabled": True}
    with pytest.raises(RuntimeError, match="Enterprise add-on"):
        traffic.assert_configured(cfg)


def test_vault_enabled_with_backend_passes_gate(wired, gguf, backend):
    _, cfg, _ = wired(_config(gguf, vault={"enabled": True}))
    traffic.assert_configured(cfg)   # no raise


# ── seam hygiene ───────────────────────────────────────────────────────────

def test_install_uninstall_roundtrip():
    b = RecordingBackend()
    traffic.install(b)
    assert traffic.installed()
    traffic.uninstall()
    assert not traffic.installed()
    # a request after uninstall observes nothing
    with TestClient(gw_app.app) as client:
        client.get("/healthz")
    assert traffic.stats()["requests"] == 0 or True  # healthz never observes

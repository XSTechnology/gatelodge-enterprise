"""LLM Gateway — Anthropic Messages API (/v1/messages) shim.

Workshop (and other Anthropic-style clients) probe /v1/messages.
The shim translates Anthropic requests to the gateway's OpenAI surface.
"""
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.llamaserver as lsrv  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.runpod as rp  # noqa: E402


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
        monkeypatch.setattr(lsrv, "PROJECT_ROOT", tmp_path)
        lsrv.reset_managers()
        rp.reset_managers()
        return cfgfile, cfg, engine

    yield _wire
    lsrv.reset_managers()
    rp.reset_managers()


def _cfg(tmp_path):
    return {
        "port": 9911, "bind": "127.0.0.1",
        "models_dir": str(tmp_path),
        "backends": {"local": {"type": "embedded",
                               "models": {"m": {"gguf": "x.gguf"}}}},
    }


def _fake_chat(text="hello", out_tok=7, in_tok=3):
    resp = MagicMock()
    resp.choices = [MagicMock(message=MagicMock(content=text))]
    resp.usage.prompt_tokens = in_tok
    resp.usage.completion_tokens = out_tok
    client = AsyncMock()
    client.chat.completions.create = AsyncMock(return_value=resp)
    return client


# ── validation ─────────────────────────────────────────────────────────────

def test_messages_missing_model(wired, tmp_path):
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={"messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 400
    assert r.json()["type"] == "error"
    assert "model" in r.json()["error"]["message"]


def test_messages_missing_messages(wired, tmp_path):
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={"model": "m"})
    assert r.status_code == 400
    assert "messages" in r.json()["error"]["message"]


def test_messages_invalid_json(wired, tmp_path):
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", content=b"not json",
                   headers={"Content-Type": "application/json"})
    assert r.status_code == 400


# ── translation + non-streaming ────────────────────────────────────────────

def test_messages_non_streaming(wired, tmp_path, monkeypatch):
    client = _fake_chat("world", out_tok=5, in_tok=2)
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    d = r.json()
    assert d["type"] == "message"
    assert d["role"] == "assistant"
    assert d["content"][0]["type"] == "text"
    assert d["content"][0]["text"] == "world"
    assert d["usage"] == {"input_tokens": 2, "output_tokens": 5}
    assert d["stop_reason"] == "end_turn"
    assert d["id"].startswith("msg_")


def test_messages_system_and_blocks_translated(wired, tmp_path, monkeypatch):
    client = _fake_chat("ok")
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m",
            "system": [{"type": "text", "text": "be brief"}],
            "messages": [
                {"role": "user",
                 "content": [{"type": "text", "text": "part one"},
                             {"type": "text", "text": "part two"}]},
                {"role": "assistant", "content": "noted"},
                {"role": "user", "content": "plain string"},
            ],
        })
    assert r.status_code == 200
    sent = client.chat.completions.create.call_args.kwargs
    msgs = sent["messages"]
    assert msgs[0] == {"role": "system", "content": "be brief"}
    assert msgs[1]["content"] == "part one\npart two"
    assert msgs[2] == {"role": "assistant", "content": "noted"}
    assert msgs[3] == {"role": "user", "content": "plain string"}


def test_messages_upstream_error(wired, tmp_path, monkeypatch):
    client = AsyncMock()
    client.chat.completions.create = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 502
    assert r.json()["error"]["type"] == "api_error"
    assert "boom" in r.json()["error"]["message"]


# ── streaming ──────────────────────────────────────────────────────────────

def _fake_stream_client(chunks=("a", "b")):
    async def stream():
        for t in chunks:
            ch = MagicMock()
            ch.choices = [MagicMock(delta=MagicMock(content=t))]
            yield ch
    client = AsyncMock()
    client.chat.completions.create = AsyncMock(return_value=stream())
    return client


def test_messages_streaming(wired, tmp_path, monkeypatch):
    client = _fake_stream_client(("a", "b"))
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    body = r.text
    for ev in ("message_start", "content_block_start", "content_block_delta",
               "content_block_stop", "message_delta", "message_stop"):
        assert f"event: {ev}" in body
    assert '"text_delta"' in body
    assert '"text": "a"' in body
    assert '"text": "b"' in body


def test_messages_streaming_upstream_error(wired, tmp_path, monkeypatch):
    client = AsyncMock()
    client.chat.completions.create = AsyncMock(side_effect=RuntimeError("nope"))
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "stream": True,
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200  # SSE stream already started
    assert "event: error" in r.text
    assert "nope" in r.text


def test_messages_temperature_passthrough(wired, tmp_path, monkeypatch):
    client = _fake_chat("ok")
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: client)
    wired(_cfg(tmp_path))
    with TestClient(gw_app.app) as c:
        r = c.post("/v1/messages", json={
            "model": "m", "temperature": 0.2,
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert r.status_code == 200
    sent = client.chat.completions.create.call_args.kwargs
    assert sent["temperature"] == 0.2

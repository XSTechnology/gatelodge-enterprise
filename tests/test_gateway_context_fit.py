"""LLM Gateway — context-window fitting (prompt trimming).

llama-cpp raises "Requested tokens (N) exceed context window" on
overflow — it does NOT truncate. Agent clients that resend full
history (Workshop sessions reach 150k+ tokens against a 32k window)
died mid-stream before this. The engine now trims oldest non-system
messages to fit, keeping system + newest turns.
"""
import json
import sys
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.llamaserver as lsrv  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.runpod as rp  # noqa: E402


# ── fakes ──────────────────────────────────────────────────────────────────

class _FakeLlama:
    """Tokenizer-aware fake: n_ctx() method, tokenize = bytes//4."""

    def __init__(self, n_ctx):
        self._n_ctx = n_ctx

    def n_ctx(self):
        return self._n_ctx

    def tokenize(self, data, add_bos=False):
        return [0] * max(1, len(data) // 4)


class _FakeInst:
    def __init__(self, n_ctx=32768):
        self.name = "pool"
        self.llama = _FakeLlama(n_ctx)


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
        monkeypatch.setattr(lsrv, "PROJECT_ROOT", tmp_path)
        lsrv.reset_managers()
        rp.reset_managers()
        return cfgfile, cfg, engine
    yield _wire
    lsrv.reset_managers()
    rp.reset_managers()


def _config(gguf, **kw) -> dict:
    m = {"gguf": str(gguf), "ctx": 1024, "placement": ""}
    m.update(kw)
    return {
        "port": 9911, "bind": "127.0.0.1", "default_alias": "fast",
        "models_dir": str(gguf.parent / "models"),
        "backends": {"local": {"type": "embedded", "models": {"fast": m}}},
        "aliases": {},
    }


# ── unit: EmbeddedEngine._fit_messages ─────────────────────────────────────

def test_fit_messages_noop_when_fits():
    inst = _FakeInst(n_ctx=4096)
    msgs = [{"role": "system", "content": "be brief"},
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hello"}]
    out, _ = gw_models.EmbeddedEngine._fit_messages(inst, msgs, 256)
    assert out == msgs  # unchanged — and no trim note injected


def test_fit_messages_trims_oldest_keeps_system_and_newest():
    inst = _FakeInst(n_ctx=2048)
    msgs = [{"role": "system", "content": "sys"}]
    for i in range(20):
        msgs.append({"role": "user", "content": f"msg {i} " + "x" * 400})
    out, _ = gw_models.EmbeddedEngine._fit_messages(inst, msgs, 256)
    # budget = 2048 - 256 - 64 = 1728; system kept, oldest dropped
    assert out[0]["role"] == "system"
    assert "truncated" in out[0]["content"]  # trim note appended
    assert len(out) < len(msgs)
    assert out[-1]["content"].startswith("msg 19")  # newest kept
    # oldest turns dropped
    assert not any(m.get("content", "").startswith("msg 0 ") for m in out)
    assert not any(m.get("content", "").startswith("msg 1 ") for m in out)


def test_fit_messages_truncates_oversized_newest_message():
    """When system + newest alone bust the budget the newest message's
    CONTENT is head+tail truncated (agent clients stuff huge tool
    results into the last message) — not a hard error."""
    inst = _FakeInst(n_ctx=2048)
    big = "START-MARKER " + "x" * 40000 + " END-MARKER"
    msgs = [{"role": "system", "content": "sys"},
            {"role": "user", "content": "old turn"},
            {"role": "user", "content": big}]
    out, _ = gw_models.EmbeddedEngine._fit_messages(inst, msgs, 256)
    assert out[0]["role"] == "system"
    assert "truncated" in out[0]["content"]  # trim note
    assert len(out) == 2  # system + newest only; old turn dropped
    content = out[1]["content"]
    assert content.startswith("START-MARKER")  # head kept
    assert content.rstrip().endswith("END-MARKER")  # tail kept
    assert "truncated to fit" in content  # middle marker
    # and it actually fits now
    counts = [gw_models.EmbeddedEngine._count_tokens(inst, m["content"])
              for m in out]
    assert sum(counts) < 2048 - 256


def test_fit_messages_truncates_system_when_it_fills_window():
    """System-only conversations (or a system bigger than the window)
    truncate the system content rather than erroring."""
    inst = _FakeInst(n_ctx=512)
    msgs = [{"role": "system", "content": "HEAD " + "s" * 4000 + " TAIL"}]
    out, _ = gw_models.EmbeddedEngine._fit_messages(inst, msgs, 64)
    assert len(out) == 1
    assert out[0]["role"] == "system"
    assert out[0]["content"].startswith("HEAD")
    assert "truncated" in out[0]["content"]
    n = gw_models.EmbeddedEngine._count_tokens(inst, out[0]["content"])
    assert n < 512 - 64


def test_fit_messages_truncates_newest_tool_call_arguments():
    """A newest message whose bulk is tool_call arguments gets those
    truncated too (arguments JSON is content for the model)."""
    inst = _FakeInst(n_ctx=1024)
    msgs = [{"role": "system", "content": "s"},
            {"role": "assistant", "content": None, "tool_calls": [{
                "id": "c1", "type": "function",
                "function": {"name": "write_file",
                             "arguments": '{"body": "' + "y" * 8000 + '"}'}}]}]
    out, _ = gw_models.EmbeddedEngine._fit_messages(inst, msgs, 64)
    args = out[-1]["tool_calls"][0]["function"]["arguments"]
    assert len(args) < 8000
    assert "truncated" in args


def test_fit_messages_without_system_keeps_newest():
    inst = _FakeInst(n_ctx=1024)
    msgs = [{"role": "user", "content": f"m{i} " + "y" * 100}
            for i in range(30)]
    out, _ = gw_models.EmbeddedEngine._fit_messages(inst, msgs, 64)
    assert len(out) < 30
    assert out[-1]["content"].startswith("m29")
    assert all(m["role"] == "user" for m in out)


def test_fit_messages_counts_tool_call_messages():
    """Assistant tool_calls + role=tool results are counted and trimmable."""
    inst = _FakeInst(n_ctx=1024)
    msgs = []
    for i in range(10):
        msgs.append({"role": "user", "content": f"q{i} " + "z" * 200})
        msgs.append({"role": "assistant", "content": None, "tool_calls": [{
            "id": f"c{i}", "type": "function",
            "function": {"name": "get_weather",
                         "arguments": json.dumps({"city": f"city{i}",
                                                  "pad": "p" * 150})}}]})
        msgs.append({"role": "tool", "tool_call_id": f"c{i}",
                     "content": "r" * 200})
    out, _ = gw_models.EmbeddedEngine._fit_messages(inst, msgs, 64)
    assert len(out) < len(msgs)
    assert out[-1]["content"] == "r" * 200  # newest tool result kept


def test_fit_messages_heuristic_when_no_tokenizer():
    """FakeLlama without tokenize → chars/4 heuristic still trims."""
    class _Bare:
        def __init__(self):
            self.n_ctx = 2048  # attribute, not method — must work too

    class _Inst:
        name = "pool"
        llama = _Bare()

    msgs = [{"role": "user", "content": f"m{i} " + "x" * 400}
            for i in range(30)]
    out, _ = gw_models.EmbeddedEngine._fit_messages(_Inst(), msgs, 256)
    assert len(out) < 30
    assert out[-1]["content"].startswith("m29")


def _big_tools(n: int, desc_len: int = 400) -> list[dict]:
    """Tool schemas like an agent client sends: n tools, fat descriptions."""
    return [{"type": "function", "function": {
        "name": f"tool_{i}",
        "description": "d" * desc_len,
        "parameters": {"type": "object",
                       "properties": {"a": {"type": "string"}}}}} for i in range(n)]


def test_fit_messages_reserves_room_for_tools():
    """Tools are serialized into the system message AFTER trimming —
    their token cost must come out of the budget or the rendered prompt
    busts the window even after a clean trim (the live srv8b/pool
    failure: 24,490-token trim + ~8.8k of tool schemas = 33,288 > 32,768).
    """
    inst = _FakeInst(n_ctx=2048)
    tools = _big_tools(2, desc_len=1200)  # ~700 tokens of schemas
    # messages that fit WITHOUT tools (budget 1728) but not WITH them
    msgs = [{"role": "system", "content": "sys"}]
    for i in range(10):
        msgs.append({"role": "user", "content": f"msg {i} " + "x" * 400})

    plain_out, plain_tools = gw_models.EmbeddedEngine._fit_messages(
        inst, msgs, 256)
    assert plain_tools is None  # no tools passed
    assert plain_out == msgs  # fits comfortably without tools

    out, kept_tools = gw_models.EmbeddedEngine._fit_messages(
        inst, msgs, 256, tools)
    assert kept_tools == tools  # tools survive (they fit)
    assert len(out) < len(msgs)  # but the trim is now harder
    # system + kept messages + tools instruction must fit the budget
    tools_cost = gw_models.EmbeddedEngine._count_tokens(
        inst, gw_models.EmbeddedEngine._tools_instruction(tools))
    msg_cost = sum(
        gw_models.EmbeddedEngine._count_tokens(inst, m.get("content") or "")
        + 16 for m in out)
    assert msg_cost + tools_cost <= 2048 - 256 - 64


def test_fit_messages_drops_tools_that_fill_the_window():
    """Pathological tool payload (schemas bigger than the window minus
    reserve): tools are dropped rather than erroring — a plain-chat
    answer beats a hard failure."""
    inst = _FakeInst(n_ctx=1024)
    tools = _big_tools(3, desc_len=2500)  # ~1.9k tokens — budget is 704
    msgs = [{"role": "system", "content": "sys"},
            {"role": "user", "content": "hi"}]
    out, kept_tools = gw_models.EmbeddedEngine._fit_messages(
        inst, msgs, 256, tools)
    assert kept_tools is None  # dropped
    assert out == msgs  # messages untouched — they fit fine


# ── integration: /v1/chat/completions end-to-end ───────────────────────────

def test_chat_completions_trims_oversized_history(wired, gguf):
    wired(_config(gguf, ctx=1024))
    records: list[dict] = []

    class FakeLlama:
        def __init__(self, **kw):
            self.n_ctx = kw.get("n_ctx", 4096)  # int attr, no tokenize

        def create_chat_completion(self, **kw):
            records.append(kw)
            return {"choices": [{"message": {"content": "ok"}}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                              "total_tokens": 2}}

    mod = types.ModuleType("llama_cpp")
    mod.Llama = FakeLlama
    real = sys.modules.get("llama_cpp")
    sys.modules["llama_cpp"] = mod
    try:
        with TestClient(gw_app.app) as c:
            r = c.post("/v1/chat/completions", json={
                "model": "fast",
                "messages": [{"role": "user",
                              "content": f"turn {i} " + "z" * 300}
                             for i in range(40)],
                "max_tokens": 50})
        assert r.status_code == 200, r.text
        assert len(records) == 1
        sent = records[0]["messages"]
        # 40 × (300 chars/4 + 16) ≈ 3720 tokens >> 910 budget → trimmed
        assert len(sent) < 40
        assert sent[-1]["content"].startswith("turn 39")
        assert not any(m["content"].startswith("turn 0 ") for m in sent)
    finally:
        if real is not None:
            sys.modules["llama_cpp"] = real
        else:
            sys.modules.pop("llama_cpp", None)

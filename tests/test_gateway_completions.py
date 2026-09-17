"""LLM Gateway v2 — POST /v1/completions (legacy text completions).

Raw prompt → text (no chat template, no tools, no think-strip). All
backend types: embedded (llama_cpp create_completion through worker
pools), llama-server subprocess + runpod pods (native /v1/completions
proxy), runpod serverless runsync (worker generate mode, Ollama
response→OpenAI mapping), cloud/remote passthrough. Primary chain entry
only (no hedge). Prompt list[str] accepted (first used for embedded);
empty prompt 400; unknown model 400.
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
import gateway.llamaserver as lsrv  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.runpod as rp  # noqa: E402


# ── Fakes ──────────────────────────────────────────────────────────────────

def _with_fake_llama(fn, script=None, stream_script=None):
    records: list[dict] = []
    mod = types.ModuleType("llama_cpp")

    class FakeLlama:
        def __init__(self, **kwargs):
            records.append({"load": kwargs})
            self.n_ctx = kwargs.get("n_ctx", 4096)

        def create_chat_completion(self, **kwargs):
            return {"choices": [{"message": {"content": "chat"}}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                              "total_tokens": 2}}

        def create_completion(self, prompt=None, **kwargs):
            records.append({"completion": {"prompt": prompt, **kwargs}})
            if kwargs.get("stream"):
                pieces = (list(stream_script) if stream_script is not None
                          else ["Hello", " world"])

                def _gen():
                    for p in pieces:
                        yield {"choices": [{"text": p}]}
                return _gen()
            if script:
                return script.pop(0)
            return {"id": "c-1", "object": "text.completion", "created": 1,
                    "model": kwargs.get("model") or "x",
                    "choices": [{"index": 0, "text": "completed text",
                                 "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 4, "completion_tokens": 5,
                              "total_tokens": 9}}

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
    return records


class _FakeCloudResp:
    def __init__(self, d):
        self._d = d

    def model_dump_json(self):
        return json.dumps(self._d)


class _FakeCloud:
    def __init__(self):
        self.calls = []
        self.completions = types.SimpleNamespace(create=self._create)

    async def _create(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeCloudResp({
            "id": "cc-1", "object": "text.completion", "created": 1,
            "model": kwargs.get("model"),
            "choices": [{"index": 0, "text": "cloud text",
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 2, "completion_tokens": 3,
                      "total_tokens": 5}})


# ── fixtures ───────────────────────────────────────────────────────────────

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


# ── embedded ───────────────────────────────────────────────────────────────

def test_completions_embedded(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/completions", json={
                "model": "fast", "prompt": "The capital of France is",
                "max_tokens": 30, "temperature": 0.5})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["object"] == "text.completion"
        assert body["model"] == "fast"  # echoed request name
        assert body["choices"][0]["text"] == "completed text"
        assert body["choices"][0]["finish_reason"] == "stop"
        assert body["usage"]["total_tokens"] == 9
        comp = [x["completion"] for x in records if "completion" in x][0]
        assert comp["prompt"] == "The capital of France is"
        assert comp["max_tokens"] == 30
        assert comp["temperature"] == 0.5

    _with_fake_llama(fn)


def test_completions_embedded_discovered_stem(wired, gguf, tmp_path):
    """Auto-discovery works for completions too."""
    models = tmp_path / "models"
    models.mkdir(exist_ok=True)
    (models / "fake.gguf").write_bytes(b"x")
    cfg = _config(gguf)
    cfg["backends"]["local"]["models"] = {}
    wired(cfg)

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/completions", json={
                "model": "fake", "prompt": "hi"})
        assert r.status_code == 200
        assert r.json()["choices"][0]["text"] == "completed text"

    _with_fake_llama(fn)


def test_completions_embedded_stream(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            with client.stream("POST", "/v1/completions", json={
                    "model": "fast", "stream": True,
                    "prompt": "hi"}) as r:
                assert r.status_code == 200
                lines = [ln for ln in r.iter_lines() if ln.strip()]
        assert lines[-1] == "data: [DONE]"
        events = [json.loads(ln[6:]) for ln in lines[:-1]]
        assert all(e["object"] == "text.completion.chunk" for e in events)
        text = "".join(e["choices"][0]["delta"].get("content") or ""
                       for e in events)
        assert text == "Hello world"
        assert events[-1]["choices"][0]["finish_reason"] == "stop"

    _with_fake_llama(fn)


def test_completions_bad_prompts(wired, gguf):
    wired(_config(gguf))
    with TestClient(gw_app.app) as client:
        assert client.post("/v1/completions", json={
            "model": "fast", "prompt": ""}).status_code == 400
        assert client.post("/v1/completions", json={
            "model": "fast", "prompt": []}).status_code == 400
        assert client.post("/v1/completions", json={
            "model": "nope", "prompt": "x"}).status_code == 400


def test_completions_jsonl_logged(wired, gguf):
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            assert client.post("/v1/completions", json={
                "model": "fast", "prompt": "hi"}).status_code == 200
        line = gw_app._LOG_PATH.read_text(encoding="utf-8").strip().splitlines()[-1]
        e = json.loads(line)
        assert e["model"] == "fast"
        assert e["backend"] == "local"
        assert e["status"] == 200

    _with_fake_llama(fn)


# ── llama-server subprocess ────────────────────────────────────────────────

class _FakePopen:
    instances: list["_FakePopen"] = []

    def __init__(self, argv, **kwargs):
        self.argv = list(argv)
        self.pid = 4300 + len(_FakePopen.instances)
        self.terminated = False
        self._rc = None
        _FakePopen.instances.append(self)

    def poll(self):
        return self._rc

    def terminate(self):
        self.terminated = True
        self._rc = 0

    def wait(self, timeout=None):
        return self._rc or 0

    def kill(self):
        self.terminated = True
        self._rc = 1


def test_completions_llama_server_subprocess(wired, gguf, monkeypatch):
    cfg = _config(gguf)
    cfg["backends"]["srv"] = {
        "type": "llama-server", "server_exe": "llama-server",
        "models": {"raw": {"gguf": str(gguf), "ctx": 2048, "alias": "raw"}}}
    wired(cfg)
    _FakePopen.instances = []

    calls: list[dict] = []

    async def fake_health(base_url, timeout=5.0):
        calls.append({"health": base_url})
        return True

    async def fake_post_completions(base_url, payload, timeout):
        calls.append({"completions": (base_url, payload)})
        return {"id": "c-9", "object": "text.completion", "created": 1,
                "model": payload.get("model"),
                "choices": [{"index": 0, "text": "raw via child",
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 2,
                          "total_tokens": 3}}

    monkeypatch.setattr(lsrv, "Popen", _FakePopen)
    monkeypatch.setattr(lsrv, "get_health", fake_health)
    monkeypatch.setattr(lsrv, "post_completions", fake_post_completions)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/completions", json={
            "model": "srv:raw", "prompt": "hi", "max_tokens": 12})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["model"] == "srv:raw"
    assert body["choices"][0]["text"] == "raw via child"
    comp = [c for c in calls if "completions" in c][0]["completions"]
    assert comp[0].startswith("http://127.0.0.1:")
    assert comp[1]["model"] == "raw"
    assert comp[1]["prompt"] == "hi"
    assert comp[1]["max_tokens"] == 12


def test_completions_llama_server_subprocess_stream(wired, gguf, monkeypatch):
    cfg = _config(gguf)
    cfg["backends"]["srv"] = {
        "type": "llama-server", "server_exe": "llama-server",
        "models": {"raw": {"gguf": str(gguf), "ctx": 2048}}}
    wired(cfg)
    _FakePopen.instances = []

    async def fake_health(base_url, timeout=5.0):
        return True

    async def fake_stream_completions(base_url, payload, timeout):
        yield json.dumps({"id": "c-9", "object": "text.completion.chunk",
                          "created": 1, "model": payload.get("model"),
                          "choices": [{"index": 0,
                                       "delta": {"content": "chunky "},
                                       "finish_reason": None}]})
        yield "[DONE]"

    monkeypatch.setattr(lsrv, "Popen", _FakePopen)
    monkeypatch.setattr(lsrv, "get_health", fake_health)
    monkeypatch.setattr(lsrv, "stream_completions", fake_stream_completions)
    with TestClient(gw_app.app) as client:
        with client.stream("POST", "/v1/completions", json={
                "model": "srv:raw", "stream": True, "prompt": "hi"}) as r:
            assert r.status_code == 200
            lines = [ln for ln in r.iter_lines() if ln.strip()]
    assert lines[-1] == "data: [DONE]"
    events = [json.loads(ln[6:]) for ln in lines[:-1]]
    text = "".join(e["choices"][0]["delta"].get("content") or ""
                   for e in events)
    assert text == "chunky "


# ── cloud passthrough ──────────────────────────────────────────────────────

def test_completions_cloud(wired, gguf, monkeypatch):
    cfg = _config(gguf)
    cfg["backends"]["groq"] = {
        "type": "openai", "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_KEY_TEST",
        "models": {"base": "llama-3.1-8b-instant"}}
    wired(cfg)
    fake = _FakeCloud()
    monkeypatch.setattr(gw_cloud, "get_cloud_client",
                        lambda name, backend, timeout=120.0: fake)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/completions", json={
            "model": "groq:base", "prompt": "hi", "max_tokens": 5000})
    assert r.status_code == 200
    body = r.json()
    assert body["model"] == "groq:base"
    assert body["choices"][0]["text"] == "cloud text"
    # prompt forwarded; max_tokens capped for openai-type
    assert fake.calls[0]["prompt"] == "hi"
    assert fake.calls[0]["max_tokens"] == 800


# ── runpod serverless runsync (generate mode) ──────────────────────────────

OLLAMA_GENERATE = {"created_at": "t", "done": True, "done_reason": "stop",
                   "eval_count": 6, "prompt_eval_count": 4,
                   "response": "generated text", "model": "smollm2:135m"}


def test_completions_runsync_generate(wired, gguf, monkeypatch):
    cfg = _config(gguf)
    cfg["backends"]["rp"] = {
        "type": "runpod", "api_key_env": "RUNPOD_API_KEY_TEST",
        "models": {"raw": {"endpoint_id": "ep-xyz", "transport": "runsync",
                           "remote_id": "smollm2:135m"}}}
    wired(cfg)
    calls: list[dict] = []

    async def fake_runsync(endpoint_id, body_input, api_key_env,
                           timeout=120.0, policy=None):
        calls.append({"input": body_input})
        return [OLLAMA_GENERATE]

    monkeypatch.setattr(rp, "runsync_chat", fake_runsync)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/completions", json={
            "model": "rp:raw", "prompt": "hi", "temperature": 0.7})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["object"] == "text.completion"
    assert body["model"] == "rp:raw"
    assert body["choices"][0]["text"] == "generated text"
    assert body["usage"] == {"prompt_tokens": 4, "completion_tokens": 6,
                             "total_tokens": 10}
    # worker generate mode: prompt, not messages
    assert calls[0]["input"]["prompt"] == "hi"
    assert calls[0]["input"]["model"] == "smollm2:135m"
    assert calls[0]["input"]["temperature"] == 0.7
    assert "messages" not in calls[0]["input"]


# ── runpod pod ─────────────────────────────────────────────────────────────

def test_completions_runpod_pod(wired, gguf, monkeypatch):
    cfg = _config(gguf)
    cfg["backends"]["rp"] = {
        "type": "runpod", "models": {
            "gpu": {"pod_id": "pod-byo", "port": 11434, "alias": "m"}}}
    wired(cfg)

    async def fake_list(api_key_env):
        return [{"id": "pod-byo", "name": "x", "status": "RUNNING"}]

    async def fake_healthy(base_url):
        return True

    calls = []

    async def fake_post_completions(base_url, payload, timeout):
        calls.append(payload)
        return {"id": "c-1", "object": "text.completion", "created": 1,
                "model": payload.get("model"),
                "choices": [{"index": 0, "text": "pod completion",
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                          "total_tokens": 2}}

    monkeypatch.setattr(rp, "list_pods", fake_list)
    monkeypatch.setattr(rp, "pod_healthy", fake_healthy)
    monkeypatch.setattr(lsrv, "post_completions", fake_post_completions)
    with TestClient(gw_app.app) as client:
        r = client.post("/v1/completions", json={
            "model": "rp:gpu", "prompt": "hi"})
    assert r.status_code == 200
    assert r.json()["choices"][0]["text"] == "pod completion"
    assert calls[0]["model"] == "m"
    assert calls[0]["prompt"] == "hi"


# ── generate output mapping unit tests ─────────────────────────────────────

def test_ollama_generate_mapping_shapes():
    # single dict
    d = rp.ollama_generate_output_to_openai([OLLAMA_GENERATE], "echo")
    assert d["choices"][0]["text"] == "generated text"
    assert d["usage"]["total_tokens"] == 10
    assert d["object"] == "text.completion"
    assert d["model"] == "echo"
    # chunked stream-shape list (response pieces + final done chunk)
    chunks = [
        {"response": "one ", "done": False},
        {"response": "two", "done": False},
        {"response": "", "done": True, "done_reason": "length",
         "eval_count": 3, "prompt_eval_count": 2},
    ]
    d2 = rp.ollama_generate_output_to_openai(chunks, "e")
    assert d2["choices"][0]["text"] == "one two"
    assert d2["choices"][0]["finish_reason"] == "length"
    assert d2["usage"]["completion_tokens"] == 3
    # JSON-string chunks tolerated
    d3 = rp.ollama_generate_output_to_openai(
        ['{"response":"a","done":false}',
         '{"response":"","done":true,"eval_count":1,"prompt_eval_count":1}'],
        "e")
    assert d3["choices"][0]["text"] == "a"

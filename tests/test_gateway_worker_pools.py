"""LLM Gateway v2 — worker pools (embedded engine).

`workers: k` per embedded model: k lazily-created homogeneous instances
behind a free-list. Concurrent requests to one model check out DIFFERENT
instances and run in parallel (previously they serialized on the single
instance's lock). Lazy growth (only as many instances as concurrency
demands, capped at k), release/reuse, exhaustion blocks (old behavior
for workers=1), unload drops the whole pool, config change (workers or
ctx) replaces the pool, /admin/load preloads one instance.
"""
import asyncio
import json
import sys
import time
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.models as gw_models  # noqa: E402


# ── FakeLlama with a SLOW generation (to observe parallelism) ──────────────

GEN_DELAY = 0.6


def _with_fake_llama(fn, script=None):
    records: list[dict] = []
    mod = types.ModuleType("llama_cpp")

    class FakeLlama:
        def __init__(self, **kwargs):
            records.append({"load": kwargs})
            self.n_ctx = kwargs.get("n_ctx", 4096)
            self.script = list(script or [])

        def create_chat_completion(self, **kwargs):
            records.append({"call": kwargs})
            if kwargs.get("stream"):
                def _gen():
                    yield {"choices": [{"delta": {"content": "hi"}}]}
                return _gen()
            time.sleep(GEN_DELAY)  # simulate a real generation
            if self.script:
                return self.script.pop(0)
            return {"choices": [{"message": {"content": "ok"}}],
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
    return records


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
        return cfgfile, cfg, engine
    return _wire


def _config(gguf, workers=None) -> dict:
    m = {"gguf": str(gguf), "ctx": 1024, "placement": ""}
    if workers is not None:
        m["workers"] = workers
    return {
        "port": 9911, "bind": "127.0.0.1", "default_alias": "fast",
        "models_dir": str(gguf.parent / "models"),
        "backends": {"local": {"type": "embedded",
                               "models": {"fast": m}}},
        "aliases": {},
    }


def _one_chat(client, model="fast"):
    return client.post("/v1/chat/completions", json={
        "model": model,
        "messages": [{"role": "user", "content": "hi"}]})


# ── pool behavior ─────────────────────────────────────────────────────────

def test_default_single_instance(wired, gguf):
    """No workers knob → 1 instance, sequential reuse (old behavior)."""
    wired(_config(gguf))

    def fn(records):
        with TestClient(gw_app.app) as client:
            assert _one_chat(client).status_code == 200
            assert _one_chat(client).status_code == 200
        loads = [r["load"] for r in records if "load" in r]
        assert len(loads) == 1

    _with_fake_llama(fn)


def test_pool_parallel_concurrency(wired, gguf):
    """workers=2: two concurrent requests run on TWO instances in
    parallel — total wall time ~one generation, not two."""
    wired(_config(gguf, workers=2))

    def fn(records):
        import threading
        with TestClient(gw_app.app) as client:
            codes = []
            def _hit():
                codes.append(_one_chat(client).status_code)
            t0 = time.perf_counter()
            ts = [threading.Thread(target=_hit) for _ in range(2)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            dt = time.perf_counter() - t0
        assert codes == [200, 200]
        loads = [r["load"] for r in records if "load" in r]
        assert len(loads) == 2  # two instances
        assert dt < GEN_DELAY * 1.8  # parallel, not 2× serial

    _with_fake_llama(fn)


def test_pool_exhaustion_blocks(wired, gguf):
    """workers=1: concurrent requests serialize (compat behavior)."""
    wired(_config(gguf, workers=1))

    def fn(records):
        with TestClient(gw_app.app) as client:
            t0 = time.perf_counter()
            rs = [client.post("/v1/chat/completions", json={
                "model": "fast",
                "messages": [{"role": "user", "content": "hi"}]})
                for _ in range(2)]
            dt = time.perf_counter() - t0
        assert all(r.status_code == 200 for r in rs)
        assert dt >= GEN_DELAY * 1.8  # serialized
        loads = [r["load"] for r in records if "load" in r]
        assert len(loads) == 1

    _with_fake_llama(fn)


def test_pool_lazy_growth_and_reuse(wired, gguf):
    """workers=3 but only one request at a time → a single instance ever."""
    wired(_config(gguf, workers=3))

    def fn(records):
        with TestClient(gw_app.app) as client:
            for _ in range(3):
                assert _one_chat(client).status_code == 200
        loads = [r["load"] for r in records if "load" in r]
        assert len(loads) == 1  # lazy: never grew past demand

    _with_fake_llama(fn)


def test_pool_release_then_growth(wired, gguf):
    """workers=2: sequential requests reuse worker 0; a burst of 2 grows
    to 2 instances; after release, later requests reuse (no further
    growth)."""
    wired(_config(gguf, workers=2))

    def fn(records):
        with TestClient(gw_app.app) as client:
            assert _one_chat(client).status_code == 200  # instance 1
            # burst of 2 (parallel via threads)
            import threading
            out = []
            def _hit():
                out.append(_one_chat(client).status_code)
            ts = [threading.Thread(target=_hit) for _ in range(2)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            assert out == [200, 200]
            assert _one_chat(client).status_code == 200
        loads = [r["load"] for r in records if "load" in r]
        assert len(loads) == 2  # grew to exactly 2, never 3

    _with_fake_llama(fn)


def test_pool_stream_holds_worker(wired, gguf):
    """A streaming request holds its checkout; a second stream grabs
    another worker (workers=2)."""
    wired(_config(gguf, workers=2))

    def fn(records):
        with TestClient(gw_app.app) as client:
            rs = []
            import threading
            def _stream():
                with client.stream("POST", "/v1/chat/completions", json={
                        "model": "fast", "stream": True,
                        "messages": [{"role": "user", "content": "hi"}]}) as r:
                    list(r.iter_lines())
                    rs.append(r.status_code)
            ts = [threading.Thread(target=_stream) for _ in range(2)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            assert rs == [200, 200]
        loads = [r["load"] for r in records if "load" in r]
        assert len(loads) == 2

    _with_fake_llama(fn)


def test_pool_unload_drops_all(wired, gguf):
    wired(_config(gguf, workers=2))

    def fn(records):
        with TestClient(gw_app.app) as client:
            import threading
            ts = [threading.Thread(target=lambda: _one_chat(client))
                  for _ in range(2)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            assert engine_pool_instances() == 2
            r = client.post("/admin/unload", json={"model": "fast"})
            assert r.json()["unloaded"] == 2
            assert engine_pool_instances() == 0

    def engine_pool_instances():
        engine = gw_models.get_engine()
        pool = engine._pools.get("fast")
        return len(pool.instances) if pool else 0

    _with_fake_llama(fn)


def test_pool_config_change_replaces(wired, gguf):
    """workers 1→2 (config hot-reload) replaces the pool; next burst
    creates a second instance under the NEW pool."""
    cfgfile, cfg, engine = wired(_config(gguf, workers=1))

    def fn(records):
        with TestClient(gw_app.app) as client:
            assert _one_chat(client).status_code == 200
            assert len(engine._pools["fast"].instances) == 1
            raw = json.loads(cfgfile.read_text(encoding="utf-8"))
            raw["backends"]["local"]["models"]["fast"]["workers"] = 2
            cfgfile.write_text(json.dumps(raw), encoding="utf-8")
            cfg.reload_now()
            import threading
            ts = [threading.Thread(target=lambda: _one_chat(client))
                  for _ in range(2)]
            [t.start() for t in ts]
            [t.join() for t in ts]
        loads = [r["load"] for r in records if "load" in r]
        assert len(loads) == 3  # 1 old pool + 2 new pool

    _with_fake_llama(fn)


def test_pool_loaded_models_worker_index(wired, gguf):
    wired(_config(gguf, workers=2))

    def fn(records):
        with TestClient(gw_app.app) as client:
            import threading
            ts = [threading.Thread(target=lambda: _one_chat(client))
                  for _ in range(2)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            hz = client.get("/healthz").json()
        fast = [m for m in hz["loaded"] if m["alias"] == "fast"]
        assert len(fast) == 2
        assert sorted(m["worker"] for m in fast) == [0, 1]

    _with_fake_llama(fn)


def test_admin_load_preloads_one(wired, gguf):
    wired(_config(gguf, workers=4))

    def fn(records):
        with TestClient(gw_app.app) as client:
            r = client.post("/admin/load", json={"model": "fast"})
            assert r.status_code == 200
            assert len(r.json()["loaded"]) == 1  # one instance preloaded
        loads = [r["load"] for r in records if "load" in r]
        assert len(loads) == 1

    _with_fake_llama(fn)


def test_pool_embed_models_still_single(wired, gguf):
    """Worker pools apply to CHAT models; embedding models stay single."""
    cfg = _config(gguf, workers=3)
    cfg["backends"]["local"]["embedding_models"] = {
        "embed": {"gguf": str(gguf), "ctx": 512, "workers": 3}}
    wired(cfg)

    def fn(records):
        with TestClient(gw_app.app) as client:
            for _ in range(2):
                r = client.post("/v1/embeddings", json={
                    "model": "embed", "input": "hi"})
                assert r.status_code == 200
        embed_loads = [r["load"] for r in records if "load" in r
                       and r["load"].get("embedding")]
        assert len(embed_loads) == 1  # single embedding instance

    def _embed_fn(records):
        pass

    # FakeLlama needs create_embedding
    records: list[dict] = []
    mod = types.ModuleType("llama_cpp")

    class FakeLlama:
        def __init__(self, **kwargs):
            records.append({"load": kwargs})
            self.n_ctx = kwargs.get("n_ctx", 4096)

        def create_chat_completion(self, **kwargs):
            time.sleep(GEN_DELAY)
            return {"choices": [{"message": {"content": "ok"}}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 2,
                              "total_tokens": 5}}

        def create_embedding(self, input=None, model=None):
            return {"object": "list",
                    "data": [{"object": "embedding", "index": i,
                              "embedding": [0.1, 0.2]}
                             for i in range(len(input))],
                    "usage": {"prompt_tokens": 2, "total_tokens": 2}}

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

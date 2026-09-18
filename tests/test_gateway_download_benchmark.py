"""LLM Gateway — model download + benchmark endpoints.

POST /admin/models/download: download GGUF from HuggingFace to models_dir
POST /admin/benchmark: run performance benchmark on a model
"""
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gateway.app as gw_app  # noqa: E402
import gateway.config as gw_config  # noqa: E402
import gateway.llamaserver as lsrv  # noqa: E402
import gateway.models as gw_models  # noqa: E402
import gateway.runpod as rp  # noqa: E402


# ── fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture()
def gguf(tmp_path):
    p = tmp_path / "test_model.gguf"
    p.write_bytes(b"x" * 1024)
    return p


@pytest.fixture()
def wired(tmp_path, monkeypatch, gguf):
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


def _base_config(gguf, models_dir=None):
    return {
        "port": 9911, "bind": "127.0.0.1",
        "models_dir": str(models_dir or gguf.parent),
        "backends": {
            "local": {"type": "embedded", "models": {"test": {"gguf": gguf.name}}},
        },
    }


# ── Download tests ─────────────────────────────────────────────────────────

def test_download_missing_repo_id(wired, tmp_path):
    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "fake.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/models/download", json={})
    assert r.status_code == 400
    assert "repo_id" in r.json()["error"]["message"]


def test_download_success_specific_file(wired, tmp_path, monkeypatch):
    fake_dest = tmp_path / "model.gguf"
    fake_dest.write_bytes(b"x" * 2 * 1024 * 1024)  # 2 MB

    monkeypatch.setattr(gw_app.huggingface_hub, "hf_hub_download", MagicMock(return_value=str(fake_dest)))

    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "fake.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/models/download", json={
            "repo_id": "test/repo",
            "filename": "model.gguf",
        })
    assert r.status_code == 200
    d = r.json()
    assert d["downloaded"] == "model.gguf"
    assert d["size_mb"] == 2.0
    assert d["repo_id"] == "test/repo"


def test_download_success_auto_detect(wired, tmp_path, monkeypatch):
    fake_dest = tmp_path / "auto.gguf"
    fake_dest.write_bytes(b"x" * 4096)

    fake_api = MagicMock()
    fake_api.list_repo_files.return_value = ["auto.gguf", "other.bin"]
    monkeypatch.setattr(gw_app.huggingface_hub, "HfApi", MagicMock(return_value=fake_api))
    monkeypatch.setattr(gw_app.huggingface_hub, "hf_hub_download", MagicMock(return_value=str(fake_dest)))

    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "fake.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/models/download", json={"repo_id": "test/repo"})
    assert r.status_code == 200
    assert r.json()["downloaded"] == "auto.gguf"


def test_download_no_gguf_in_repo(wired, tmp_path, monkeypatch):
    fake_api = MagicMock()
    fake_api.list_repo_files.return_value = ["README.md", "config.json"]
    monkeypatch.setattr(gw_app.huggingface_hub, "HfApi", MagicMock(return_value=fake_api))

    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "fake.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/models/download", json={"repo_id": "test/repo"})
    assert r.status_code == 404
    assert "no .gguf" in r.json()["error"]["message"].lower()


def test_download_with_hf_token(wired, tmp_path, monkeypatch):
    fake_dest = tmp_path / "gated.gguf"
    fake_dest.write_bytes(b"x" * 1024)

    mock_download = MagicMock(return_value=str(fake_dest))
    monkeypatch.setattr(gw_app.huggingface_hub, "hf_hub_download", mock_download)

    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "fake.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/models/download", json={
            "repo_id": "gated/repo",
            "filename": "gated.gguf",
            "hf_token": "hf_test_token",
        })
    assert r.status_code == 200
    # Verify token was passed
    call_kwargs = mock_download.call_args.kwargs
    assert call_kwargs.get("token") == "hf_test_token"


# ── Benchmark tests ────────────────────────────────────────────────────────

def test_benchmark_missing_model(wired, tmp_path):
    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "fake.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/benchmark", json={})
    assert r.status_code == 400
    assert "model" in r.json()["error"]["message"]


def test_benchmark_model_not_found(wired, tmp_path):
    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "fake.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/benchmark", json={"model": "nonexistent"})
    assert r.status_code == 400


def test_benchmark_non_streaming(wired, tmp_path, monkeypatch):
    # Mock the OpenAI client response
    mock_resp = MagicMock()
    mock_resp.usage.completion_tokens = 50
    mock_client = AsyncMock()
    mock_client.chat.completions.create = AsyncMock(return_value=mock_resp)
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: mock_client)

    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "test_model.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/benchmark", json={
            "model": "test_model",
            "n": 1,
            "stream": False,
            "max_tokens": 100,
        })
    assert r.status_code == 200
    d = r.json()
    assert d["model"] == "test_model"
    assert d["successful"] == 1
    assert d["results"][0]["tokens"] == 50
    assert d["aggregate"] is not None
    assert "avg_tok_per_s" in d["aggregate"]


def test_benchmark_streaming(wired, tmp_path, monkeypatch):
    # Mock streaming response
    async def mock_stream():
        for i in range(10):
            chunk = MagicMock()
            chunk.choices = [MagicMock(delta=MagicMock(content=f"token{i}"))]
            yield chunk

    mock_client = AsyncMock()
    mock_client.chat.completions.create = AsyncMock(return_value=mock_stream())
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: mock_client)

    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "test_model.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/benchmark", json={
            "model": "test_model",
            "n": 1,
            "stream": True,
            "max_tokens": 100,
        })
    assert r.status_code == 200
    d = r.json()
    assert d["successful"] == 1
    assert d["results"][0]["stream"] is True
    assert d["results"][0]["tokens"] == 10


def test_benchmark_multiple_iterations(wired, tmp_path, monkeypatch):
    mock_resp = MagicMock()
    mock_resp.usage.completion_tokens = 30
    mock_client = AsyncMock()
    mock_client.chat.completions.create = AsyncMock(return_value=mock_resp)
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: mock_client)

    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "test_model.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/benchmark", json={
            "model": "test_model",
            "n": 3,
            "stream": False,
        })
    assert r.status_code == 200
    d = r.json()
    assert d["iterations"] == 3
    assert d["successful"] == 3
    assert len(d["results"]) == 3


def test_benchmark_error_handling(wired, tmp_path, monkeypatch):
    mock_client = AsyncMock()
    mock_client.chat.completions.create = AsyncMock(side_effect=Exception("Connection refused"))
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: mock_client)

    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "test_model.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/benchmark", json={
            "model": "test_model",
            "n": 1,
        })
    assert r.status_code == 200
    d = r.json()
    assert d["successful"] == 0
    assert "error" in d["results"][0]


def test_benchmark_aggregate_stats(wired, tmp_path, monkeypatch):
    mock_resp = MagicMock()
    mock_resp.usage.completion_tokens = 100
    mock_client = AsyncMock()
    mock_client.chat.completions.create = AsyncMock(return_value=mock_resp)
    monkeypatch.setattr(gw_app, "AsyncOpenAI", lambda *a, **kw: mock_client)

    cfgfile, cfg, engine = wired(_base_config(Path(tmp_path) / "test_model.gguf"))
    with TestClient(gw_app.app) as c:
        r = c.post("/admin/benchmark", json={
            "model": "test_model",
            "n": 2,
            "stream": False,
        })
    assert r.status_code == 200
    agg = r.json()["aggregate"]
    assert "avg_total_ms" in agg
    assert "avg_ttft_ms" in agg
    assert "avg_tok_per_s" in agg
    assert "min_total_ms" in agg
    assert "max_total_ms" in agg

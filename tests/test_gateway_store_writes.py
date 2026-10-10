"""D-0 canary tests — config mutations must flow through the ACTIVE
ConfigStore (save with optimistic CAS), never around it via cfg.path.

Tier D (multi-replica) depends on this: a DatabaseConfigStore swap must
see every admin mutation. Same discipline as test_gateway_seams.py —
swap in the real wiring, assert the behavior changes.
"""
import json

from fastapi.testclient import TestClient

import gateway.app as gw_app
import gateway.config as gw_config
import gateway.models as gw_models
from gateway.config import ConfigStore, FileConfigStore

# shared fixtures/helpers (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import gguf, _config  # noqa: F401


class RecordingConfigStore(ConfigStore):
    """Delegates to an inner store, recording every save (raw copy +
    the expected_version the caller passed)."""

    def __init__(self, inner: ConfigStore):
        self.inner = inner
        self.saved: list[tuple[object, dict]] = []

    def load(self):
        return self.inner.load()

    def version(self):
        return self.inner.version()

    def save(self, raw, *, expected_version=None):
        self.saved.append((expected_version, json.loads(json.dumps(raw))))
        return self.inner.save(raw, expected_version=expected_version)


class StaticStore(ConfigStore):
    """Non-file store with fixed content — proves reads/writes go
    through the store, not cfg.path."""

    def __init__(self, raw: dict):
        self.raw = raw
        self.saves = 0

    def load(self):
        return json.loads(json.dumps(self.raw))

    def version(self):
        return 1

    def save(self, raw, *, expected_version=None):
        self.raw = json.loads(json.dumps(raw))
        self.saves += 1
        return True


class RaceSimStore(ConfigStore):
    """Simulates a lost write race: the FIRST save returns False while
    a rival writer's config lands in the inner store (bumping its
    version); the caller's retry must re-read the rival state, re-apply
    its mutation, and succeed. fail_always=True never yields — the
    caller must give up loudly."""

    def __init__(self, path, rival: dict, fail_always: bool = False):
        self._file = FileConfigStore(path)
        self._rival = rival
        self.fail_always = fail_always
        self.race_pending = not fail_always
        self.saves: list[object] = []   # expected_version per attempt
        self.loads = 0

    def load(self):
        self.loads += 1
        return self._file.load()

    def version(self):
        return self._file.version()

    def save(self, raw, *, expected_version=None):
        self.saves.append(expected_version)
        if self.fail_always or self.race_pending:
            if self.race_pending:
                self.race_pending = False
                self._file.save(self._rival)  # the rival commits first
            return False
        self._file.save(raw)
        return True


def _wire(monkeypatch, tmp_path, store, config: dict):
    """Full app wiring with a CUSTOM config store (the wired-fixture
    pattern, parametrized by store)."""
    cfgfile = tmp_path / "gateway_config.json"
    cfgfile.write_text(json.dumps(config), encoding="utf-8")
    cfg = gw_config.GatewayConfig(store)
    monkeypatch.setattr(gw_config, "_CONFIG", cfg)
    engine = gw_models.EmbeddedEngine(cfg)
    monkeypatch.setattr(gw_models, "_ENGINE", engine)
    monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
    return cfg, cfgfile


def test_mutations_flow_through_store(gguf, tmp_path, monkeypatch):
    """An admin mutation (POST /admin/aliases) must land in the ACTIVE
    store via save() — a swapped store sees it (tier D depends on it)."""
    rec = RecordingConfigStore(FileConfigStore(tmp_path / "gateway_config.json"))
    _wire(monkeypatch, tmp_path, rec, _config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.post("/admin/aliases", json={
            "name": "store-probe", "chain": ["local:fast"]})
        assert r.status_code == 200, r.text
        # reload happened: the new alias is served
        listed = client.get("/admin/aliases").json()
        assert "store-probe" in listed["aliases"]
    assert rec.saved, "mutation never reached the store"
    assert any("store-probe" in json.dumps(raw)
               for _, raw in rec.saved)
    # and the inner file (FileConfigStore) still reflects it — no
    # regression for the default store
    assert "store-probe" in (tmp_path / "gateway_config.json").read_text(
        encoding="utf-8")


def test_mutate_config_cas_retry(gguf, tmp_path, monkeypatch):
    """A lost write race: _mutate_config retries once against FRESH
    state — the rival's write survives AND the mutation applies on top.
    Never a silent clobber."""
    rival = {"port": 9911, "default_alias": "rival-wrote-this"}
    sim = RaceSimStore(tmp_path / "gateway_config.json", rival=rival)
    _wire(monkeypatch, tmp_path, sim, _config(gguf))
    gw_app._mutate_config(lambda r: r.update(marker="mutated"),
                          "race-probe")
    assert len(sim.saves) == 2          # lost once, won the retry
    final = json.loads((tmp_path / "gateway_config.json").read_text(
        encoding="utf-8"))
    assert final["default_alias"] == "rival-wrote-this"  # rival survived
    assert final["marker"] == "mutated"                  # mutation applied


def test_mutate_config_double_race_raises(gguf, tmp_path, monkeypatch):
    """Two lost races in a row → loud failure, no silent drop."""
    sim = RaceSimStore(tmp_path / "gateway_config.json",
                       rival={"port": 9911}, fail_always=True)
    _wire(monkeypatch, tmp_path, sim, _config(gguf))
    try:
        gw_app._mutate_config(lambda r: r.update(marker="lost"),
                              "doomed")
    except RuntimeError as e:
        assert "write race" in str(e)
    else:
        raise AssertionError("expected RuntimeError after two lost races")
    assert len(sim.saves) == 2


def test_admin_config_get_reads_store(gguf, tmp_path, monkeypatch):
    """GET /admin/config renders the ACTIVE store's content — a
    non-file store serves its own config, not whatever file happens
    to sit at cfg.path."""
    store = StaticStore({"port": 9911, "default_alias": "from-store"})
    _wire(monkeypatch, tmp_path, store, _config(gguf))
    with TestClient(gw_app.app) as client:
        r = client.get("/admin/config")
        assert r.status_code == 200
        body = r.json()
        assert "from-store" in body["text"]
        assert body["path"] == "(config store)"  # honest label


def test_admin_config_put_writes_store(gguf, tmp_path, monkeypatch):
    """PUT /admin/config persists the parsed text through the ACTIVE
    store (wholesale replace) — a swapped store sees it."""
    store = StaticStore({"port": 9911, "default_alias": "initial"})
    _wire(monkeypatch, tmp_path, store, _config(gguf))
    payload = json.dumps({"port": 9911, "default_alias": "put-via-store"})
    with TestClient(gw_app.app) as client:
        r = client.put("/admin/config", json={"text": payload})
        assert r.status_code == 200, r.text
    assert store.saves == 1
    assert store.raw["default_alias"] == "put-via-store"

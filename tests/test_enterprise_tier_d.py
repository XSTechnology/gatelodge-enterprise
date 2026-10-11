"""Tier D canary tests — multi-replica shared state.

Section 1 (tier-d-2): RedisStorage protocol parity vs InMemoryStorage.
fakeredis supplies the server — the REAL storage class runs, no mocks.
Section 2 (tier-d-3): DatabaseConfigStore round-trip/CAS/seed (sqlite).
Section 3 (tier-d-4): wiring canaries — real swaps, serving behavior.
"""
import json
import sqlite3

import fakeredis
from fastapi.testclient import TestClient

import gateway.app as gw_app
import gateway.config as gw_config
import gateway.models as gw_models
from gateway.config import ConfigStore, FileConfigStore
from gateway.storage import (InMemoryStorage, StorageBackend, get_storage,
                             reset_storage, set_storage)
from gateway_enterprise.db_config import DatabaseConfigStore
from gateway_enterprise.redis_storage import RedisStorage

# shared fixtures/helpers (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import (gguf, wired, _config, _patch_cloud,  # noqa: F401
                                 _FakeCloud)
from test_enterprise_sso import (ADMIN_CLAIMS, FakeIdP, _sso_config,  # noqa: F401
                                 _sso_section, _wire_enterprise, auth)


def _redis() -> RedisStorage:
    return RedisStorage(client=fakeredis.FakeRedis())


# ── section 1: RedisStorage protocol parity ────────────────────────────────

def _exercise(backend: StorageBackend) -> dict:
    """A fixed protocol exercise → observable results. Run against BOTH
    backends; identical results = behavioral parity."""
    out: dict = {}
    backend.set("ns1", "plain", "value")
    backend.set("ns1", "dict", {"a": 1, "b": [1.5, 2.5], "c": None})
    backend.set("ns1", "coloned:key", "spend-scope-style key")
    backend.set("ns2", "other", 42)
    backend.set("ns1", "flag", True)

    out["get"] = backend.get("ns1", "plain")
    out["get_dict"] = backend.get("ns1", "dict")
    out["get_missing"] = backend.get("ns1", "nope")
    out["get_default_missing"] = backend.get_default("ns1", "nope", "fb")
    out["get_default_present"] = backend.get_default("ns1", "plain", "fb")
    out["get_bool"] = backend.get("ns1", "flag")
    out["contains_yes"] = backend.contains("ns1", "plain")
    out["contains_no"] = backend.contains("ns1", "nope")
    out["coloned_roundtrip"] = backend.get("ns1", "coloned:key")

    out["keys_ns1"] = sorted(backend.keys("ns1"))
    out["items_ns1"] = sorted(backend.items("ns1"))
    out["len_ns1"] = backend.len("ns1")
    out["len_ns2"] = backend.len("ns2")

    out["incr_new"] = backend.incr("counters", "c")
    out["incr_again"] = backend.incr("counters", "c", 2.5)
    out["incr_readback"] = backend.get("counters", "c")

    backend.set("minns", "x", {"last_used": 5.0, "v": 1})
    backend.set("minns", "y", {"last_used": 2.0, "v": 2})
    out["min_by"] = backend.min_by("minns", "last_used")
    out["min_by_missing_field"] = backend.min_by("minns", "nope")
    out["min_by_empty"] = backend.min_by("emptyns", "f")

    backend.delete("ns1", "plain")
    out["after_delete"] = backend.get("ns1", "plain")
    out["len_after_delete"] = backend.len("ns1")

    out["clear_ns2"] = backend.clear("ns2")
    out["len_ns2_after_clear"] = backend.len("ns2")
    out["len_ns1_untouched"] = backend.len("ns1")
    return out


def test_redis_protocol_parity_with_inmemory():
    """Same exercise, same results — RedisStorage is a drop-in backend
    (the protocol is behavioral, not just structural)."""
    assert _exercise(InMemoryStorage()) == _exercise(_redis())


def test_redis_clear_all_and_shared_flag():
    """clear('*') wipes everything the backend holds (a dedicated DB
    index per deployment is the isolation rule); the shared flag is
    what turns the core's local-log assumptions off."""
    b = _redis()
    b.set("a", "1", 1)
    b.set("b", "2", 2)
    assert b.clear("*") == 2
    assert b.len("a") == 0 and b.len("b") == 0
    assert RedisStorage.shared is True
    assert InMemoryStorage.shared is False
    # structural typing: the REAL class satisfies the protocol
    assert isinstance(b, StorageBackend)


def test_redis_two_clients_share_state():
    """The tier-D point: two backends over one server see each other's
    writes (two replicas, one Redis)."""
    server = fakeredis.FakeRedis()
    a = RedisStorage(client=server)
    b = RedisStorage(client=server)
    a.set("rate_windows", "vk-shared", [1000.0, 1001.5])
    assert b.get("rate_windows", "vk-shared") == [1000.0, 1001.5]
    assert b.contains("rate_windows", "vk-shared")
    assert sorted(b.keys("rate_windows")) == ["vk-shared"]


# ── section 2: DatabaseConfigStore (sqlite — the real class, real SQL) ─────

def _db(conn=None) -> DatabaseConfigStore:
    # check_same_thread=False: TestClient runs the app in a worker
    # thread; the store's usage is serialized by the single event loop
    # (production uses psycopg, which is thread-safe).
    return DatabaseConfigStore(
        conn=conn or sqlite3.connect(":memory:", check_same_thread=False))


def test_db_config_roundtrip_and_version():
    store = _db()
    assert store.load() is None
    assert store.version() is None
    assert store.save({"a": 1}) is True          # unconditional insert
    assert store.load() == {"a": 1}
    assert store.version() == 1
    assert store.save({"a": 2}) is True          # unconditional bump
    assert store.load() == {"a": 2}
    assert store.version() == 2
    assert isinstance(store, ConfigStore)        # GatewayConfig's check


def test_db_config_cas_wins_and_loses():
    """Two writers, one table: the observed version wins the CAS; a
    stale version LOSES without clobbering (the core retries)."""
    conn = sqlite3.connect(":memory:")
    a = DatabaseConfigStore(conn=conn)
    b = DatabaseConfigStore(conn=conn)           # second handle, same table
    a.save({"v": 1})
    assert a.version() == 1
    # b observed version 1 — its CAS write wins
    assert b.save({"v": 2}, expected_version=1) is True
    assert a.load() == {"v": 2} and a.version() == 2
    # a still holds the stale version 1 — its CAS write LOSES, no clobber
    assert a.save({"v": 3}, expected_version=1) is False
    assert a.load() == {"v": 2}
    # a fresh-observed CAS succeeds
    assert a.save({"v": 3}, expected_version=a.version()) is True
    assert b.load() == {"v": 3}


def test_db_config_cas_on_empty_table():
    """expected_version against a missing row loses (rowcount 0)."""
    store = _db()
    assert store.save({"x": 1}, expected_version=0) is False
    assert store.load() is None


def test_db_config_seed_from_file(tmp_path):
    """First boot on an empty table seeds from the legacy config FILE;
    an existing row is NEVER overwritten by the seed."""
    legacy = tmp_path / "gateway_config.json"
    legacy.write_text(json.dumps(
        {"port": 9911, "default_alias": "seeded"}), encoding="utf-8")
    conn = sqlite3.connect(":memory:")
    store = DatabaseConfigStore(conn=conn, seed_file=legacy)
    assert store.load()["default_alias"] == "seeded"
    assert store.version() == 1
    # mutate, then re-init with the same seed file: no reseed
    store.save({"default_alias": "changed"})
    store2 = DatabaseConfigStore(conn=conn, seed_file=legacy)
    assert store2.load()["default_alias"] == "changed"


# ── section 3: wiring canaries — real swaps, serving behavior ──────────────

CHAT_BODY = {"model": "groq:cloud-fast",
             "messages": [{"role": "user", "content": "tier-d probe"}]}


def test_chat_state_lands_in_redis(wired, gguf, monkeypatch):
    """One cacheable chat through the REAL wiring writes response-cache
    + spend-ledger entries into Redis (the anti-decorative-seam check:
    the swap changes where serving state lives)."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    server = fakeredis.FakeRedis()
    set_storage(RedisStorage(client=server))
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/v1/chat/completions", json=CHAT_BODY)
            assert r.status_code == 200, r.text
        st = RedisStorage(client=server)
        assert st.len("response_cache") >= 1
        assert st.len("spend_ledger") >= 1
    finally:
        reset_storage()


def test_shared_rate_window_two_clients(wired, gguf, monkeypatch):
    """The tier-D point: two clients (replicas) sharing one backend see
    the SAME per-key RPM window — the second client's request 429s on
    the first client's admissions."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    cfgd = _config(gguf)
    cfgd["gateway_keys"] = ["admin-secret"]
    cfgd["api_keys"] = [{"id": "limited", "key": "vk-limited",
                         "rate_limit_rpm": 1}]
    wired(cfgd)
    _patch_cloud(monkeypatch, _FakeCloud())
    set_storage(RedisStorage(client=fakeredis.FakeRedis()))
    try:
        with TestClient(gw_app.app) as c1, TestClient(gw_app.app) as c2:
            r1 = c1.post("/v1/chat/completions",
                         headers=auth("vk-limited"), json=CHAT_BODY)
            assert r1.status_code == 200, r1.text
            r2 = c2.post("/v1/chat/completions",
                         headers=auth("vk-limited"), json=CHAT_BODY)
            assert r2.status_code == 429
    finally:
        reset_storage()


def test_response_cache_shared_two_clients(wired, gguf, monkeypatch):
    """Identical request from the second client is served from the
    SHARED response cache — the cloud backend sees exactly one call."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_config(gguf))
    fake = _FakeCloud()
    _patch_cloud(monkeypatch, fake)
    set_storage(RedisStorage(client=fakeredis.FakeRedis()))
    try:
        with TestClient(gw_app.app) as c1, TestClient(gw_app.app) as c2:
            assert c1.post("/v1/chat/completions",
                           json=CHAT_BODY).status_code == 200
            assert c2.post("/v1/chat/completions",
                           json=CHAT_BODY).status_code == 200
        assert len(fake.calls) == 1
    finally:
        reset_storage()


def test_sso_introspection_cache_shared(wired, gguf, monkeypatch):
    """Tier C's introspection cache rides the shared backend: two
    clients, one IdP hit (the cache in SHARED storage serves the
    second)."""
    monkeypatch.setenv("TEST_SSO_SECRET", "secret")
    wired(_sso_config(gguf, sso=_sso_section()))
    fake = FakeIdP(ADMIN_CLAIMS)
    monkeypatch.setattr("gateway_enterprise.sso.introspect", fake)
    set_storage(RedisStorage(client=fakeredis.FakeRedis()))
    _wire_enterprise()
    try:
        with TestClient(gw_app.app) as c1, TestClient(gw_app.app) as c2:
            assert c1.get("/admin/stats",
                          headers=auth("AAA.BBB.CCC")).status_code == 200
            assert c2.get("/admin/stats",
                          headers=auth("AAA.BBB.CCC")).status_code == 200
        assert fake.calls == 1
    finally:
        from gateway.auth import AuthPipeline, set_auth_pipeline
        set_auth_pipeline(AuthPipeline())
        reset_storage()


def test_config_serves_and_mutates_via_db(gguf, tmp_path, monkeypatch):
    """The config seam swap: the singleton serves from the DATABASE —
    admin mutations land in the DB (version bumped) and GET renders
    the DB's content."""
    store = DatabaseConfigStore(
        conn=sqlite3.connect(":memory:", check_same_thread=False))
    store.save(_config(gguf))
    original = gw_config._CONFIG
    monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
    cfg = gw_config.set_config_store(store)
    engine = gw_models.EmbeddedEngine(cfg)
    monkeypatch.setattr(gw_models, "_ENGINE", engine)
    try:
        with TestClient(gw_app.app) as client:
            r = client.post("/admin/aliases", json={
                "name": "db-probe", "chain": ["local:fast"]})
            assert r.status_code == 200, r.text
            assert "db-probe" in store.load()["aliases"]  # in the DB
            assert store.version() >= 2                   # CAS bumped
            text = client.get("/admin/config").json()["text"]
            assert "db-probe" in text                     # rendered from DB
    finally:
        gw_config._CONFIG = original


def test_cross_replica_config_propagation(gguf, tmp_path, monkeypatch):
    """THE tier-D demo: a config mutation via replica A's admin API
    propagates to replica B through the shared store's version token —
    B reloads on its next request, no restart."""
    db = tmp_path / "replicas.db"
    store_a = DatabaseConfigStore(
        conn=sqlite3.connect(db, check_same_thread=False))
    store_b = DatabaseConfigStore(
        conn=sqlite3.connect(db, check_same_thread=False))
    store_a.save(_config(gguf))
    original = gw_config._CONFIG
    monkeypatch.setattr(gw_app, "_LOG_PATH", tmp_path / "gateway_log.jsonl")
    cfg_a = gw_config.set_config_store(store_a)
    cfg_b = gw_config.GatewayConfig(store_b)          # replica B's snapshot
    engine = gw_models.EmbeddedEngine(cfg_a)
    monkeypatch.setattr(gw_models, "_ENGINE", engine)
    try:
        with TestClient(gw_app.app) as client_a:
            r = client_a.post("/admin/aliases", json={
                "name": "from-replica-a", "chain": ["local:fast"]})
            assert r.status_code == 200, r.text
        assert "from-replica-a" not in cfg_b.aliases   # B is on its snapshot
        assert cfg_b.maybe_reload() is True            # version token moved
        assert "from-replica-a" in cfg_b.aliases       # propagated
        # and B SERVES it: swap the singleton to B's config
        gw_config._CONFIG = cfg_b
        with TestClient(gw_app.app) as client_b:
            listed = client_b.get("/admin/aliases").json()["aliases"]
            assert "from-replica-a" in listed
    finally:
        gw_config._CONFIG = original


def test_wire_tier_d_no_env_leaves_defaults(monkeypatch):
    """The wiring guard: without the env vars, _wire_tier_d is a no-op —
    single-process defaults, byte-for-byte."""
    import enterprise_main
    monkeypatch.delenv("GATELODGE_REDIS_URL", raising=False)
    monkeypatch.delenv("GATELODGE_CONFIG_DSN", raising=False)
    reset_storage()
    original_cfg = gw_config._CONFIG
    enterprise_main._wire_tier_d()
    assert isinstance(get_storage(), InMemoryStorage)
    assert gw_config._CONFIG is original_cfg   # no store swap happened

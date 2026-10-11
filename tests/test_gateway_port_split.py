"""Control-plane split tests (v2.1) — port configurability + the
serving/admin port scoping:

    GATELODGE_PORT / GATELODGE_BIND / GATELODGE_ADMIN_PORT /
    GATELODGE_ADMIN_BIND env overrides (boot context: env wins over
    the file and SURVIVES hot-reload — editing the file must not snap
    the ports back),

    admin_port config parsing (None = single-port, the behavior of
    every prior release),

    the port-scope shims: on the SERVING port /admin/*, /teams, /scim
    and the console 404 (network isolation, no existence leak); on the
    ADMIN port only /admin/*, the console and /healthz are hosted —
    /v1 and /mcp 404 there. /healthz is on both.
"""
import pytest
from fastapi.testclient import TestClient

import gateway.app as gw_app
import gateway.config as gw_config

from test_gateway_stage3 import gguf, wired, _config, _secured_config  # noqa: F401


def _config_with(gguf, **over):
    cfg = _secured_config(gguf)      # keys on → 401s prove auth path
    cfg.update(over)
    return cfg


# ── config: admin_port parsing + env overrides ─────────────────────────────

def test_admin_port_defaults_to_none(wired, gguf):  # noqa: F811
    """No admin_port key → None → single-port behavior (every prior
    release, byte-for-byte)."""
    wired(_config_with(gguf))
    cfg = gw_config.get_gateway_config()
    assert cfg.admin_port is None
    assert cfg.admin_bind == "127.0.0.1"


def test_admin_port_parsed(wired, gguf):  # noqa: F811
    wired(_config_with(gguf, admin_port=9011, admin_bind="10.0.0.5"))
    cfg = gw_config.get_gateway_config()
    assert cfg.admin_port == 9011
    assert cfg.admin_bind == "10.0.0.5"


def test_env_overrides_win_and_survive_reload(wired, gguf,  # noqa: F811
                                              monkeypatch, tmp_path):
    """Env = boot context: PORT/BIND/ADMIN_PORT/ADMIN_BIND win over the
    file AND are re-applied on every hot-reload (a file edit must not
    snap the ports back — the container set them for a reason)."""
    monkeypatch.setenv("GATELODGE_PORT", "8013")
    monkeypatch.setenv("GATELODGE_BIND", "0.0.0.0")
    monkeypatch.setenv("GATELODGE_ADMIN_PORT", "8014")
    monkeypatch.setenv("GATELODGE_ADMIN_BIND", "127.0.0.9")
    cfgfile, cfg, _ = wired(_config_with(gguf, port=8010, admin_port=9011))
    assert cfg.port == 8013
    assert cfg.bind == "0.0.0.0"
    assert cfg.admin_port == 8014
    assert cfg.admin_bind == "127.0.0.9"
    # hot-reload with a file change: env values are RE-APPLIED
    import json as _json
    import time as _time
    raw = _json.loads(cfgfile.read_text())
    raw["port"] = 9999                      # a file edit...
    cfgfile.write_text(_json.dumps(raw))
    _time.sleep(0.02)                       # mtime must move
    cfg.maybe_reload()
    assert cfg.port == 8013                 # ...env still wins


def test_env_admin_port_enables_split(wired, gguf, monkeypatch):  # noqa: F811
    """GATELODGE_ADMIN_PORT alone (no config key) turns the split on."""
    monkeypatch.setenv("GATELODGE_ADMIN_PORT", "8014")
    _, cfg, _ = wired(_config_with(gguf))
    assert cfg.admin_port == 8014


def test_bad_env_values_ignored(wired, gguf, monkeypatch):  # noqa: F811
    wired(_config_with(gguf, port=8010))
    monkeypatch.setenv("GATELODGE_PORT", "not-a-number")
    cfg = gw_config.get_gateway_config()
    assert cfg.port == 8010            # file value stands


# ── the port-scope shims ───────────────────────────────────────────────────

def test_serving_scope_blocks_control_plane(wired, gguf):  # noqa: F811
    """Through the SERVING shim: /v1 serves (auth applies), /healthz
    serves, but /admin/*, /teams, /scim and the console shell 404 —
    the control plane is not HOSTED on this port."""
    wired(_config_with(gguf))
    serving = gw_app.serving_scope_app()
    auth = {"Authorization": "Bearer admin-secret"}
    with TestClient(serving) as client:
        assert client.get("/healthz", headers=auth).status_code == 200
        r = client.get("/v1/models", headers=auth)
        assert r.status_code == 200
        assert client.get("/admin/stats").status_code == 404
        assert client.get("/admin/stats",
                          headers=auth).status_code == 404  # even WITH a key
        assert client.get("/teams").status_code == 404
        assert client.get("/scim/v2/Users").status_code == 404
        assert client.get("/").status_code == 404


def test_admin_scope_hosts_only_control_plane(wired, gguf):  # noqa: F811
    """Through the ADMIN shim: /admin/* + console + /healthz serve;
    the DATA plane (/v1, /mcp) 404s there."""
    wired(_config_with(gguf))
    admin = gw_app.admin_scope_app()
    auth = {"Authorization": "Bearer admin-secret"}
    with TestClient(admin) as client:
        assert client.get("/healthz", headers=auth).status_code == 200
        assert client.get("/").status_code == 200          # console
        r = client.get("/admin/stats", headers=auth)
        assert r.status_code == 200                        # works here
        assert client.get("/v1/models",
                          headers=auth).status_code == 404  # not hosted
        assert client.post("/mcp", json={"jsonrpc": "2.0", "id": 1,
                                         "method": "ping"}
                           ).status_code == 404


def test_shims_leave_subpaths_alone(wired, gguf):  # noqa: F811
    """Prefix matching is exact-boundary: /administrator (a path that
    merely STARTS with 'admin') is not blocked on the serving port."""
    wired(_config_with(gguf))
    serving = gw_app.serving_scope_app()
    with TestClient(serving) as client:
        # /administrator is not /admin/* — 404 from ROUTING (unknown
        # path), not the scope shim; the shim didn't eat it
        r = client.get("/administrator")
        assert r.status_code in (401, 404)      # either is fine — the
        # point: no scope-404 body distinction needed; assert instead
        # that a REAL admin path WITH auth would have worked had it
        # been hosted (see admin_scope test) — here we just assert the
        # shim's own matching logic directly:
        assert gw_app._path_blocked("/administrator", gw_app._ADMIN_PATHS) is False
        assert gw_app._path_blocked("/admin/stats", gw_app._ADMIN_PATHS) is True
        assert gw_app._path_blocked("/admin", gw_app._ADMIN_PATHS) is True
        assert gw_app._path_blocked("/", gw_app._ADMIN_PATHS) is True
        assert gw_app._path_blocked("/v1/models", gw_app._ADMIN_PATHS) is False


def test_healthz_reports_admin_when_split(wired, gguf):  # noqa: F811
    wired(_config_with(gguf, admin_port=9011))
    with TestClient(gw_app.app) as client:
        h = client.get("/healthz",
                       headers={"Authorization": "Bearer admin-secret"})
        assert h.json()["admin"] == {"bind": "127.0.0.1", "port": 9011}
    # and None when not split
    wired(_config_with(gguf))
    with TestClient(gw_app.app) as client:
        h2 = client.get("/healthz",
                        headers={"Authorization": "Bearer admin-secret"})
        assert h2.json()["admin"] is None

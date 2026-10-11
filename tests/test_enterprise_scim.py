"""SCIM provisioning canary tests (v1.4) — the router seam's first
consumer.

Real wiring: the router is mounted via set_extra_router (as
enterprise_main does), serving behavior asserted through TestClient.
The IdP bearer is checked against scim.token_hash (hashed at rest);
provisioning events land in the hash-chained audit trail via the core
mutation chokepoint.
"""
import json

from fastapi.testclient import TestClient

import gateway.app as gw_app
from gateway.app import set_extra_router
from gateway.storage import reset_storage

from gateway_enterprise.scim import GW_EXT, build_router

# shared fixtures/helpers (same directory; pytest inserts it in sys.path)
from test_gateway_stage3 import (gguf, wired, _config, _patch_cloud,  # noqa: F401
                                 _FakeCloud, _secured_config)

SCIM_TOKEN = "idp-scim-secret-1"
SCIM_AUTH = {"Authorization": f"Bearer {SCIM_TOKEN}"}


def _scim_config(gguf, enabled=True):
    import hashlib
    cfgd = _secured_config(gguf)   # admin-secret gateway key, no api_keys
    cfgd["scim"] = {"enabled": enabled,
                    "token_hash": hashlib.sha256(
                        SCIM_TOKEN.encode()).hexdigest()}
    return cfgd


def _mount():
    set_extra_router(build_router(), prefix="/scim")


def _umount():
    set_extra_router(None, prefix="/scim")


def _create(client, user_name="alice@corp.example",
            display_name="Alice"):
    return client.post("/scim/v2/Users", headers=SCIM_AUTH, json={
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "userName": user_name, "displayName": display_name,
        "active": True})


def test_scim_bearer_required(wired, gguf):
    """The SCIM surface owns its auth: no/wrong bearer → 401 in the
    SCIM error shape; the right IdP token passes. Gateway keys do NOT
    authenticate SCIM (they're a different credential domain)."""
    wired(_scim_config(gguf))
    _mount()
    try:
        with TestClient(gw_app.app) as client:
            r = client.get("/scim/v2/Users")
            assert r.status_code == 401
            assert r.json()["schemas"][0].endswith(":Error")
            r2 = client.get("/scim/v2/Users",
                            headers={"Authorization": "Bearer wrong"})
            assert r2.status_code == 401
            # a GATEWAY key is not a SCIM credential
            r3 = client.get("/scim/v2/Users",
                            headers={"Authorization": "Bearer admin-secret"})
            assert r3.status_code == 401
            assert client.get("/scim/v2/Users",
                              headers=SCIM_AUTH).status_code == 200
    finally:
        _umount()


def test_scim_create_and_gateway_key_serves(wired, gguf, monkeypatch):
    """Create a user → a virtual key is minted (returned ONCE in the
    gatelodge extension) → that key actually SERVES /v1 traffic."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_scim_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    _mount()
    try:
        with TestClient(gw_app.app) as client:
            r = _create(client)
            assert r.status_code == 201, r.text
            user = r.json()
            assert user["userName"] == "alice@corp.example"
            assert user["id"].startswith("scim-")
            gw_key = user[GW_EXT]["gateway_key"]
            assert gw_key.startswith("vk-")
            # the minted key is a real consumer key
            chat = client.post("/v1/chat/completions",
                               headers={"Authorization": f"Bearer {gw_key}"},
                               json={"model": "groq:cloud-fast",
                                     "messages": [{"role": "user",
                                                   "content": "scim probe"}]})
            assert chat.status_code == 200, chat.text
    finally:
        _umount()
        reset_storage()


def test_scim_duplicate_conflict(wired, gguf):
    wired(_scim_config(gguf))
    _mount()
    try:
        with TestClient(gw_app.app) as client:
            assert _create(client).status_code == 201
            r = _create(client)
            assert r.status_code == 409
            assert r.json()["scimType"] == "uniqueness"
    finally:
        _umount()


def test_scim_list_filter_get(wired, gguf):
    wired(_scim_config(gguf))
    _mount()
    try:
        with TestClient(gw_app.app) as client:
            _create(client, "alice@corp.example", "Alice")
            _create(client, "bob@corp.example", "Bob")
            # unfiltered list — ONLY SCIM-managed entries appear
            listed = client.get("/scim/v2/Users", headers=SCIM_AUTH).json()
            assert listed["totalResults"] == 2
            # userName filter (the IdP sync form)
            f = client.get("/scim/v2/Users",
                           params={"filter": 'userName eq "alice@corp.example"'},
                           headers=SCIM_AUTH).json()
            assert f["totalResults"] == 1
            assert f["Resources"][0]["displayName"] == "Alice"
            # unsupported filter → 400
            bad = client.get("/scim/v2/Users",
                             params={"filter": 'displayName eq "x"'},
                             headers=SCIM_AUTH)
            assert bad.status_code == 400
            # get by id
            kid = f["Resources"][0]["id"]
            one = client.get(f"/scim/v2/Users/{kid}", headers=SCIM_AUTH)
            assert one.status_code == 200
            assert one.json()["userName"] == "alice@corp.example"
    finally:
        _umount()


def test_scim_patch_deactivate_revokes(wired, gguf, monkeypatch):
    """PATCH active:false → the api_keys entry is REMOVED (true
    deprovisioning): the minted key 401s immediately."""
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    wired(_scim_config(gguf))
    _patch_cloud(monkeypatch, _FakeCloud())
    _mount()
    try:
        with TestClient(gw_app.app) as client:
            user = _create(client).json()
            kid = user["id"]
            gw_key = user[GW_EXT]["gateway_key"]
            # patch: path-style (RFC form)
            r = client.patch(f"/scim/v2/Users/{kid}", headers=SCIM_AUTH,
                             json={"Operations": [
                                 {"op": "replace", "path": "active",
                                  "value": False}]})
            assert r.status_code == 200, r.text
            # the key no longer authenticates
            chat = client.post("/v1/chat/completions",
                               headers={"Authorization": f"Bearer {gw_key}"},
                               json={"model": "groq:cloud-fast",
                                     "messages": [{"role": "user",
                                                   "content": "after revoke"}]})
            assert chat.status_code == 401
            # the user is gone
            assert client.get(f"/scim/v2/Users/{kid}",
                              headers=SCIM_AUTH).status_code == 404
    finally:
        _umount()
        reset_storage()


def test_scim_okta_style_patch_and_put(wired, gguf):
    """Value-object PATCH (the Okta form) and PUT both deactivate; PUT
    also updates displayName."""
    wired(_scim_config(gguf))
    _mount()
    try:
        with TestClient(gw_app.app) as client:
            u1 = _create(client, "carol@corp.example", "Carol").json()
            # Okta-style value-object patch
            r = client.patch(f"/scim/v2/Users/{u1['id']}", headers=SCIM_AUTH,
                             json={"Operations": [
                                 {"op": "replace",
                                  "value": {"active": False}}]})
            assert r.status_code == 200
            assert client.get(f"/scim/v2/Users/{u1['id']}",
                              headers=SCIM_AUTH).status_code == 404
            # PUT: displayName update on a live user
            u2 = _create(client, "dave@corp.example", "Dave").json()
            put = client.put(f"/scim/v2/Users/{u2['id']}", headers=SCIM_AUTH,
                             json={"userName": "dave@corp.example",
                                   "displayName": "David",
                                   "active": True})
            assert put.status_code == 200
            assert put.json()["displayName"] == "David"
            # PUT active:false revokes
            put2 = client.put(f"/scim/v2/Users/{u2['id']}", headers=SCIM_AUTH,
                              json={"userName": "dave@corp.example",
                                    "active": False})
            assert put2.status_code == 200
            assert client.get(f"/scim/v2/Users/{u2['id']}",
                              headers=SCIM_AUTH).status_code == 404
    finally:
        _umount()


def test_scim_delete_and_mgmt_boundary(wired, gguf):
    """DELETE revokes; the scim- id prefix is the management boundary —
    an IdP cannot touch admin-managed keys (404, not 403: not visible)."""
    wired(_scim_config(gguf, ))
    cfgd = None
    _mount()
    try:
        with TestClient(gw_app.app) as client:
            user = _create(client, "erin@corp.example").json()
            kid = user["id"]
            r = client.delete(f"/scim/v2/Users/{kid}", headers=SCIM_AUTH)
            assert r.status_code == 204
            assert client.get(f"/scim/v2/Users/{kid}",
                              headers=SCIM_AUTH).status_code == 404
            # non-SCIM ids are invisible to the surface
            r2 = client.delete("/scim/v2/Users/program-x",
                               headers=SCIM_AUTH)
            assert r2.status_code == 404
            r3 = client.get("/scim/v2/Users/admin",
                            headers=SCIM_AUTH)
            assert r3.status_code == 404
    finally:
        _umount()


def test_scim_events_audited(wired, gguf):
    """Provisioning events land in the hash-chained audit trail (the
    SOC2 CC6.2 evidence) via the core mutation chokepoint."""
    wired(_scim_config(gguf))
    _mount()
    try:
        with TestClient(gw_app.app) as client:
            _create(client, "frank@corp.example")
        actions = [json.loads(l)["action"] for l in
                   (gw_app._LOG_PATH.parent / "config-audit.jsonl")
                   .read_text(encoding="utf-8").strip().splitlines()]
        assert any(a.startswith("scim-user-create frank@corp.example")
                   for a in actions)
        assert gw_app.verify_audit_chain()["valid"] is True
    finally:
        _umount()


def test_scim_disabled_not_mounted(wired, gguf):
    """scim.enabled false → _wire_scim is a no-op (the unmounted
    surface 401s through the CORE pipeline, not the SCIM shape)."""
    wired(_scim_config(gguf, enabled=False))
    import enterprise_main
    enterprise_main._wire_scim()     # no-op
    with TestClient(gw_app.app) as client:
        r = client.get("/scim/v2/Users")
        # nothing mounted → the core pipeline gates it (keys configured)
        assert r.status_code == 401
        assert "schemas" not in r.json()

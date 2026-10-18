"""Teams/workspaces canary tests (v2.0 Gatehouse) — the multi-tenant
layer, proven live:

    key→team attribution (log line carries rec["team"]; team scope
    charged alongside the key),
    team budgets (429 at cap — USD and token shapes),
    the /teams surface: gateway keys see everything, TEAM-ADMIN keys
    see only their team and fail-closed on /admin/*,
    team-scoped key minting (pinned to the team, hash-at-rest, audit
    record, plaintext returns once), revocation, team CRUD,
    the license gate, and SCIM+MCP+teams triple-mount coexistence.
"""
import hashlib
import json
import time

import pytest
from fastapi.testclient import TestClient

import enterprise_main
import gateway.app as gw_app
from gateway.auth import set_auth_pipeline, AuthPipeline
from gateway_enterprise import teams as teams_mod
from gateway_enterprise.teams import (team_budget_link, team_link,
                                      build_router)

from test_gateway_stage3 import (gguf, wired, _config, _patch_cloud,  # noqa: F401
                                 _FakeCloud, _secured_config)

ADMIN = "admin-secret"                       # gateway key (full)
TEAM_ADMIN = "eng-lead-key"                  # team-scoped admin
TA_HASH = hashlib.sha256(TEAM_ADMIN.encode()).hexdigest()
OTHER_TEAM_ADMIN = "sales-lead-key"
OTA_HASH = hashlib.sha256(OTHER_TEAM_ADMIN.encode()).hexdigest()


@pytest.fixture(autouse=True)
def _reset():
    teams_mod.reset_teams_state()
    yield
    teams_mod.reset_teams_state()
    set_auth_pipeline(AuthPipeline())        # core default
    gw_app.set_extra_router(None, prefix="/cleanup")
    from gateway.storage import reset_storage
    reset_storage()


def _teams_config(gguf, **team_over):
    eng = {"name": "Engineering", "keys": ["program-x"],
           "admins": [TA_HASH], **team_over}
    cfg = _secured_config(gguf, api_keys=[
        {"id": "program-x", "key": "vk-team"},
        {"id": "lone-key", "key": "vk-lone"}])
    cfg["teams"] = {"enabled": True,
                    "definitions": {"eng": eng,
                                    "sales": {"name": "Sales",
                                              "keys": ["crm-bot"],
                                              "admins": [OTA_HASH]}}}
    cfg["api_keys"].append({"id": "crm-bot", "key": "vk-crm"})
    return cfg


def _wire(wired, gguf, **team_over):
    wired(_teams_config(gguf, **team_over))
    # the ENTERPRISE pipeline as enterprise_main wires it (teams on)
    from gateway.auth import (admin_gate_link, rate_limit_link,
                              resolve_link, rbac_link)
    set_auth_pipeline(AuthPipeline(
        [resolve_link, rbac_link, team_link, team_budget_link,
         admin_gate_link, rate_limit_link]))
    gw_app.add_extra_router(build_router(), prefix="/teams", auth="owned")


def _chat(client, token="vk-team", content="team probe"):
    return client.post("/v1/chat/completions", headers={
        "Authorization": f"Bearer {token}"},
        json={"model": "groq:cloud-fast",
              "messages": [{"role": "user", "content": content}]})


def _month_log():
    return gw_app._month_log_path(gw_app._month_key())


def _seed_team_spend(team: str, tokens: int, usd: float) -> None:
    _month_log().parent.mkdir(parents=True, exist_ok=True)
    with open(_month_log(), "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "model": "m", "backend": "b", "stream": False, "ms": 1,
            "usage": {"total_tokens": tokens}, "status": 200,
            "cost_usd": usd, "key": "program-x", "team": team}) + "\n")


# ── attribution + budgets (serving path) ───────────────────────────────────

def test_attribution_and_charge(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    _wire(wired, gguf)
    _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        r = _chat(client)
        assert r.status_code == 200, r.text
    lines = [json.loads(l) for l in
             _month_log().read_text(encoding="utf-8").splitlines()]
    chat = [l for l in lines if l.get("model") == "groq:cloud-fast"][-1]
    assert chat["team"] == "eng"                     # attributed
    spend = gw_app._scope_spend("team:eng")
    assert spend["tokens"] == chat["usage"]["total_tokens"]
    # a key NOT in any team: no attribution, no team scope
    with TestClient(gw_app.app) as client:
        r = _chat(client, token="vk-lone")
        assert r.status_code == 200
    lines = [json.loads(l) for l in
             _month_log().read_text(encoding="utf-8").splitlines()]
    last = [l for l in lines if l.get("key") == "lone-key"][-1]
    assert "team" not in last


def test_team_budget_usd_gate(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    _wire(wired, gguf, budget_usd_month=1.0)
    _patch_cloud(monkeypatch, _FakeCloud())
    _seed_team_spend("eng", 10, usd=1.5)             # already over
    with TestClient(gw_app.app) as client:
        r = _chat(client)
        assert r.status_code == 429
        assert r.json()["error"]["code"] == "team_budget_exceeded"
        # the OTHER team's key is unaffected (sales has no budget)
        r2 = _chat(client, token="vk-crm")
        assert r2.status_code == 200


def test_team_budget_tokens_gate(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    _wire(wired, gguf, budget_tokens_month=100)
    _patch_cloud(monkeypatch, _FakeCloud())
    _seed_team_spend("eng", 200, usd=0.0)
    with TestClient(gw_app.app) as client:
        r = _chat(client)
        assert r.status_code == 429
        assert "token budget" in r.json()["error"]["message"]


def test_no_budgets_no_gate(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    _wire(wired, gguf)                               # no budgets set
    _patch_cloud(monkeypatch, _FakeCloud())
    _seed_team_spend("eng", 99999, usd=999.0)
    with TestClient(gw_app.app) as client:
        assert _chat(client).status_code == 200


# ── the /teams surface ─────────────────────────────────────────────────────

def _admin_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def test_surface_auth_scoping(wired, gguf):  # noqa: F811
    _wire(wired, gguf)
    with TestClient(gw_app.app) as client:
        # full gateway key: ALL teams
        r = client.get("/teams", headers=_admin_headers(ADMIN))
        assert r.status_code == 200
        assert r.json()["scope"] == "full"
        assert {t["id"] for t in r.json()["teams"]} == {"eng", "sales"}
        # team admin: ONLY their team
        r2 = client.get("/teams", headers=_admin_headers(TEAM_ADMIN))
        assert r2.status_code == 200
        assert r2.json()["scope"] == "team"
        assert [t["id"] for t in r2.json()["teams"]] == ["eng"]
        # cross-team detail: 403
        r3 = client.get("/teams/sales", headers=_admin_headers(TEAM_ADMIN))
        assert r3.status_code == 403
        # the FAIL-CLOSED posture: a team-admin key is NOT a gateway
        # key — the core pipeline 401s it on /admin/*
        r4 = client.get("/admin/stats", headers=_admin_headers(TEAM_ADMIN))
        assert r4.status_code == 401
        # and on /v1 it is not a consumer key either
        r5 = client.get("/v1/models", headers=_admin_headers(TEAM_ADMIN))
        assert r5.status_code == 401
        # anonymous: 401
        assert client.get("/teams").status_code == 401


def test_rollups(wired, gguf):  # noqa: F811
    _wire(wired, gguf, budget_usd_month=10.0)
    _seed_team_spend("eng", 100, usd=2.5)
    with TestClient(gw_app.app) as client:
        r = client.get("/teams/eng", headers=_admin_headers(ADMIN))
        assert r.status_code == 200
        d = r.json()
        assert d["month_spend"] == {"tokens": 100, "usd": 2.5}
        assert d["budgets"]["usd_month"] == 10.0
        assert d["over_budget"] is False
        assert d["admin_keys"] == 1
        assert [m["id"] for m in d["members"]] == ["program-x"]
        assert d["members"][0]["tokens"] == 100   # per-member breakdown


def test_team_admin_key_lifecycle(wired, gguf, monkeypatch):  # noqa: F811
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    _wire(wired, gguf)
    _patch_cloud(monkeypatch, _FakeCloud())
    with TestClient(gw_app.app) as client:
        # team admin mints a key INTO their team — plaintext ONCE
        r = client.post("/teams/eng/keys", headers=_admin_headers(TEAM_ADMIN),
                        json={"id": "ci-runner", "owner": "eng"})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["key"].startswith("vk-")
        minted = body["key"]
        # membership + key entry landed in ONE mutation
        cfg = gw_app.get_gateway_config()
        cfg.reload_now()
        assert "ci-runner" in cfg.api_keys
        assert "ci-runner" in teams_mod._STATE.definitions()["eng"]["keys"]
        assert cfg.api_keys["ci-runner"]["key_hash"] == \
            hashlib.sha256(minted.encode()).hexdigest()
        # the minted key WORKS on /v1 and is team-attributed
        r2 = client.post("/v1/chat/completions", headers={
            "Authorization": f"Bearer {minted}"},
            json={"model": "groq:cloud-fast",
                  "messages": [{"role": "user", "content": "hi"}]})
        assert r2.status_code == 200, r2.text
        lines = [json.loads(l) for l in
                 _month_log().read_text(encoding="utf-8").splitlines()]
        assert [l for l in lines if l.get("key") == "ci-runner"][-1][
            "team"] == "eng"
        # sales admin CANNOT mint into eng
        r3 = client.post("/teams/eng/keys",
                         headers=_admin_headers(OTHER_TEAM_ADMIN),
                         json={"id": "nope"})
        assert r3.status_code == 403
        # revoke removes key AND membership atomically
        r4 = client.post("/teams/eng/keys",
                         headers=_admin_headers(TEAM_ADMIN),
                         json={"id": "ci-runner", "delete": True})
        assert r4.status_code == 200
        cfg = gw_app.get_gateway_config()
        cfg.reload_now()
        assert "ci-runner" not in cfg.api_keys
        assert "ci-runner" not in \
            teams_mod._STATE.definitions()["eng"]["keys"]


def test_team_crud_full_admin_only(wired, gguf):  # noqa: F811
    _wire(wired, gguf)
    with TestClient(gw_app.app) as client:
        # team admin cannot create teams
        r = client.post("/teams/newteam",
                        headers=_admin_headers(TEAM_ADMIN),
                        json={"name": "New", "budget_usd_month": 5})
        assert r.status_code == 403
        # full admin creates one (membership list starts empty)
        r2 = client.post("/teams/newteam", headers=_admin_headers(ADMIN),
                         json={"name": "New Team",
                               "budget_usd_month": 5.0,
                               "admins": [TA_HASH]})
        assert r2.status_code == 200, r2.text
        assert "newteam" in teams_mod._STATE.definitions()
        # update preserves membership
        client.post("/teams/newteam/keys", headers=_admin_headers(ADMIN),
                    json={"id": "mk-1", "owner": "x"})
        client.post("/teams/newteam", headers=_admin_headers(ADMIN),
                    json={"name": "Newer Team", "budget_usd_month": 9.0})
        assert teams_mod._STATE.definitions()["newteam"]["keys"] == ["mk-1"]
        # delete (full admin)
        r3 = client.post("/teams/newteam", headers=_admin_headers(ADMIN),
                         json={"delete": True})
        assert r3.status_code == 200
        assert "newteam" not in teams_mod._STATE.definitions()


def test_audit_records_for_mutations(wired, gguf):  # noqa: F811
    _wire(wired, gguf)
    with TestClient(gw_app.app) as client:
        client.post("/teams/eng/keys", headers=_admin_headers(ADMIN),
                    json={"id": "audited-key", "owner": "a"})
    p = gw_app._LOG_PATH.parent / "config-audit.jsonl"
    recs = [json.loads(l) for l in
            p.read_text(encoding="utf-8").strip().splitlines() if l.strip()]
    actions = [r.get("action") for r in recs]
    assert "team-key-create eng/audited-key" in actions


def test_license_gate(wired, gguf, monkeypatch):  # noqa: F811
    import gateway.app as gwa
    wired(_teams_config(gguf))
    mounted = []
    monkeypatch.setattr(gwa, "add_extra_router",
                        lambda r, prefix, auth="": mounted.append(prefix))
    enterprise_main._wire_teams(enabled=False)
    assert mounted == []
    enterprise_main._wire_teams(enabled=True)
    assert mounted == ["/teams"]
    # section absent → no mount even when licensed
    wired(_config(gguf))
    enterprise_main._wire_teams(enabled=True)
    assert mounted == ["/teams"]


def test_triple_mount_coexistence(wired, gguf, monkeypatch):  # noqa: F811
    cfg = _teams_config(gguf)
    cfg["scim"] = {"enabled": True,
                   "token_hash": hashlib.sha256(b"scim-token").hexdigest()}
    cfg["mcp"] = {"enabled": True, "keys": [TA_HASH],
                  "servers": {"stub": {"url": "http://127.0.0.1:9/mcp"}}}
    wired(cfg)
    enterprise_main._wire_scim(enabled=True)
    enterprise_main._wire_mcp(enabled=True)
    enterprise_main._wire_teams(enabled=True)
    try:
        with TestClient(gw_app.app) as client:
            assert client.get("/teams", headers=_admin_headers(
                ADMIN)).status_code == 200
            assert client.get("/scim/v2/Users", headers={
                "Authorization": "Bearer scim-token"}).status_code == 200
            r = client.post("/mcp", headers=_admin_headers(TEAM_ADMIN),
                            json={"jsonrpc": "2.0", "id": 1,
                                  "method": "ping"})
            assert r.status_code == 200
    finally:
        gw_app.set_extra_router(None, prefix="/teams")


def test_team_create_cap_enforced(wired, gguf):  # noqa: F811
    """v2.2 scale caps: creating a NEW team beyond the licensed count
    refuses (403 license_caps); UPDATING an existing team at the cap
    still passes (the cap counts teams, not edits)."""
    from gateway import licensing
    _wire(wired, gguf)                    # config already has team "eng"
    licensing.set_license_state({
        "mode": "licensed", "edition": "enterprise",
        "license": {"caps": {"teams": 1}}, "source": "file",
        "detail": "", "trial": None})
    try:
        with TestClient(gw_app.app) as client:
            # at the cap: a NEW team refuses (config already has eng
            # + sales — hr is genuinely new)
            r = client.post("/teams/hr", headers=_admin_headers(ADMIN),
                            json={"name": "HR", "budget_usd_month": 1})
            assert r.status_code == 403, r.text
            assert r.json()["error"]["code"] == "license_caps"
            # updating an EXISTING team at the cap passes
            r2 = client.post("/teams/eng", headers=_admin_headers(ADMIN),
                             json={"name": "Engineering",
                                   "budget_usd_month": 2})
            assert r2.status_code == 200, r2.text
    finally:
        licensing.reset_licensing()

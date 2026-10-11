"""Teams core-seam tests (v2.0 teams/workspaces) — the CORE side only:

    team:<id> spend scopes derive from request-log lines carrying a
    "team" field (the same ledger machinery as key/session/backend
    scopes — multi-replica for free via the storage seam),
    _charge_request charges the team scope when the ENTERPRISE team
    link has attributed the request (here: the ContextVar set directly
    — upstream has no link), and _log_request stamps rec["team"].

The team link, budget gate, /teams surface and admin scoping are the
enterprise module (fork: tests/test_enterprise_teams.py).
"""
import json
import time

import gateway.app as gw_app
from gateway.auth import _REQUEST_KEY_ID, _REQUEST_TEAM
from gateway.storage import reset_storage

from test_gateway_stage3 import (gguf, wired, _config,  # noqa: F401
                                 _secured_config)


def _team_config(gguf):
    return _secured_config(gguf, api_keys=[
        {"id": "program-x", "key": "vk-team"}])


def _month_log():
    path = gw_app._month_log_path(gw_app._month_key())
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _seed_team_line(team: str, tokens: int, usd: float = 0.0,
                    key: str = "program-x") -> None:
    _month_log().parent.mkdir(parents=True, exist_ok=True)
    with open(_month_log(), "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "model": "m", "backend": "b", "stream": False, "ms": 1,
            "usage": {"total_tokens": tokens}, "status": 200,
            "cost_usd": usd, "key": key, "team": team}) + "\n")


def test_team_scope_derives_from_log(wired, gguf):  # noqa: F811
    wired(_team_config(gguf))
    try:
        _seed_team_line("eng", 100, usd=1.5)
        _seed_team_line("eng", 50, usd=0.5)
        _seed_team_line("sales", 999, usd=99.0)   # other team
        _seed_team_line("eng", 7, usd=0.0, key="other")  # team filter wins
        spend = gw_app._scope_spend("team:eng")
        assert spend["tokens"] == 157             # 100+50+7 (team-tagged)
        assert abs(spend["usd"] - 2.0) < 1e-9
        assert gw_app._scope_spend("team:sales")["tokens"] == 999
    finally:
        reset_storage()


def test_charge_request_charges_team_scope(wired, gguf):  # noqa: F811
    cfgfile, cfg, _ = wired(_team_config(gguf))
    try:
        tok = _REQUEST_TEAM.set("eng")
        _REQUEST_KEY_ID.set("program-x")
        try:
            gw_app._charge_request(
                cfg, "groq", {"total_tokens": 40,
                              "prompt_tokens": 30,
                              "completion_tokens": 10}, 0.25)
        finally:
            _REQUEST_TEAM.reset(tok)
        spend = gw_app._scope_spend("team:eng")
        assert spend["tokens"] == 40
        assert abs(spend["usd"] - 0.25) < 1e-9
        # the key scope charged alongside (unchanged behavior)
        assert gw_app._scope_spend("key:program-x")["tokens"] == 40
    finally:
        reset_storage()


def test_charge_without_team_context_charges_nothing(wired, gguf):  # noqa: F811
    _, cfg, _ = wired(_team_config(gguf))
    try:
        gw_app._charge_request(cfg, "groq", {"total_tokens": 10}, None)
        assert gw_app._scope_spend("team:anything")["tokens"] == 0
    finally:
        reset_storage()


def test_log_request_stamps_team(wired, gguf):  # noqa: F811
    wired(_team_config(gguf))
    tok = _REQUEST_TEAM.set("eng")
    try:
        gw_app._log_request("m", "b", False, 5,
                            {"total_tokens": 3}, 200, cost_usd=0.01)
    finally:
        _REQUEST_TEAM.reset(tok)
    path = _month_log()
    lines = [json.loads(l) for l in
             path.read_text(encoding="utf-8").strip().splitlines()]
    assert lines and lines[-1].get("team") == "eng"
    # and without the context var: no team field
    gw_app._log_request("m", "b", False, 5, {"total_tokens": 1}, 200)
    lines = [json.loads(l) for l in
             path.read_text(encoding="utf-8").strip().splitlines()]
    assert "team" not in lines[-1]

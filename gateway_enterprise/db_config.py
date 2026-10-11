"""DatabaseConfigStore — the multi-replica config store (tier D).

Implements the core's ConfigStore contract (gateway/config.py) against
a SQL database, so every replica serves from ONE config and admin
mutations propagate via the version token (maybe_reload polls it per
request):

    from gateway import config as gw_config
    from gateway_enterprise.db_config import DatabaseConfigStore
    gw_config.set_config_store(DatabaseConfigStore(
        dsn="postgresql://user:pw@host:5432/gatelodge",
        seed_file="data/gateway_config.json"))

Storage: one row (id=1) — raw TEXT (JSON), version BIGINT (monotonic;
the change-detection token), updated_at TEXT. TEXT-JSON over JSONB
deliberately: identical schema on Postgres and sqlite, and a
single-row config gains nothing from JSONB indexing.

Concurrency: save() is optimistic-CAS — UPDATE ... WHERE version =
expected → rowcount 1 wins, 0 loses (the core's _mutate_config
re-reads and retries once on a loss). save() WITHOUT expected_version
is an unconditional upsert (wholesale replace, e.g. PUT /admin/config).

Drivers: psycopg (v3) via dsn=; any qmark DB-API connection via conn=
(sqlite3 in tests — the REAL class, REAL SQL, no mocks). The
connection is held for the process lifetime; psycopg connections are
thread-safe, sqlite connections are single-threaded (tests).
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from gateway.config import ConfigStore

logger = logging.getLogger(__name__)

_DDL = ("CREATE TABLE IF NOT EXISTS gw_config ("
        "id INTEGER PRIMARY KEY, "
        "raw TEXT NOT NULL, "
        "version BIGINT NOT NULL, "
        "updated_at TEXT NOT NULL)")


class DatabaseConfigStore(ConfigStore):
    def __init__(self, dsn: str | None = None, *, conn: Any = None,
                 seed_file: str | Path | None = None):
        if conn is not None:
            self._conn = conn
            self._qmark = True     # sqlite3-style ? placeholders
            self._owns = False
        elif dsn:
            import psycopg  # lazy: fork-only dependency
            self._conn = psycopg.connect(dsn)
            self._qmark = False    # psycopg %-style placeholders
            self._owns = True
        else:
            raise ValueError("DatabaseConfigStore needs dsn= or conn=")
        self._exec(_DDL)
        self._commit()
        if seed_file is not None and self.load() is None:
            self._seed_from_file(Path(seed_file))

    # ── plumbing ───────────────────────────────────────────────────────

    def _exec(self, sql: str, *params: Any) -> Any:
        if not self._qmark:
            sql = sql.replace("?", "%s")
        return self._conn.execute(sql, params)

    def _commit(self) -> None:
        try:
            self._conn.commit()
        except Exception:  # sqlite autocommit conns: nothing to commit
            pass

    def _rollback(self) -> None:
        try:
            self._conn.rollback()
        except Exception:
            pass

    def _seed_from_file(self, path: Path) -> None:
        """One-time migration: an empty table seeds from the legacy
        config FILE (the operator's pre-tier-D data/gateway_config.json)
        so switching to a shared store keeps the deployment's config.
        Best effort — a missing/unreadable file leaves the store empty
        (the core then serves defaults)."""
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.warning("db config store: seed file %s unreadable — "
                           "starting empty", path)
            return
        if not isinstance(raw, dict):
            logger.warning("db config store: seed file %s is not a JSON "
                           "object — starting empty", path)
            return
        self.save(raw)  # unconditional first write (version 1)
        logger.info("db config store: seeded from %s (version 1)", path)

    def close(self) -> None:
        if self._owns:
            try:
                self._conn.close()
            except Exception:
                pass

    # ── ConfigStore contract ───────────────────────────────────────────

    def load(self) -> dict[str, Any] | None:
        row = self._exec(
            "SELECT raw FROM gw_config WHERE id = 1").fetchone()
        if row is None:
            return None
        try:
            raw = json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            logger.error("db config store: stored config is not valid "
                         "JSON — previous values kept upstream")
            return None
        return raw if isinstance(raw, dict) else None

    def version(self) -> Any:
        row = self._exec(
            "SELECT version FROM gw_config WHERE id = 1").fetchone()
        return int(row[0]) if row is not None else None

    def save(self, raw: dict[str, Any],
             *, expected_version: Any = None) -> bool:
        payload = json.dumps(raw)
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        try:
            if expected_version is None:
                # unconditional upsert: bump-update, else insert (a
                # raced insert falls back to the bump-update)
                cur = self._exec(
                    "UPDATE gw_config SET raw = ?, version = version + 1, "
                    "updated_at = ? WHERE id = 1", payload, now)
                if not (getattr(cur, "rowcount", 0) or 0):
                    try:
                        self._exec(
                            "INSERT INTO gw_config (id, raw, version, "
                            "updated_at) VALUES (1, ?, 1, ?)", payload, now)
                    except Exception:
                        self._rollback()
                        cur = self._exec(
                            "UPDATE gw_config SET raw = ?, "
                            "version = version + 1, updated_at = ? "
                            "WHERE id = 1", payload, now)
                        if not (getattr(cur, "rowcount", 0) or 0):
                            self._rollback()
                            return False
                self._commit()
                return True
            # optimistic CAS: exactly the version the caller observed
            cur = self._exec(
                "UPDATE gw_config SET raw = ?, version = version + 1, "
                "updated_at = ? WHERE id = 1 AND version = ?",
                payload, now, int(expected_version))
            won = bool(getattr(cur, "rowcount", 0) or 0)
            if won:
                self._commit()
            else:
                self._rollback()
            return won
        except Exception:
            self._rollback()
            raise

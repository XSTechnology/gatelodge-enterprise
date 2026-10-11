# SOC2 Type 1 — Scoping Baseline (started 2026-10-10, v1.3.0)

SOC2 Type 1 = a point-in-time audit of control design. The clock is
quarters-long; this is the working inventory of what the CODEBASE
already implements, mapped to the Trust Services Criteria (Security,
CC series), plus the gaps v1.4 closes. Owner decisions still open at
the bottom.

## System description (the audit boundary)

- **Gatelodge standard v1.3.0** — self-hosted LLM gateway (the
  auditable product): OpenAI-compatible serving across cloud,
  llama-server, and RunPod backends; governance in core (RBAC,
  SSO/OIDC + JWKS, audit trails); single-process default.
- **Gatelodge Enterprise v1.3.0** — the commercial tier: multi-replica
  shared state (Redis + Postgres) and the deployable stack
  (deploy/QUICKSTART.md).
- Out of scope: customer-managed IdPs, the LLM providers themselves,
  the operator's Redis/Postgres hosting.

## Control inventory (implemented, with evidence anchors)

| CC criterion | Control | Implementation | Evidence |
|---|---|---|---|
| CC6.1 (logical access) | Bearer authentication on every non-console route; 401 on unknown tokens | resolve_link + core resolver; tests/test_gateway_sso.py | upstream 755432f |
| CC6.1 | Admin/consumer key separation; keys hashed at rest (sha256), plaintext shown once | gateway_keys vs api_keys; _migrate_plaintext_keys | core since v1.x |
| CC6.1 | RBAC on the admin surface (3 roles, fail-closed on unknown roles) | gateway/rbac.py; tests/test_gateway_rbac.py | upstream 755432f |
| CC6.1 | SSO/OIDC: RFC 7662 introspection + JWKS local validation; HS/none rejected; IdP-down = fail closed | gateway/sso.py; tests/test_gateway_sso.py | upstream b7a49e6 |
| CC6.2 (access provisioning) | Virtual-key CRUD with per-key budgets, model allowlists, RPM/TPM limits | /admin/keys + rate_limit_link | upstream ec2e944 |
| CC6.2 | **GAP → v1.4**: SCIM provisioning/deprovisioning | set_extra_router seam planned | — |
| CC6.3 (least privilege) | ops_readonly/key_manager roles; whoami role awareness; console hides unauthorized actions | gateway/auth.py, console.html | upstream 62bf955 |
| CC7.1 (vulnerability/incident detection) | Audit trail of every config mutation + RBAC denial (who/when/what) | _audit_config, config-audit.jsonl + SIEM webhook | core |
| CC7.1 | **GAP → v1.4**: hash-chained (tamper-evident) audit | planned | — |
| CC7.2 (anomaly monitoring) | Scheduled synthetic verification w/ alerting; Prometheus metrics + 5 alert rules; Grafana dashboard | verify_state; deploy/prometheus/alerts.yml | fork 1ddec25 |
| CC8.1 (change management) | Optimistic-CAS config writes (no silent cross-replica clobber); store-versioned hot reload | ConfigStore.save(expected_version) | upstream c2a0b5f |
| CC8.1 | Reproducible builds: uv-locked Dockerfile, win32-marker wheel source | deploy/Dockerfile | fork 1ddec25 |
| CC9.x (risk mitigation) | Data residency: local_only aliases enforced in code; self-hosted by design | chain resolution local_only | core |
| CC6.6 (boundary) | Non-root container (uid 10001), healthchecks, dedicated Redis DB index per deployment | deploy/Dockerfile, docker-compose.yml | fork 1ddec25 |

## Gaps (v1.4 closes; ALL LANDED — evidence period starts at v1.4.0)

1. ✅ SCIM provisioning (CC6.2) — set_extra_router seam (e1aff98) +
   SCIM 2.0 (fork 8e459f6), live-verified.
2. ✅ Hash-chained audit (CC7.1) — upstream 5dcd44d, live tamper
   check passed.
3. ✅ Session budgets (CC6.x) — charge hook (57f429e) + enterprise
   gate (fork 2a56de1), live-verified cross-process.
4. ✅ Cloud credential chains (CC6.1) — auth: sigv4 / auth: adc
   (upstream 9e1797c), pure-function pinned.

## Owner decisions (blocking engagement, not engineering)

- **Auditor + window**: boutique vs Big-4; Type 1 first, Type 2 after
  3-6 months of evidence collection. Recommend scoping calls THIS
  quarter (the evidence period starts when monitoring/audit controls
  are LIVE, not when tagged).
- **License posture** (ELv2-style source-available recommended) —
  affects the system description and IP boundary in the audit.
- **Public pricing** — not an audit item, but pairs with the launch
  window.

## Next steps

1. Owner: shortlist auditors (e.g. one boutique quote), pick window.
2. v1.4 lands the four gap controls → evidence collection starts.
3. Freeze the system description at v1.4.0 for the Type 1 read.

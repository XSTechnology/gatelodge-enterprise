# Gatelodge Enterprise — 10-Minute Quickstart

A self-hosted, multi-replica LLM gateway with governance (RBAC +
SSO/OIDC) in the core: one Postgres config store, one Redis for
shared serving state, N replicas behind your load balancer.

## Prereqs

- Docker + Docker Compose v2
- (optional) `openssl` or Python for hashing your admin key

## 1. Set an admin key (1 min)

The gateway seeds its config from `deploy/data/gateway_config.json`
into Postgres on FIRST boot only — set your admin key before that:

    python -c "import hashlib; print(hashlib.sha256(b'my-admin-secret').hexdigest())"

Put the hash in `deploy/data/gateway_config.json` under `gateway_keys`
(replace `CHANGE_ME_sha256_of_your_admin_key`). Plaintext keys also
work; sha256-at-rest is the recommended posture.

Role-scoped admin keys (optional, same file):

    "admin_key_roles": [
      {"key": "<sha256 of an ops key>", "role": "ops_readonly"}
    ]

Roles: `full_admin` (default), `ops_readonly` (admin GETs except
raw config), `key_manager` (virtual-key CRUD only).

## 2. Start the stack (2 min)

    cd deploy
    docker compose up -d --build

Gateway on http://localhost:8011 (console at `/`), Postgres and Redis
internal to the compose network. With observability:

    docker compose --profile obs up -d --build

→ Grafana on http://localhost:3000 (anonymous admin; the Gatelodge
dashboard is pre-provisioned), Prometheus on http://localhost:9090.

## 3. Smoke it (1 min)

    python smoke_test.py http://127.0.0.1:8011 --key my-admin-secret

All checks green = the gateway, governance, and shared stores are
healthy. The console (open `/`, paste the key) shows a role badge and
hides actions your role cannot perform.

## 4. Configure backends (2 min)

Edit the config in the console (Config pane) or `PUT /admin/config` —
backends are hot-reloaded. Example OpenAI-compatible cloud backend:

    "backends": {
      "groq": {"type": "openai",
               "base_url": "https://api.groq.com/openai/v1",
               "api_key_env": "GROQ_API_KEY",
               "models": {"fast": "llama-3.1-8b-instant"}}
    }

Provider API keys ride environment variables — add them to the
`gateway` service in `docker-compose.yml`. Local GPU inference: run
llama-server (or the standard Gatelodge distribution) on your GPU
box and add it as a remote `llama-server` backend.

## 5. Create consumer keys (2 min)

Console → API Keys pane (or `POST /admin/keys`): per-key budgets
(tokens/USD per month), model allowlists, RPM and TPM limits. The
plaintext key is shown exactly once.

## 6. Point a client at it (1 min)

    from openai import OpenAI
    client = OpenAI(base_url="http://localhost:8011/v1",
                    api_key="vk-...")   # a consumer key
    print(client.chat.completions.create(
        model="groq:fast",
        messages=[{"role": "user", "content": "hello"}]))

## Scaling out

Replicas share everything (config via Postgres version tokens, rate
windows/caches/SSO sessions via Redis). Remove the gateway's `ports:`
mapping, put replicas behind your load balancer, and scale:

    docker compose up -d --scale gateway=3

Prometheus metrics are per-replica by design — add one scrape target
per replica (see `prometheus/prometheus.yml`).

## SSO/OIDC (optional)

Add an `sso` section to the config: RFC 7662 introspection and/or
JWKS local validation, with role rules from claims:

    "sso": {
      "enabled": true,
      "introspection_url": "https://idp.example/oauth2/introspect",
      "jwks_url": "https://idp.example/.well-known/jwks.json",
      "client_id": "gatelodge",
      "client_secret_env": "GATELODGE_SSO_CLIENT_SECRET",
      "default_role": "ops_readonly",
      "role_rules": [
        {"claim": "groups", "value": "llm-admins", "role": "full_admin"}
      ]
    }

The client secret rides the environment (same convention as provider
keys). Governance is core: RBAC and SSO ship in every distribution —
multi-replica (this stack) is the enterprise tier.

## Troubleshooting

- **401 on everything** — the admin key hash in the seed config
  doesn't match; note the seed runs only on the EMPTY database
  (`docker volume rm deploy_pgdata` to re-seed, or edit via psql).
- **Errno 10048 in a container** — a stale replica still holds the
  port; `docker compose down` then up.
- **Config edits don't stick** — check the console Config pane's
  path label: `(config store)` means Postgres is live; the JSON file
  is only the first-boot seed.

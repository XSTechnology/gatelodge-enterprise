# Troubleshooting — common issues and fixes

title: Troubleshooting — trial, license, keys, backends, budgets, logs
description: Fixes for the most common Gatelodge issues: trial expiry, license not loading, backend errors, budgets, finding logs

## Trial and license

- **"Trial expired" on cloud routes** — the 30-day Community trial
  ended. Buy a key ($99, site → Buy) and install `license.json` in the
  data folder, or continue using local (embedded) models, which are
  never time-limited.
- **License not loading** — check that the file is named exactly
  `license.json`, sits in the **data directory beside the exe**, and is
  the full block from your purchase email (starts with `{`). Restart
  Gatelodge; the License tile should show the serial. If it shows an
  error, the message explains what failed.
- **"No license — community trial" on an Enterprise binary** —
  Enterprise requires a license to boot; contact sales.

## Backends and models

- **401 from a provider** — the provider credential is missing or
  wrong. Check the Access tile (console-managed secrets) or the
  `api_key_env`/`api_key_file` reference in the backend config. The
  gateway never logs the key value — check the name, not the secret.
- **Unknown model / model not in catalog** — the model isn't
  configured, or isn't named on the license (don't-display returns the
  same error as a typo). Check the Models tile and the license's
  provider list.
- **Embedded/local model won't load** — confirm the GGUF file is in
  `data/models` and the config names it exactly. Large models need
  RAM; check the boot log.
- **Empty responses from a reasoning model** — raise `max_tokens`
  (reasoning models spend tokens on reasoning first).

## Budgets

- **Requests refused with a budget error** — the team or key hit its
  monthly cap. An admin raises or resets the budget in the console
  (Governance tile). Refusals are by design: hard stops, not warnings.

## Console and API

- **401 opening the console** — you need an admin or gateway key. The
  boot log points at the configured keys.
- **503 on an endpoint** — usually a backend not configured for that
  route; check the Services tile for the alias chain and fallbacks.

## Finding things

- Request logs: `data/gateway-log-*.jsonl` (append-only audit chain)
- Boot/service log: `data/gateway-service.log` (when run as a service)
- Health check: `GET /healthz` (authenticated) shows status, version,
  license mode, and backend summary

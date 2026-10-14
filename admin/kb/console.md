# Console — the admin interface

title: Console — admin interface tiles and where things live
description: The Gatelodge admin console: tiles, quick-add presets, access management, license view

## Overview

The console is the built-in admin UI, served by the binary at
http://localhost:8010 (default). One page, no separate install. Tiles:

- **Models** — the serving catalog: aliases, the models behind them,
  and which backend each routes to
- **Services** — alias chains, hedge/fallback plans
- **Cloud** — cloud backend catalog and status
- **Performance** — latency and throughput views
- **Syslog** — live request log stream
- **Parameters** — backend configuration (base URLs, key references)
- **Access** — gateway keys (create, revoke, roles), admin keys,
  console-managed provider credentials
- **Health** — healthz detail, backend reachability
- **Aliases** — alias resolution preview
- **Governance** — audit chain verification, budget views
- **License** — serial, edition, expiry, trial countdown

## Quick add

The console includes a **quick-add panel with 10 backend presets**
(OpenAI, Anthropic, Gemini, OpenAI Responses, Azure, Vertex, Bedrock,
RunPod, llama.cpp server, embedded local) — pick a preset, paste the
provider key or point at a base URL, save. Backends appear in the
catalog immediately.

## Access and keys

- **Gateway keys** are what applications use (the key your app sends
  instead of a provider key). Create per team/service, assign roles,
  revoke instantly.
- **Roles**: admin (full), read-only ops (view everything, change
  nothing), key-manager (manage keys only). Tiles are role-aware.
- Provider credentials can be pasted in the console (Access tile) —
  they are stored in the managed secrets store and **never render back
  or appear in logs**.

## License tile

Shows edition, serial, expiry (or trial days remaining), and the
providers named on the license. If a license file fails to load, this
tile shows why.

## Troubleshooting the console

- 401 on opening endpoints: you need an admin key — see the boot log
  for the generated key path, or use the key you configured
- The console is read-protected by the same auth pipeline as the API —
  gateway keys with the right role work everywhere

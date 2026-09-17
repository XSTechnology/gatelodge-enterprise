# LLM Gateway v1 — Design Doc

**Status:** approved design, not yet built. Execute in a fresh session; read
`.workshop/context.md` first for environment gotchas. Estimate: ~50-80 turns
across 4 checkpointed stages.

## Purpose

One local OpenAI-compatible endpoint fronting local llama.cpp (our embedded
shim) AND any OpenAI-compatible cloud (Groq / RunPod pods / OpenRouter /
OpenAI). Local programs need only a standard OpenAI client pointed at
`http://localhost:PORT/v1`. Differentiators vs stock llama-server / LiteLLM:
JSON-convention tool calling for ANY model, template robustness fixes
(system-at-beginning folding, tools=None guard, think-stripping, echo retry),
hedge-on-latency fallback, free-VRAM GPU placement, home-scale simplicity.

## Shape

Standalone FastAPI app in a new top-level package `gateway/` that IMPORTS the
existing agent modules (no code copy). Runs as its own process on its own
port (default 8010) — NOT a mode inside the agent server (keeps lifespans
independent; both can run simultaneously; the 24GB card is the shared
resource and GPU placement configs must be coordinated by the operator).

Frozen build: extend `exe_entry.py` routing with `--gateway` (mirrors
`--catalog-runner`), reusing the existing PyInstaller machinery.

## API surface (v1)

- `GET /v1/models` — union of all backend models + aliases
- `POST /v1/chat/completions` — non-streaming + SSE streaming (`stream:true`)
- `POST /v1/completions` — passthrough for cloud; embedded via shim
  `create_completion` (only if trivial — else defer to v2)
- `GET /healthz` — backend status, VRAM, loaded instances
- `POST /admin/aliases` — CRUD on aliases (hot-reload config)
- `POST /admin/load` / `POST /admin/unload` — instance control (embeds the
  existing shim machinery)
- `GET /` (Stage 5, optional) — console web page: config editor, models
  table, performance stats, live logs (see Stage plan)

Auth: bearer tokens from config (`gateway_keys` list). No keys configured =
open (localhost trust). Bind 127.0.0.1 by default; `gateway_bind` config to
open up.

## Config: `data/gateway_config.json` (hot-reloadable)

```json
{
  "port": 8010,
  "bind": "127.0.0.1",
  "gateway_keys": [],
  "default_alias": "smart",
  "backends": {
    "local": {"type": "embedded", "models": {
        "fast":  {"gguf": "qwen2.5-0.5b-instruct-q4_k_m", "ctx": 4096},
        "smart": {"gguf": "Qwen3.8-27B-Uncensored-Q4_K_M", "ctx": 4096},
        "home":  {"gguf": "Home-3B-v3.q4_k_m", "ctx": 3072}
    }},
    "groq":  {"type": "openai", "base_url": "https://api.groq.com/openai/v1",
               "api_key_env": "GROQ_API_KEY",
               "models": {"cloud-fast": "llama-3.1-8b-instant"}}
  },
  "aliases": {
    "fast":  {"chain": ["local:fast"], "hedge_s": 0},
    "smart": {"chain": ["local:smart", "groq:cloud-fast"], "hedge_s": 8},
    "private": {"chain": ["local:smart"], "local_only": true}
  },
  "queue": {"max_waiting": 4, "timeout_s": 120}
}
```

Env vars resolve `api_key_env` at startup (never store keys in the JSON).
`local_only` aliases never leave the machine (privacy guarantee).

## Request flow

1. Resolve `model` → alias (or `backend:model` direct form) → chain
2. Acquire backend client: embedded → `get_shared_shim()`-style instance
   keyed by (gguf, ctx); openai → `AsyncOpenAI(base_url, key)`
3. Tools present + embedded backend → the shim's JSON-convention tool
   instruction + response parsing (already built — `_normalize_messages`,
   `_parse_tool_call`); tools + cloud → forwarded as-is (native support)
4. Hedge: if chain has 2+ entries, start primary; after `hedge_s` (or on
   error) race the fallback — first completion wins (pattern already exists
   in `llm_client._cloud_chat_hedged`; lift it into the gateway)
5. Streaming: embedded → the shim's `_stream` (async generator, think-strip
   hold-back included); cloud → SSE passthrough
6. Log one JSONL line per request to `data/gateway_log.jsonl`:
   `{ts, model, backend, stream, ms, usage, status}`

Concurrency: per-instance asyncio locks serialize embedded generations (as
today). Queue with `max_waiting` — over-limit requests get 429 with
`Retry-After`. (Worker pools / batching = v2.)

## Reuse map (import, do not copy)

| Need | Source |
|---|---|
| Embedded engine, instances, placement, q8-fallback, tools shim | `agent/embedded_llm.py` (EmbeddedShimClient, installed_ggufs, detect_gpus, _placement_kwargs) |
| Hedge pattern | `agent/llm_client.py` `_cloud_chat_hedged` |
| GGUF registry | `installed_ggufs()` (data/models scan) |
| GPU placement config | LLM_GGUF_*_GPU keys — gateway defines its OWN per-model placement in its config (`"placement": "0"|"auto"`) passed to `_Instance` |
| Benchmarks | `model_manager._run_benchmark_impl` (optional admin hook, defer if time) |

Config decoupling note: embedded_llm reads agent settings via
settings_manager — the gateway must NOT depend on agent app_config. Either
(a) instantiate the shim and set `_cfg` directly from gateway config
(PreferredShimClient subclass or a small adapter in gateway/), or (b) pass
`set_db`-free config. Decide during Stage 1; keep agent/ untouched.

## Stage plan (each stage = commit + live verification)

1. **Skeleton + non-streaming**: gateway app, config load, `/v1/models`,
   `/v1/chat/completions` non-stream against the embedded backend only.
   Verify with a plain `openai` python client pointed at localhost.
2. **Streaming**: SSE for embedded (bridge shim `_stream`) + `/healthz`.
3. **Cloud backends + aliases + hedge**: openai-type backends, chain
   resolution, hedge race, `private` flag, JSONL logging.
4. **Auth + admin + packaging**: bearer keys, alias CRUD, load/unload,
   exe_entry `--gateway` routing, full test suite, README-less (config
   comments only).
5. **Console page (optional)**: single static page served at `/` with four
   panes — Config (edit aliases/backends/chains, hits hot-reload), Models
   (embedded instances: loaded/on-disk, VRAM, ctx, placement; cloud models
   per backend), Performance (latency/tokens per model from
   gateway_log.jsonl — reuse the admin benchmark-history rendering), Logs
   (live tail of request log + gateway events). Assemble from existing
   admin patterns (cat-btn panels, api() wrapper) — no new machinery.
   ~15-25 turns.

## Tests (mirror existing patterns)

- Alias resolution incl. `backend:model`, unknown → 400
- Hedge: primary slow → fallback answers; primary fast → fallback cancelled
- `local_only` never constructs a cloud client (mock + assert)
- Embedded tools path end-to-end with the FakeLlama module trick
  (sys.modules injection — see test_home_embedded.py)
- SSE: chunks parse as OpenAI stream events; think-tags never leak
- Auth: 401 without key when configured; open when not
- Config hot-reload: alias change applies without restart

## Deliberately deferred (v2)

- llama-server subprocess backend (continuous batching / slots)
- RunPod pod lifecycle (spin-up on demand, capacity overflow)
- Worker pools (k instances of small models), instance-per-ctx cache
- `/v1/embeddings`, LoRA, multimodal
- Token accounting / cost dashboards

## Open decisions (resolve at Stage 1, default in parentheses)

- Direct-model form `backend:model` accepted? (yes)
- Per-request `max_tokens` cap for cloud like LLMClient's 800? (yes, 800)
- Does the gateway share data/models with the agent? (yes — same dir; both
  processes can hold the same GGUF only if VRAM allows — placement configs
  must be planned by the operator; document in config comments)

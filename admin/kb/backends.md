# Backends — the 10 backend types and configuration

title: Backends — 10 backend types, config shapes, alias chains, hedges
description: How to configure each backend type, models, alias chains, hedge routing and failover

## The 10 backend types

1. **embedded** — local GGUF inference inside the binary (no external
   service; works offline)
2. **llama-server** — a llama.cpp server you run (OpenAI-compatible)
3. **runpod** — RunPod pods (dedicated)
4. **runpod-public** — RunPod serverless/public endpoints
5. **openai** — OpenAI API (or any OpenAI-compatible base URL)
6. **anthropic** — Anthropic Messages API (native wire type)
7. **gemini** — Google Gemini (native)
8. **responses** — OpenAI Responses API
9. **azure** — Azure OpenAI (needs resource + api_version)
10. **vertex** — Google Vertex AI (needs project)
11. **bedrock** — AWS Bedrock (needs region; native Converse wire type)

## Configuration shape

Backends live in `data/gateway_config.json` (or via the console's
quick-add). Common shapes:

- **openai-shape** (openai/llama-server/runpod-public): `base_url`,
  `api_key_env` (or `api_key_file`), and `models: {"alias": "remote-id"}`
- **azure**: `resource`, `api_version`, key env, model map
- **vertex**: `project`, credentials, model map
- **bedrock**: `region` (auth via AWS credentials), model map
- **embedded**: `models: {"alias": {"gguf": "<model-file>", "ctx": 8192}}`

Model files for embedded inference go in the `data/models` directory.

## Alias chains

Aliases route per task. A chain entry maps an alias to a model, and can
carry **hedges** (a slow primary loses the race to its fallback) and
**fallback** plans (used when the primary errors). Example: `smart` →
anthropic sonnet with a fallback to groq. Applications only ever name
the alias — swapping providers is zero-code.

## Don't-display doctrine

Models not named on the license (or not configured) **never appear** in
the catalog — and a request for one returns the same unknown-model
error as any typo. Nothing about unapproved models leaks.

## Notes

- Provider keys are referenced by env var or file path — never inline
- The same gateway config drives the console, API, and audit views

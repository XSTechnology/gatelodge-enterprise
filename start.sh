#!/usr/bin/env bash
# LLM Gateway — serves http://127.0.0.1:8010/v1 (OpenAI-compatible)
# Config: data/gateway_config.json (hot-reloadable). Models: data/models.
cd "$(dirname "$0")"
if [ -x ".venv/bin/python" ]; then
    exec .venv/bin/python -m gateway
elif [ -f ".venv/Scripts/python.exe" ]; then
    exec .venv/Scripts/python.exe -m gateway
else
    echo "[gateway] .venv missing. Create it with: uv venv && uv sync" >&2
    exit 1
fi

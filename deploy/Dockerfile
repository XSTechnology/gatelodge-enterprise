# Gatelodge Enterprise — multi-replica gateway image (tier D).
#
# The image EXCLUDES the embedded llama.cpp engine (the enterprise
# deployment story: replicas serve cloud / llama-server / RunPod
# backends; GPU inference stays on GPU boxes via llama-server remote
# mode — the gateway's import is deferred, so the app runs fine
# without it). Build from the repo root:
#
#   docker build -f deploy/Dockerfile -t gatelodge-enterprise .
#
# Runtime wiring (tier D — see deploy/docker-compose.yml):
#   GATELODGE_REDIS_URL    shared serving state (rate windows, caches…)
#   GATELODGE_CONFIG_DSN   shared config (Postgres, seeded from the
#                          mounted data/gateway_config.json on first boot)
FROM python:3.12-slim

# uv for a locked, reproducible install (matches the dev workflow)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1

# deps first (layer cache): lock + pyproject only
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-package llama-cpp-python

# app code (gateway/ + gateway_enterprise/ + entry point)
COPY gateway ./gateway
COPY gateway_enterprise ./gateway_enterprise
COPY enterprise_main.py ./

# operator config + runtime state (mount over these; the DB store
# seeds from data/gateway_config.json when its table is empty)
RUN mkdir -p data
VOLUME ["/app/data"]

# non-root, per the self-hosted posture
RUN useradd -r -u 10001 gatelodge && chown -R gatelodge:gatelodge /app
USER gatelodge

EXPOSE 8011
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD /app/.venv/bin/python -c "import httpx,sys; sys.exit(0 if httpx.get('http://127.0.0.1:8011/healthz', timeout=4).status_code == 200 else 1)"

CMD ["/app/.venv/bin/python", "enterprise_main.py"]

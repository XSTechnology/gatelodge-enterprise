# Gatelodge licensing service — trial download + paid-key issuance.
#
# Deployed as a Coolify application behind the same Traefik/tunnel as the
# sites. Env-configured (see app.py docstring); the community binary is
# bind-mounted at /app/binary, the sqlite store lives in /data.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py emailer.py .

RUN mkdir -p /data && useradd -r -u 10002 gatelodge \
    && chown -R gatelodge:gatelodge /app /data
USER gatelodge

EXPOSE 8100
VOLUME ["/data"]

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8100"]

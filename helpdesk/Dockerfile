# Gatelodge helpdesk service — the Gatekeeper (phase 4 v1).
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY kb/ ./kb/

RUN mkdir -p /data && useradd -r -u 10002 gatelodge \
    && chown -R gatelodge:gatelodge /app
USER gatelodge

EXPOSE 8100

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8100"]

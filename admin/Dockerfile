# Gatelodge admin — the Lodge Office (LAN-only).
# Needs the KB docs too (seed content for overrides + doc list).
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY kb/ ./kb/

RUN useradd -r -u 10002 gatelodge && chown -R gatelodge:gatelodge /app
USER gatelodge

EXPOSE 8111

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8111"]

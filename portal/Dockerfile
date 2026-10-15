# Gatelodge Task Portal — the client-facing task product.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

RUN useradd -r -u 10002 gatelodge && chown -R gatelodge:gatelodge /app
USER gatelodge

EXPOSE 8106

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8106"]

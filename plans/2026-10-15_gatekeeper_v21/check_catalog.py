"""Check the live groq catalog for embedding models (names only)."""
import httpx

KEY = open(r"C:\Users\dlang\Workspace\gateway_secrets\groq_key.txt",
           encoding="utf-8").read().strip()

r = httpx.get("https://api.groq.com/openai/v1/models",
              headers={"Authorization": f"Bearer {KEY}"}, timeout=30)
ids = sorted(m["id"] for m in r.json().get("data", []))
print("catalog:", len(ids), "models")
for i in ids:
    print("  ", i)
emb = [i for i in ids if "embed" in i.lower()]
print("\nembedding models:", emb if emb else "NONE — swap stays parked")

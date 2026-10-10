"""benchmark.py — LLM Gateway overhead measurement.

Measures the delta between calling a cloud provider directly vs through
the gateway (non-cached: unique prompts defeat the response cache).

Usage: python benchmark.py [base_url] [model] [rounds]
"""
import asyncio
import statistics
import sys
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8010"
MODEL = sys.argv[2] if len(sys.argv) > 2 else "hf:glm53"
ROUNDS = int(sys.argv[3]) if len(sys.argv) > 3 else 20


async def measure(client: httpx.AsyncClient, url: str, model: str,
                  headers: dict, nonce: int) -> float:
    """One round-trip with a unique prompt; returns elapsed ms."""
    t0 = time.perf_counter()
    r = await client.post(
        url,
        headers=headers,
        json={"model": model,
              "messages": [{"role": "user",
                            "content": f"Reply with exactly: bench {nonce}"}]})
    elapsed = (time.perf_counter() - t0) * 1000
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return elapsed


async def bench_route(client: httpx.AsyncClient, url: str, model: str,
                      headers: dict, label: str) -> list[float]:
    """Warm up + measure N rounds with unique prompts."""
    for _ in range(2):
        try:
            await measure(client, url, model, headers, 0)
        except Exception:
            pass
    times = []
    base_nonce = int(time.time() * 1000)
    for i in range(ROUNDS):
        t = await measure(client, url, model, headers, base_nonce + i)
        times.append(t)
        await asyncio.sleep(0.1)
    print(f"  {label}: median={statistics.median(times):.0f}ms  "
          f"mean={statistics.mean(times):.0f}ms  "
          f"p95={sorted(times)[int(len(times) * 0.95)]:.0f}ms")
    return times


async def main() -> None:
    print(f"LLM Gateway overhead benchmark — {ROUNDS} rounds, unique prompts")
    print(f"  gateway: {BASE}   model: {MODEL}\n")

    async with httpx.AsyncClient(timeout=120, verify=False) as client:
        # 1. through the gateway
        print("Through the gateway:")
        gw_times = await bench_route(client, f"{BASE}/v1/chat/completions",
                                     MODEL, {}, "gateway")

        # 2. direct to the provider
        r = await client.get(f"{BASE}/admin/backends")
        backends = r.json().get("backends", [])
        parts = MODEL.split(":")
        bname = parts[0] if len(parts) > 1 else ""
        alias = parts[1] if len(parts) > 1 else MODEL
        hf_backend = next((b for b in backends
                           if b.get("name") == bname), None)
        if not hf_backend or not hf_backend.get("base_url"):
            print(f"\n  (backend {bname!r} not found — gateway-only result)")
            return

        # provider base_url already includes /v1
        provider_url = hf_backend["base_url"].rstrip("/")
        if not provider_url.endswith("/v1"):
            provider_url += "/v1"
        chat_url = f"{provider_url}/chat/completions"

        # wire model: the value in the models dict (not the alias)
        models_dict = hf_backend.get("models", {})
        wire_model = models_dict.get(alias, alias)

        # resolve the key
        import os
        import keyring
        key_env = hf_backend.get("api_key_env", "HF_TOKEN")
        api_key = (os.environ.get(key_env)
                   or keyring.get_password("Workshop", key_env)
                   or keyring.get_password("workshop", key_env) or "")
        if not api_key:
            print(f"\n  (cannot resolve {key_env} — gateway-only result)")
            return

        headers = {"Authorization": f"Bearer {api_key}"}
        print(f"\nDirect to provider ({chat_url}, model={wire_model}):")
        try:
            direct_times = await bench_route(client, chat_url,
                                             str(wire_model), headers,
                                             "direct")
            overhead = [g - d for g, d in zip(gw_times, direct_times)]
            print(f"\n  ── Gateway overhead (the enterprise pitch number) ──")
            print(f"  median: +{statistics.median(overhead):.1f}ms")
            print(f"  mean:   +{statistics.mean(overhead):.1f}ms")
            print(f"  p95:    +{sorted(overhead)[int(len(overhead) * 0.95)]:.1f}ms")
        except Exception as e:
            print(f"\n  (direct benchmark failed: {e})")
            print(f"\n  Gateway median: {statistics.median(gw_times):.0f}ms")


if __name__ == "__main__":
    asyncio.run(main())

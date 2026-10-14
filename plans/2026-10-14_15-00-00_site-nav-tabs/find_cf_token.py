"""Look for a Cloudflare API token in the workshop keyring (names only)."""
import keyring

candidates = [
    "CLOUDFLARE_TOKEN", "CF_TOKEN", "CF_API_TOKEN",
    "CLOUDFLARE_API_TOKEN", "CLOUDFLARE", "cloudflare",
]
for name in candidates:
    try:
        v = keyring.get_password("workshop", name)
    except Exception as e:
        print(name, "ERR", type(e).__name__)
        continue
    if v:
        print(f"{name}: present (len={len(v)}, prefix={v[:5]}***)")
    else:
        print(f"{name}: -")

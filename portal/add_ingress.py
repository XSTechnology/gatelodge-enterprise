"""Add the portal ingress to the vm-rehearsal tunnel via the CF API."""
import json

import httpx
import keyring

TOKEN = keyring.get_password("workshop", "CLOUDFLARE_API_TOKEN")
ACCOUNT = "728d583f1a24c4b3dc8658c4e2dac9ae"
TUNNEL = "2107a01d-4f34-4e2e-bda8-a869978290a7"
H = {"Authorization": f"Bearer {TOKEN}"}
BASE = (f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT}"
        f"/cfd_tunnel/{TUNNEL}/configurations")

with httpx.Client(timeout=30) as c:
    r = c.get(BASE, headers=H)
    j = r.json()
    if not j.get("success"):
        raise SystemExit(f"get config failed: {j.get('errors')}")
    config = j["result"]["config"]
    print("current ingress:")
    for ing in config.get("ingress", []):
        print(f"  {ing.get('hostname', '(catch-all)')} -> "
              f"{ing.get('service', '?')}")

    # add portal if not present
    if any("portal" in str(i.get("hostname", "")) for i in
           config.get("ingress", [])):
        print("portal ingress already present")
    else:
        # insert before the catch-all (last entry)
        portal_entry = {"hostname": "portal.datagatelodge.com",
                        "service": "http://10.1.2.217:8106"}
        ingress = config.get("ingress", [])
        # insert before the last (catch-all) entry
        if ingress and ingress[-1].get("hostname") is None:
            ingress.insert(-1, portal_entry)
        else:
            ingress.append(portal_entry)
        config["ingress"] = ingress

        r = c.put(BASE, headers=H, json={"config": config})
        j2 = r.json()
        print("update:", j2.get("success"),
              "" if j2.get("success") else str(j2.get("errors"))[:300])
        if j2.get("success"):
            print("new ingress:")
            for ing in config["ingress"]:
                print(f"  {ing.get('hostname', '(catch-all)')} -> "
                      f"{ing.get('service', '?')}")

"""Raise the gateway groq backend max_tokens_cap 800 -> 3000 via
PUT /admin/config (the gateway's own config store, audited)."""
import paramiko

PW = "@231Garton"
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("10.1.2.217", username="coolify", password=PW, timeout=15)

PROBE = r'''
import json, os, httpx
GW = os.environ.get("GATEWAY_URL", "http://gatekeeper-gw:8110")
KEY = os.environ["GATEWAY_KEY"]          # the gateway key the services use
H = {"Authorization": f"Bearer {KEY}"}

r = httpx.get(f"{GW}/admin/config", headers=H, timeout=30)
print("get config:", r.status_code)
cfg = r.json()
raw = json.loads(cfg["text"])
gq = (raw.get("backends") or {}).get("groq")
print("groq backend:", json.dumps(gq, indent=None))
gq["max_tokens_cap"] = 3000
body = {"text": json.dumps(raw, indent=2)}
r2 = httpx.put(f"{GW}/admin/config", headers=H, json=body, timeout=30)
print("put config:", r2.status_code, r2.text[:200])
r3 = httpx.get(f"{GW}/admin/config", headers=H, timeout=30)
gq2 = json.loads(r3.json()["text"])["backends"]["groq"]
print("groq max_tokens_cap now:", gq2.get("max_tokens_cap"))
# smoke: a call that needs >800 completion tokens gets them now
r4 = httpx.post(f"{GW}/v1/chat/completions", headers=H,
    json={"model": "answer", "messages": [
        {"role": "system", "content": "Output ONLY JSON: {\"ok\": true, \"note\": \"<one sentence>\"}"},
        {"role": "user", "content": "note about caps"}],
     "max_tokens": 2000, "temperature": 0}, timeout=60)
d = r4.json()
u = d.get("usage", {})
print("smoke: finish", d["choices"][0].get("finish_reason"),
      "completion", u.get("completion_tokens"),
      "reasoning", u.get("completion_tokens_details", {}).get("reasoning_tokens"),
      "content:", repr((d["choices"][0]["message"].get("content") or "")[:80]))
'''

SCRIPT = ("docker exec -i lodge-office python - <<'PYEOF'\n" + PROBE
          + "\nPYEOF\n")
stdin, stdout, stderr = ssh.exec_command("sudo -S -p '' bash -s", timeout=180)
stdin.write(PW + "\n")
stdin.write(SCRIPT)
stdin.channel.shutdown_write()
print(stdout.read().decode("utf-8", "replace"))
e = stderr.read().decode("utf-8", "replace").strip()
if e:
    print("STDERR:", e[-400:])
ssh.close()

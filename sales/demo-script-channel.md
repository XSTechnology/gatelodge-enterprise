# Gatelodge — 15-Minute Channel Demo Script

**Audience:** inference-platform BD / engineering leadership (Groq, DeepInfra,
Novita, Nebius — the channel-trial targets).
**Goal:** they see, in fifteen minutes on one laptop, that (1) the co-branded
trial is real and takes seconds to issue, (2) their platform is the entitled
default and competitors are structurally excluded, (3) the governance story
(budgets, audit chain, keys) is their stickiness story, and (4) conversion
mechanics are clean.
**Every step below was executed and verified on the v2.2.1 community binary
(2026-10-12).** Expected outputs are copied from that run.

---

## Props & prep (once, before the meeting — ~10 minutes)

Laptop with:

1. **The demo workspace** — a copy of the community binary tree:
   ```
   robocopy C:\Users\dlang\Workspace\gl_binary_test\community ^
            C:\Users\dlang\Workspace\gl_binary_test\channel_demo /MIR /XD data
   mkdir C:\Users\dlang\Workspace\gl_binary_test\channel_demo\data
   ```
2. **The demo config** — `channel_demo\data\gateway_config.json`:
   ```json
   {
     "port": 8015, "bind": "127.0.0.1",
     "models_dir": "C:/Users/dlang/Workspace/gatelodge_standard/data/models",
     "gateway_keys": ["smoke-community-8015"],
     "auth": {"token_forms": ["path", "query"]},
     "backends": {
       "local": {"type": "embedded",
                  "models": {"fast": {"gguf": "qwen2.5-0.5b-instruct-q4_k_m",
                                       "ctx": 8192}}},
       "groq": {"type": "openai",
                 "base_url": "https://api.groq.com/openai/v1",
                 "api_key_file": "C:/Users/dlang/Workspace/gateway_secrets/groq_key.txt",
                 "models": {"gptoss": "openai/gpt-oss-20b"},
                 "monthly_token_budget": 200},
       "together": {"type": "openai",
                     "base_url": "https://api.together.xyz/v1",
                     "api_key_env": "TOGETHER_API_KEY",
                     "models": {"lh": "meta-llama/Llama-3.1-8B-Instruct"}}
     }
   }
   ```
   The `together` backend is the plant for the channel-lock beat. The 200-token
   budget is the plant for the budget beat.
3. **Verify the model id against the live catalog** the morning of the demo
   (groq rotates ids — `llama-3.1-8b-instant` died between runs):
   `GET /admin/backends/groq/remote-models` once the gateway is up, or check
   the 8010 daily driver. Current verified chat models: `openai/gpt-oss-20b`,
   `openai/gpt-oss-120b`, `qwen/qwen3.8-27b`, `allam-2-7b`.
4. Terminal 1 open in `gatelodge_standard` (for license issuance), Terminal 2
   open in `channel_demo`. Browser bookmarked to `http://127.0.0.1:8015`.
5. **No license file in `channel_demo\data\`** — that gets issued live.

---

## T-minus (5 minutes before they join)

- Delete `channel_demo\data\license.json` if a rehearsal left one.
- Confirm the groq key file exists (`gateway_secrets\groq_key.txt`).
- Close other gateways if ports conflict (8015 must be free).
- Rehearsal rule: run the whole flow once the same morning; budget state
  resets on restart (monthly budget persists in the log across restarts —
  if a rehearsal exhausted it, delete `data\gateway-log-*.jsonl`).

---

## The fifteen minutes

### 0:00–1:00 — Cold open (no screen, or the folder view)

**DO:** Show the `channel_demo` folder — one `.exe`, one `data` folder.
**SAY:** *"This is the whole product. One compiled binary — machine code,
no installer, no Python, no source. What your customer downloads from you
is exactly this, plus a license file. Speaking of which —"*

### 1:00–3:00 — Issue THEIR license, live

**DO:** In Terminal 1 (tailor `--business-name` to the person on the call):
```
uv run scripts/license_tool.py issue --edition community ^
  --business-name "Acme Robotics" --contract-email "it-lead@acme.example" ^
  --vendor groq --provider groq --provider llama-server ^
  --duration 90d --grace-hours 0 ^
  --out C:\Users\dlang\Workspace\gl_binary_test\channel_demo\data\license.json
```
**SEE (instant):**
```
license written: ...channel_demo\data\license.json
  serial=GL-7B162AEE8DCE edition=community business='Acme Robotics'
  features(on)=[] providers=['groq', 'llama-server'] duration=90d
```
**SAY:** *"Two seconds. Your name is on it as the vendor, the providers claim
names only your backend plus local compute, ninety days, and grace is zero —
when it expires, serving stops. No drift, no renewal nagging."*

### 3:00–4:30 — Boot + the license pane

**DO:** Terminal 2: `gatelodge-community.exe`. Watch the boot log scroll.
**SEE:**
```
License: community-edition license active (serial=GL-… business='Acme
Robotics' vendor='groq' support=False expires=2027-01-10…)
license: 1 cloud backend(s) NOT entitled and dropped from serving …: together
Gatelodge starting on http://127.0.0.1:8015
```
**DO:** Open the console (`http://127.0.0.1:8015`, key `smoke-community-8015`)
→ **License tile (🪪)**.
**SEE:** the entitlement picture — vendor, serial, providers, days remaining.
**SAY:** *"Note the boot log — it already told us `together` isn't entitled
and got dropped. Hold that thought for two minutes."*

### 4:30–6:30 — Serving through their brand

**DO:** Console → **Cloud tile** → the groq provider panel → catalog button.
**SEE:** 11 live models from their platform (pulled through their API key).
**DO:** Then a real completion — console or curl:
```
curl http://127.0.0.1:8015/v1/chat/completions ^
  -H "Authorization: Bearer smoke-community-8015" ^
  -H "Content-Type: application/json" ^
  -d "{\"model\":\"groq:gptoss\",\"max_tokens\":300,
       \"messages\":[{\"role\":\"user\",
       \"content\":\"Say 'the gate is open' and nothing else.\"}]}"
```
**SEE:** `"content": "the gate is open"` — and the usage block (210 tokens,
reasoning included).
**SAY:** *"Your models, your key, your pricing — behind one OpenAI-compatible
endpoint. Their app code never changes, whichever model they pick."*

### 6:30–8:00 — The channel lock (don't-display)

**DO:** Show `/v1/models` (console Models tile or curl). Then request the
planted model:
```
curl … -d "{\"model\":\"together:lh\",\"max_tokens\":10,
            \"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"
```
**SEE:** the model list contains the groq model and the local model —
`together`'s never appears. The request returns:
```
400 {"error":{"message":"unknown model: 'together:lh' (not an alias or a
backend model)","type":"invalid_request_error","code":"model_not_found"}}
```
**SAY:** *"Together is right there in the config — I just showed you the file.
It's not entitled by your license, so it doesn't render, doesn't serve, and
the error is the generic unknown-model answer. No 403, no signal it exists.
That's the channel lock: the customer's gateway only knows your platform."*

### 8:00–10:00 — Governance I: keys & the budget trip

**DO:** Console → **Access tile**: show the gateway keys, roles, and the
console-managed secrets store (set any `TEST_KEY` → stored, never displayed).
Then the budget beat — fire the same completion again:
**SEE:**
```
503 {"error":{"message":"backend 'groq': monthly token budget exhausted
(210/200 tokens this month) — raise monthly_token_budget or wait for the
month to roll over","type":"overloaded_error","code":"budget_exceeded"}}
```
**SAY:** *"The demo config set a 200-token monthly budget. The first request
charged 210 and the second one is refused — hard stop, clean error, per
backend or per team. This is the feature that turns their invoice from a
surprise into a dial. And notice the budget survived in the request log —
it's their data, in their perimeter."*

### 10:00–12:00 — Governance II: the audit chain

**DO:** Console → **Governance tile** (or `GET /admin/audit`).
**SEE:** entries like:
```
{'ts': '2026-10-12T19:41:02', 'key': 'admin',
 'action': 'secret-set DEMO_AUDIT_PROBE',
 'hash': '54955ac20abab0cf…'}
chain: lines 2 / chained 2 / valid: true
```
**SAY:** *"Every admin action is hash-chained — each entry links to the last,
tampering breaks the chain, and the console renders that verdict. This is the
artifact their auditor asks for, and it's why governance-with-your-inference
sells upmarket. Every request also lands in the structured event log —
who, what model, what it cost."*

### 12:00–13:30 — The local fallback + cost story

**DO:** Request the embedded local model (`model: "fast"`).
**SEE:** an instant completion from the GGUF running inside the same binary.
**SAY:** *"Same gateway, embedded open-weights model, zero inference spend.
Routing cheap tasks here and hard tasks to your platform is the cost-saver —
and if your API ever blips, their product still answers. Resilience they
can't get from a raw API key."*

### 13:30–15:00 — Close

**SAY:** *"Recap: one binary, your brand on the license, only your endpoints
entitled, budgets and audit from day one. On day 90 serving stops — they
keep the binary and buy the key, or upgrade to enterprise for teams, SCIM,
and scale. Either way you keep the inference and a 20–30% first-year share.
Next step: we issue a real trial license under your brand today and you put
it in front of two design-partner accounts. What's the right name on the
first one?"*

---

## Fallbacks

| Failure | Recovery |
|---|---|
| No network / groq unreachable | Skip beats 4–5; the local-model beat still lands; say "everything except the cloud hop runs offline — that's the point." |
| Model id 404s | Swap to `openai/gpt-oss-120b` or `qwen/qwen3.8-27b` in the config (hot-reload, no restart) — checked the catalog that morning. |
| Budget already exhausted from rehearsal | Delete `data\gateway-log-*.jsonl`, restart the binary (10 s). |
| Catalog fetch is slow | Skip to the completion; the catalog is the same endpoint. |
| License issuance fails (rare) | Pre-issue a backup license as `-bak.json` in the folder; copy it over and say "issued this morning, same command." |

## Follow-up kit (send same day)

- The marketing site URL (datagatelodge.com once deployed)
- The demo license file + this binary tree as a zip — "this is literally what your customers receive"
- The pricing one-pager (once built)
- Proposed trial terms: 90 days, co-brand, revenue share, 2 design-partner accounts

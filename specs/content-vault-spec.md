# Content Vault — engineering spec

Status: APPROVED FOR BUILD (owner, 2026-10-16). Enterprise-only add-on.
Default OFF. Owner decisions on record: go; Enterprise-only gating;
spec + messaging alignment before the standard-repo seam and the fork
vault module.

## 1. What it is

An optional, Enterprise-licensed capture of full AI traffic — request
and response bodies — written to an encrypted, tamper-evident, locally
stored vault. Designed for large organizations that must audit what
actually flows to and from AI: regulated industries, incident
forensics, EU-AI-Act-style logging duties.

Properties, in priority order:

1. **Content is never captured unless explicitly enabled.** The
   default gateway records metadata only (who/model/when/cost/
   outcome/trace) — that default posture is unchanged and remains the
   public claim.
2. **The gateway can write but never read.** Records encrypt to the
   customer's public keys. The gateway holds no private key material.
3. **Searchable by user without decrypting content.** A separate
   search key blinds a per-record index of the consuming gateway key
   (and model/team), so an investigator extracts a user's records
   without opening any payload.
4. **Escrow by construction.** Every record's data key wraps to every
   key in the active wrap set (primary + escrow). Losing any single
   key never loses data; loss of all keys is the only data-loss mode,
   and canaries make that loss loud on day one, not at investigation
   time.
5. **Tamper-evident like the audit chain.** Each record hashes the
   previous record's ciphertext; verification needs no keys at all.

## 2. Cryptographic design

### 2.1 Keys

- **Key D — data keys (asymmetric, customer-held).** A wrap set of
  customer public keys: one operational primary plus one or more
  escrow keys (offline / counsel-held / Shamir-split). Algorithms:
  X25519 (HPKE, RFC 9180) preferred; RSA-OAEP-3072 accepted.
- **Key S — search key (symmetric, 32 bytes).** Keys the blind
  indexes: `idx = HMAC-SHA256(KeyS, field_value)`. Escrowed inside the
  keyfile (§2.4).
- **Per-record DEK.** AES-256-GCM, fresh per record, wrapped to every
  key in the wrap set (multi-wrap).

### 2.2 Record format (JSONL, one per line)

```json
{"v": 1, "seq": 4127, "ts": "2026-10-16T12:00:00Z",
 "trace": "…", "sha_req": "…", "sha_resp": "…",
 "idx": {"user": "hmacS(key-abc123)",
         "model": "hmacS(groq:cloud-fast)",
         "team": "hmacS(platform)"},
 "prev": "sha256(prev_record.ct)",
 "dek": [{"v": 2, "alg": "HPKE-X25519", "wrap": "b64"},
         {"v": 1, "alg": "HPKE-X25519", "wrap": "b64"}],
 "nonce": "b64",
 "ct": "b64(AES-256-GCM(request + response + record metadata))"}
```

- `sha_req` / `sha_resp`: plaintext digests — after decryption the
  customer verifies content integrity against them.
- `prev`: sha256 over the previous record's `ct` bytes. Part files
  begin with a header record referencing the previous part's last
  hash, so the chain spans rotated files. Genesis: sha256 of the
  vault-id string.
- Plaintext payload inside `ct`: the full request body (messages,
  tools, params), the full response (assembled text for streams), and
  the same metadata the request log carries (key id, team, model,
  backend, usage, cost, status, trace id, session id).

### 2.3 Blind indexes

`idx.user` = HMAC-SHA256(KeyS, gateway-key-id). Deterministic —
equality search without decryption. Additional indexes (model, team)
under the same Key S. Optional trace-id index later. A sidecar index
file (`idx-user.json`: hmac → [seq…]) is a v2 optimization; v1 scans
the JSONL (HMAC compare per record is cheap).

### 2.4 Keyfile (`vault.keys`, written at vault init)

```json
{"v": 1, "created": "…", "vault_id": "…",
 "keys": [mirror of the wrap-set registry],
 "search_key": [{"v": 2, "wrap": "b64(KeyS to key v2)"},
                {"v": 1, "wrap": "b64(KeyS to key v1)"}]}
```

Key S itself, multi-wrapped to the same wrap set. Any private-key
holder can recover it. Losing the keyfile and all private keys is the
all-keys-lost case.

### 2.5 Key registry (vault config)

Append-only: `[{v: 1, role: "escrow", alg, pubkey}, {v: 2, role:
"primary", alg, pubkey}, …]`. Rotation adds a version; nothing is ever
removed. Records reference the wrap set in force at write time.
Retiring a key requires either keeping its private key in custody or
running `vault rewrap` first (§4.5).

## 3. Architecture — the seam rule

The gateway core never persists content today and stays that way by
default. The vault is wired through a new seam in gatelodge_standard;
the implementation lives in gateway_enterprise (fork), injected at
startup like the other four seams.

### 3.1 Standard repo — the capture seam

- **`TrafficCapture` protocol** (new module `gateway/traffic.py`):
  - `observe_request(meta, body)` — called once per request at entry,
    with the parsed request body.
  - `observe_response(meta, text)` — called at completion with the
    assembled response text (streams reassembled; the streaming paths
    already walk every content delta for counting — they will
    accumulate into the collector when a capture backend is active).
  - `flush(meta, status, usage)` — closes the record at the same
    chokepoint `_log_request` uses.
- **Default backend: complete no-op.** Zero writes, zero retention,
  zero measurable overhead when no capture backend is installed. The
  683-test suite must stay green untouched; new seam tests assert the
  no-op default and that no content reaches disk.
- **Config schema** (`vault` section, standard side): presence,
  passthrough to the installed backend, and the rule that enabling
  requires an Enterprise license claim (§6). The standard binary
  without the enterprise module rejects `vault.enabled` with a clear
  error.
- The seam is synchronous-cheap and non-blocking by contract (§4.4).

### 3.2 Fork — `gateway_enterprise/vault/`

- `writer.py` — record assembly, encryption, chain, part rotation.
- `keys.py` — registry, keyfile, wrap set, canary records.
- `index.py` — blind indexes (+ v2 sidecar index).
- `tools.py` — admin endpoints: verify, keycheck, search-extract.
- Console tile: status (mode, disk, parts, last seq/ts, dropped
  count), verify result, keycheck result.
- Customer-side reference script (documented format): decrypt +
  verify a record set with a private key; search with Key S. The file
  format is documented so any tooling can be built without us.

## 4. Flows

### 4.1 Write path (hot path)

1. Seam calls collect into a per-request buffer.
2. `flush` enqueues the record onto a **bounded async queue**
   (capacity: 10k records or 64 MB, configurable).
3. A writer task batches: HMAC indexes → DEK → multi-wrap → chain
   hash → append to the active part file.
4. **Overflow policy: drop and flag — never block a request.** A full
   queue increments a dropped counter, logs a warning, and surfaces on
   the console tile. An audit vault that causes an outage is worse
   than no vault; the drop counter keeps the failure honest and
   visible.

Performance target: capture overhead < 1 ms per request on the hot
path (HMAC + enqueue); encryption and disk happen off-path.

### 4.2 Search (Key S holder)

`vault search --user key-abc123` → compute HMAC once, scan indexes,
return the matching records **still encrypted** (+ seq/ts/chain refs).
Extraction without decryption. (The existing plaintext request log
already carries key ids — investigators start there for the coarse
picture and join into the vault via `idx.user`.)

### 4.3 Decrypt (Key D holder)

Unwrap the DEK with any private key from the record's wrap set →
AES-GCM decrypt → verify `sha_req`/`sha_resp`. Only the extracted
records are ever decrypted.

### 4.4 Capability split

| holds | can | cannot |
|---|---|---|
| files only | verify the chain (no keys) | read or search |
| Key S | search + extract by user/model/team | read content |
| Key D (any one) | decrypt extracted records; recover Key S from keyfile | search without S |

### 4.5 Rotation, rewrap, canaries

- **Rotate:** add a public key version → new records multi-wrap to the
  new set. Old records unchanged, still openable by the keys live at
  their write time. The console shows which versions exist and their
  record coverage.
- **Rewrap (planned retirement):** offline tool: old private key +
  new public key → re-wrap DEKs of selected records. Requires the old
  key, so it must happen before retirement.
- **Canaries:** every key addition/rotation writes a canary record
  (known plaintext) wrapped to that key. `vault keycheck` (operator-
  run or scheduled from the console) attempts each canary with each
  held private key and reports per-version health. Key loss is
  discovered within a check cycle — not at investigation time.

## 5. Operations

- **Capture modes:** `all` (default when enabled) · `prompts` (request
  bodies only) · `responses` · `sample:1/N`.
- **Parts:** rotate by size (default 512 MB) or day
  (`vault-YYYYMMDD.jsonl`); header records continue the chain.
- **Retention:** `keep_days` and `max_gb`; deletion drops whole oldest
  parts and writes a tombstone header (chain continues from the last
  surviving record — the deleted span is provably absent, not
  silently missing).
- **Disk:** hard cap; at cap the writer drops (counter) rather than
  filling the volume.
- Location: `data/vault/` beside the existing logs.

## 6. Licensing

Enterprise-only. A `vault` claim bit on enterprise licenses; community
licenses (trial included) reject `vault.enabled` at boot with a clear
message. Consistent with SSO/SCIM/teams/multi-replica gating.

## 7. Test plan

**Standard suite (new):** seam calls fire with correct args on
streaming and non-streaming paths; default backend is a no-op (no
files, no measurable latency); `vault.enabled` without the enterprise
module errors clearly; nothing content-shaped reaches disk by default.

**Fork suite (new):** crypto round-trip (multi-wrap: each key in the
set opens the record); blind-index search finds exactly the right
records and nothing else; chain verify passes on intact files and
fails on any byte edited anywhere; rewrap produces openable records
under the new key; canary keycheck reports per-version health;
modes capture the right halves; overflow drops and counts without
blocking; rotation + retention preserve chain verifiability; keyfile
recovery of Key S; license gating.

## 8. Messaging alignment (ships with this spec)

- **Deck** (marketing/security-architecture-deck.md): claims-discipline
  item 4 reworded — metadata by default, content never silently
  logged, Content Vault is the explicit opt-in; roadmap note added.
- **evaluation.md** ("Is prompt content logged?"): the answer stays
  "No" by default; adds a roadmap paragraph for regulated environments
  (opt-in, customer-held keys, user-searchable without decryption,
  escrowed by construction). No shipping claim until it ships.
- **Site: no change until ship.** The site claims proof, not promises;
  the exec-brief scope line stays true as written.
- At ship: console screenshot to The Product page, the deck's roadmap
  line becomes a shipped claim, evaluation.md's roadmap paragraph
  becomes present tense.

## 9. Milestones

- **M1 (this change):** spec + messaging alignment. DONE when this
  file, the deck item, and the evaluation.md note are committed and
  the KB change is deployed.
- **M2:** standard-repo seam (gateway/traffic.py + config + tests).
- **M3:** fork vault module (crypto, index, chain, keyfile, canaries,
  console tile, admin tools).
- **M4:** suites green both repos, license gating verified, docs and
  site updated, released (target: v2.4.0).

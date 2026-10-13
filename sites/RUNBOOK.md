# Sites — Production Runbook (Coolify + Cloudflare Tunnel)

Self-hosted multi-site platform on the on-site server boxes. Each site is a
self-contained directory (`sites/<name>/` — files + Dockerfile), deployed as a
Coolify application, reached through a Cloudflare tunnel. Nothing inbound,
TLS at the edge, DNS already in the datagatelodge Cloudflare account.

The container pattern is **verified**: `sites/datagatelodge` was built
(`nginx:alpine` + COPY) and served all paths correctly in local rehearsal
on 2026-10-13.

---

## Part 1 — Install Coolify on the server box (once)

On a fresh-ish Ubuntu/Debian box with Docker installed:

```bash
curl -fsSL https://cdn.coollabs.io/coolify/install.sh | bash
```

- Installs to `/data/coolify`, starts automatically (restart: always)
- Prints the admin URL: `http://<server-lan-ip>:8000` — open it ON THE LAN,
  set the admin email + password
- **Never give the panel a public hostname.** LAN-only, or behind Cloudflare
  Access if remote admin is needed. This panel controls every container.

## Part 2 — Git source for the sites (once)

Coolify deploys applications from git. Two options — pick A:

- **A (recommended): Gitea as a Coolify app.** Coolify → New Resource → one-click
  Gitea. Gives a private git server with a web UI on the same box. Create a
  user, an access token, and an empty repo `sites`.
- **B (minimal): bare repo.** `git init --bare /srv/git/sites.git`, then add it
  in Coolify as a private repository with a deploy key.

Push this repo's `sites/` content to it (from the dev box):

```
cd sites && git init && git add -A && git commit -m "sites v1"
git remote add origin <gitea-url>/sites.git && git push -u origin main
```

## Part 3 — Cloudflare tunnel on the server (once)

1. Cloudflare Zero Trust → Networks → Tunnels → **Create tunnel** (name e.g.
   `srv1`) → copy the Docker install command it shows (has the token)
2. Run it on the server (it includes `--restart unless-stopped`)
3. Public hostnames on the NEW tunnel point at **Coolify's proxy**:
   `http://<server-lan-ip>:80` — Traefik (bundled with Coolify) routes by
   hostname to each app

**The one gotcha:** Coolify defaults to Let's Encrypt certs, which need inbound
HTTP — impossible behind a tunnel. Instead, set each app's FQDN in **`http://`**
form (Coolify accepts it) so Traefik serves plain HTTP internally. Cloudflare's
edge provides the public HTTPS. SSL mode in Cloudflare: Flexible for these
hostnames.

## Part 4 — Deploy the first site (datagatelodge.com)

1. Coolify → New Resource → Application → **Private repository** → the `sites`
   repo → set the **Base directory / subdirectory**: `datagatelodge`
2. Build pack: Dockerfile (it finds `sites/datagatelodge/Dockerfile`)
3. Exposed port: **80**
4. Domains: `http://datagatelodge.com` and `http://www.datagatelodge.com`
5. Deploy — takes seconds (nginx:alpine + a COPY layer)

## Part 5 — Cutover DNS (the moment production moves)

On the NEW tunnel (dashboard → Public Hostname → Add):

| Subdomain | Domain | Service |
|---|---|---|
| *(blank)* | datagatelodge.com | HTTP → `<server-lan-ip>:80` |
| `www` | datagatelodge.com | HTTP → `<server-lan-ip>:80` |

Adding these **auto-repoints the DNS records** from the old dev-box tunnel to
the new server tunnel. Then verify:

- `https://datagatelodge.com` → 200, site content
- `https://datagatelodge.solutions` → 301 → .com (unchanged — redirects live in
  Cloudflare, independent of origin)
- `https://data-gate-lodge.com` → 301 → .com
- `https://datagatelodge.com/google91b36a516a210e8b.html` → 200 (Google
  verification survives — the file deploys with the site)
- `/robots.txt`, `/sitemap.xml` → 200

After verification, retire the dev-box origin: stop the `:3040` http.server
and the old `cloudflared` container on this workstation (keep a dev tunnel
only if wanted for previews).

## Part 6 — Databases + backups (once)

- Coolify → New Resource → Database → **PostgreSQL** (set root password; keep
  it network-internal — no public hostname)
- Same for **Redis** (cache/queues)
- Enable **scheduled backups** on each (S3-compatible target or local path),
  from day one
- Coolify's own database holds all app definitions — Settings → back that up too

## Part 7 — Adding future sites (the ongoing workflow)

1. Site gets built as `sites/<name>/` (self-contained: files + Dockerfile)
2. Local rehearsal on the dev box: `docker build` + run + verify every path
3. Commit + push to the `sites` repo
4. Server: Coolify new app from git (subdirectory `<name>`), port 80, FQDNs
5. Tunnel: add the public hostname
6. Verify, done

Per-site effort once the platform is up: **about two minutes.**
For large counts of pure-static sites (20+), consider consolidating them into
one nginx/Caddy container with vhosts instead of one app each.

## Notes

- Gatelodge gateway instances (closed Docker image) deploy as Coolify apps the
  same way when the interactive features arrive; Postgres/Redis from Part 6
- Coolify supports adding more servers to the same panel (Servers → Add) —
  that's the growth path, not migrating the panel
- Keep ingress tunnel-only; no port forwarding on the router
- The deployable directory contents (what "gets copied", if ever done by hand
  instead of git): `index.html, styles.css, site.js, robots.txt, sitemap.xml,
  google91b36a516a210e8b.html, Dockerfile` — build context = that directory

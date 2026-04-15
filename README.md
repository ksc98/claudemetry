# cc-proxy

A Cloudflare Worker, written in Rust, that proxies requests to the Anthropic API and records a structured row for every transaction into a **per-user SQLite database** (Durable Object with native SQL storage). Point `ANTHROPIC_BASE_URL` at it and every Claude Code (or Anthropic SDK) request becomes queryable, per account, forever.

Full passthrough: method, path, query, headers, and body are forwarded to `https://api.anthropic.com` unchanged. Streaming responses (SSE) are preserved — the client receives bytes immediately while the proxy captures the full body in the background via `ctx.wait_until`.

## Architecture

```
    Claude Code                        api.anthropic.com
         │                                    ▲
         │ POST /v1/messages                  │ unchanged
         ▼                                    │
  ┌──────────────────────────────────────────────┐
  │  cc-proxy (Rust Worker)                      │
  │                                              │
  │   identify         →  user_hash =            │
  │                        sha256(salt ‖ JWT.sub)│
  │                        truncate 8 bytes      │
  │                                              │
  │   forward          →  streaming response     │──► client
  │                                              │
  │   ctx.wait_until:                            │
  │     parse SSE usage                          │
  │     stub = USER_STORE.id_from_name(hash)     │
  │     stub.fetch("/ingest", JSON record)      ─┼──┐
  └──────────────────────────────────────────────┘  │
                                                    ▼
                        ┌──────────────────────────────┐
                        │  UserStore Durable Object    │
                        │  (one instance per user_hash)│
                        │                              │
                        │  state.storage().sql()       │
                        │                              │
                        │  INSERT INTO transactions    │
                        │  (tx_id, ts, model,          │
                        │   input/output/cache tokens, │
                        │   stop_reason, tools, …)     │
                        └──────────────────────────────┘
```

Each unique user_hash gets its own private SQLite database. No provisioning — the first request with a given hash materializes the DO, runs `CREATE TABLE IF NOT EXISTS`, and inserts the row. Two users share no table, no index, no memory space.

## Requirements

- Rust with the `wasm32-unknown-unknown` target
- Node (for `npx wrangler`)
- [`just`](https://github.com/casey/just)

## Run locally

```bash
just local
```

Starts `wrangler dev` on `http://localhost:8787`. In another shell:

```bash
export ANTHROPIC_BASE_URL=http://localhost:8787
claude   # or any Anthropic SDK client
```

Logs stream to the wrangler pane as JSON lines. To also capture them to a file:

```bash
just local-tee            # writes to proxy.log
just local-tee run.log    # custom path
```

## Deploy to Cloudflare

```bash
just login    # one-time, opens browser
just deploy
```

By default the worker is reachable at `https://cc-proxy.<your-subdomain>.workers.dev`. To use a custom domain, either uncomment the `routes` block in `wrangler.toml` (requires the zone to be on the same Cloudflare account) or bind the domain in the Cloudflare dashboard under Workers → cc-proxy → Settings → Domains & Routes.

Before sending traffic, generate and install a per-deployment salt so hashes aren't portable across deployments:

```bash
openssl rand -hex 16 | npx wrangler secret put HASH_SALT
```

If unset, the worker falls back to a published dev salt and emits hashes that aren't safe to publish.

Then point your client at whichever URL you ended up with:

```bash
export ANTHROPIC_BASE_URL=https://your-worker-url
```

View live logs with:

```bash
just tail
```

or in the Cloudflare dashboard (observability + traces are enabled in `wrangler.toml`).

## Identity: how user_hash is derived

Every request carries either an OAuth Bearer token (the usual Claude Code case) or an `x-api-key`. The proxy computes:

```
if   Bearer present  →  base64url-decode the JWT middle segment, take the "sub" claim
if   x-api-key only  →  use the raw key
else                  →  skip persistence

user_hash = hex(sha256(HASH_SALT ‖ identity))[:16]
```

The `sub` claim is the Anthropic account identifier and does **not** change when the Bearer wrapper refreshes, so a user's `user_hash` is stable across token rotations, devices, and months of use. Two Anthropic accounts produce two different hashes and two different Durable Objects. The raw token is never stored or logged.

## Persistence: the `transactions` table

Each user's private SQLite contains a single table (migrations are idempotent, applied on first access to the DO):

| column              | type    | source                                                      |
|---------------------|---------|-------------------------------------------------------------|
| `tx_id`             | TEXT PK | Anthropic's `message.id` from the SSE stream, or a fallback |
| `ts`                | INT     | Request arrival time (ms since epoch)                       |
| `session_id`        | TEXT    | `x-claude-code-session-id` header                           |
| `method`            | TEXT    | HTTP method                                                  |
| `url`               | TEXT    | Full upstream URL hit                                       |
| `status`            | INT     | HTTP status returned by Anthropic                            |
| `elapsed_ms`        | INT     | Total proxy-to-upstream-and-back latency                    |
| `model`             | TEXT    | Model used, from `message_start`                             |
| `input_tokens`      | INT     | Parsed from final usage snapshot                             |
| `output_tokens`     | INT     | Parsed from final usage snapshot                             |
| `cache_read`        | INT     | Prompt-cache reads                                           |
| `cache_creation`    | INT     | Prompt-cache writes                                          |
| `stop_reason`       | TEXT    | `end_turn`, `tool_use`, `max_tokens`, etc.                   |
| `tools_json`        | TEXT    | JSON array of tool names invoked in the turn                 |
| `req_body_bytes`    | INT     | Size of the forwarded request body                          |
| `resp_body_bytes`   | INT     | Size of the captured response body                          |

Indexed on `ts DESC` and `(session_id, ts)`.

## Admin probes

Three endpoints let you verify the pipeline without a dashboard. All three are scoped to **your own** `user_hash` by the auth header you present:

```bash
# Your stable user_hash for this deployment
curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/whoami

# Aggregate counts + token totals from your DO's SQLite
curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/stats

# Last 100 raw rows from your DO
curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/recent
```

These are internal endpoints intended to be replaced by a proper web dashboard later. They never expose another user's data — the hash is always derived from the caller's own token.

## What gets logged (console)

Each transaction also emits two structured JSON log lines to `wrangler tail`:

```json
{"dir":"req","ts":...,"method":"POST","url":"...","user_hash":"0203…","session_id":"…","body_len":1530}
{"dir":"resp","ts":...,"status":200,"elapsed_ms":777,"model":"claude-opus-4-6","input_tokens":443,"output_tokens":32,"cache_read":262754,"cache_creation":951,"stop_reason":"end_turn","tx_id":"msg_…","body_len":2472}
```

`authorization` and `x-api-key` values are never written to logs. The raw prompt/response bodies are not logged in the deployed worker either — they're parsed for metrics and then discarded.

## Layout

- `src/lib.rs` — fetch handler, `UserStore` Durable Object, SSE parser, user-hash derivation, admin probes
- `wrangler.toml` — proxy worker config (Durable Object binding + SQLite migration, observability on)
- `dashboard/` — Astro 6 + React dashboard worker, served behind Cloudflare Access
- `scripts/cf-access.sh` — idempotent provisioner for the Access apps/policies (`just cf-access`)
- `justfile` — `local`, `local-tee`, `build`, `login`, `deploy`, `tail`, `clean`, `dashboard-dev`, `dashboard-deploy`, `dashboard-tail`, `deploy-all`, `cf-access`

## Notes on trust

The proxy does not authenticate callers. Anyone with the deployed URL can use it as an open relay to Anthropic (they still need their own Anthropic credentials, which are forwarded as-is). If that matters for your deployment, add a shared-secret header check at the top of `fetch` in `src/lib.rs`, or put the worker behind Cloudflare Access.

The operator of the proxy has in-memory access to your raw token and prompt bodies while a request is in flight — this is unavoidable for any proxy. The code only persists a salted hash and parsed metrics; bodies are not stored. If you want stronger guarantees, the source is small (~250 lines) and trivial to fork and run on your own Cloudflare account.

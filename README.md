# claudemetry

A Cloudflare Worker, written in Rust, that proxies requests to the Anthropic API and records a structured row for every transaction into a **per-user SQLite database** (Durable Object with native SQL storage). Point `ANTHROPIC_BASE_URL` at it and every Claude Code (or Anthropic SDK) request becomes queryable, per account, forever.

Full passthrough: method, path, query, headers, and body are forwarded to `https://api.anthropic.com` unchanged. Streaming responses (SSE) are preserved — the client receives bytes immediately while the proxy captures the full body in the background via `ctx.wait_until`.

## Architecture

```
    Claude Code                                    api.anthropic.com
         │                                                ▲
         │ POST /v1/messages                              │ unchanged
         ▼                                                │
  ┌──────────────────────────────────────────────────────────┐
  │  claudemetry-api (Rust Worker)                           │
  │                                                          │
  │   identify    →  GET /oauth/profile   (KV cache, 1h TTL) │
  │                  user_hash = sha256(salt ‖ email)[:8]    │
  │                                                          │
  │   ── before upstream fetch ────────────────────────────  │
  │   stub.fetch("/ingest/start")  placeholder row,          │
  │                                in_flight=1, spinner UI   │
  │                                                          │
  │   forward     →  streaming response  ─────────────────► client
  │                                                          │
  │   ── after upstream fetch, in ctx.wait_until ──────────  │
  │   parse SSE usage + text_delta                           │
  │   stub.fetch("/ingest/finalize")  real metrics +         │
  │                                   user_text + asst_text  │
  │   embed(user+asst) → VECTORIZE.upsert({hash}:{tx_id})    │
  └──────────────────────────────────────────────────────────┘
                       │                              │
                       ▼                              ▼
      ┌─────────────────────────────────┐  ┌─────────────────────────┐
      │  UserStore Durable Object       │  │  Vectorize              │
      │  (one instance per user_hash)   │  │  (shared index,         │
      │                                 │  │   namespace=user_hash)  │
      │  transactions + FTS5 virtual    │  │                         │
      │    table (triggers mirror       │  │  claudemetry-turns,     │
      │    user_text + assistant_text)  │  │  bge-base-en-v1.5       │
      │  search_rate_limit counter      │  │  768-dim cosine         │
      │  session_ends                   │  │                         │
      │                                 │  │  search queries scoped  │
      │  POST /search orchestrates      ├─►│  to namespace=user_hash │
      │  FTS + VECTORIZE.query + RRF    │  │  topK → tx_ids          │
      └─────────────────────────────────┘  └─────────────────────────┘
```

Each unique user_hash gets its own private SQLite database. No provisioning —
the first request with a given hash materializes the DO, runs
`CREATE TABLE IF NOT EXISTS`, and inserts the row. Two users share no table,
no index, no memory space.

Vectorize uses a single shared index with per-user isolation at the namespace
level — every upsert and query carries `namespace=<user_hash>`, so a query
physically cannot see vectors outside the caller's namespace. The id prefix
`{user_hash}:` is retained as a uniqueness guard within the index. FTS5 is
trivially isolated (lives inside each user's DO); the two indexes complement
each other.

## Requirements

Toolchain versions are pinned in `mise.toml`. With [mise](https://github.com/jdx/mise) installed and activated, bootstrap a fresh clone with:

```bash
mise trust          # one-time, approves this repo's mise.toml
just setup-mise     # installs rust/node/pnpm/just, wasm target, and worker-build
```

That's everything needed to run `just local` or `just deploy-all`. `worker-build` is a cargo-installed binary (lives in `~/.cargo/bin`, not pinned in `mise.toml`) — `mise.toml`'s `[env]` block puts `~/.cargo/bin` on PATH whenever you're inside the repo, so the setup is fully portable and doesn't rely on your global shell config.

Without mise, install the equivalents manually: Rust (with `wasm32-unknown-unknown`), Node ≥22.12, pnpm, [`just`](https://github.com/casey/just), and `cargo install worker-build`.

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
just login         # one-time, opens browser
just deploy-all    # both workers (or: just deploy-api / just deploy-frontend)
```

By default the worker is reachable at `https://claudemetry-api.<your-subdomain>.workers.dev`. To use a custom domain, either uncomment the `routes` block in `wrangler.toml` (requires the zone to be on the same Cloudflare account) or bind the domain in the Cloudflare dashboard under Workers → claudemetry-api → Settings → Domains & Routes.

Before sending traffic, generate and install a per-deployment salt so hashes aren't portable across deployments:

```bash
openssl rand -hex 16 | npx wrangler secret put HASH_SALT
```

If unset, the worker falls back to a published dev salt and emits hashes that aren't safe to publish.

Both workers share a single KV namespace (binding name `SESSION`) for the OAuth-profile cache (`tok:<token_id>`) and the email→hash link (`link:<email>`). Create one and paste the id into both `wrangler.toml` and `dashboard/wrangler.jsonc`:

```bash
npx wrangler kv namespace create claudemetry-session
# copy the id into the [[kv_namespaces]] block in both configs
```

Provision the Vectorize index used for semantic search. This is idempotent
and requires a CF API token with the `Vectorize: Edit` permission:

```bash
just vectorize-create       # creates claudemetry-turns (768-dim cosine)
                            # + user_hash metadata index
```

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

Every request carries either an OAuth Bearer token (the usual Claude Code case) or an `x-api-key`. The proxy resolves both to a stable identity:

```
if   Bearer (OAuth)  →  GET https://api.anthropic.com/api/oauth/profile
                         (cached in KV under tok:<token_id>, TTL 1h)
                         user_hash = hex(sha256(SALT ‖ "email:" ‖ account.email))[:16]

if   x-api-key       →  user_hash = hex(sha256(SALT ‖ "apikey:" ‖ key))[:16]

else                  →  skip persistence
```

The profile endpoint returns both `account.uuid` and `account.email`; the hash derives from email because email is the same identity the dashboard's Google SSO / CF Access layer sees — no separate linking table needed. Email is stable across OAuth refreshes, devices, and months of use; the first request after a token refresh costs one ~150 ms profile fetch, everything after hits the KV cache. Two Anthropic accounts always produce two different hashes (and two different Durable Objects). The raw bearer is never stored or logged.

As a side-effect, every resolved request writes `link:<email> = user_hash` into the same KV namespace. The dashboard (gated by Cloudflare Access on the same email) reads that link to auto-scope itself to your DO — no `/setup` page, no copy-pasting.

If the profile endpoint is unreachable (transient outage, rate limit), the proxy falls back to a `raw:`-prefixed token hash. That hash will diverge on the next OAuth refresh, but self-heals as soon as a profile fetch succeeds again.

## Persistence: the `transactions` table

Each user's private SQLite contains a single table (migrations are idempotent, applied on first access to the DO):

| column                   | type    | source                                                              |
|--------------------------|---------|---------------------------------------------------------------------|
| `tx_id`                  | TEXT PK | Synthetic `inflight-<ts>-<rand>` assigned at request arrival        |
| `ts`                     | INT     | Request arrival time (ms since epoch)                               |
| `session_id`             | TEXT    | `x-claude-code-session-id` header                                   |
| `method`                 | TEXT    | HTTP method                                                          |
| `url`                    | TEXT    | Full upstream URL hit                                               |
| `status`                 | INT     | HTTP status returned by Anthropic                                    |
| `elapsed_ms`             | INT     | Total proxy-to-upstream-and-back latency                            |
| `model`                  | TEXT    | Model used, from `message_start`                                     |
| `input_tokens`           | INT     | Parsed from final usage snapshot                                     |
| `output_tokens`          | INT     | Parsed from final usage snapshot                                     |
| `cache_read`             | INT     | Prompt-cache reads                                                   |
| `cache_creation`         | INT     | Prompt-cache writes                                                  |
| `stop_reason`            | TEXT    | `end_turn`, `tool_use`, `max_tokens`, etc.                           |
| `tools_json`             | TEXT    | JSON array of tool names invoked in the turn                         |
| `req_body_bytes`         | INT     | Size of the forwarded request body                                  |
| `resp_body_bytes`        | INT     | Size of the captured response body                                  |
| `in_flight`              | INT     | 1 between `/ingest/start` and `/ingest/finalize`; 0 once done        |
| `anthropic_message_id`   | TEXT    | Anthropic's `message.id` (set on finalize)                           |
| `user_text`              | TEXT    | Last `user` message's text blocks + `tool_result` content (finalize) |
| `assistant_text`         | TEXT    | Concatenated `text_delta` from the SSE stream (finalize)             |

Indexed on `ts DESC`, `(session_id, ts)`, and a partial index on `ts` where `in_flight = 1` (backs the orphan sweep). An FTS5 virtual table (`transactions_fts`) mirrors `user_text` + `assistant_text` via sync triggers, and finalized turns are also embedded into a shared Vectorize index (`claudemetry-turns`, 768-dim, cosine) for semantic search — see **Search** below.

Each request writes the row twice. A placeholder (`in_flight = 1`, all metrics zero) is
inserted into the DO as soon as the request arrives, so the dashboard can show a spinner
while the upstream is still streaming. When the response completes, `/ingest/finalize`
overwrites the row with the real metrics and flips `in_flight` to 0. If the worker is
evicted between the two writes, the next fresh DO instance sweeps any placeholder older
than 5 min to `stop_reason = 'error'`.

### Search: `/_cm/search`

Hybrid FTS5 + Vectorize lookup over `user_text` and `assistant_text`. `mode`
defaults to `hybrid`; results from both indexes are merged via reciprocal-rank
fusion (k=60) and `match_source` tags each hit as `fts`, `vector`, or `both`.
Orchestration lives in the per-user DO (`UserStore::search`), so the proxy's
`/_cm/search` and the dashboard's `/api/search` are both thin forwarders over
the same implementation.

```bash
curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/search \
  -d '{"q":"parse_sse_usage","mode":"fts"}'

curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/search \
  -d '{"q":"the OAuth debugging session","mode":"vector"}'

curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/search \
  -d '{"q":"auth flow","mode":"hybrid","limit":20}'
```

Response shape (success):

```json
{
  "mode": "hybrid",
  "results": [
    {
      "tx_id": "inflight-…",
      "ts": 1776251781393,
      "session_id": "…",
      "model": "claude-opus-4-7",
      "user_snip": "…<mark>foo</mark>…",
      "asst_snip": "…<mark>bar</mark>…",
      "score": 0.033,
      "match_source": "both"
    }
  ]
}
```

Isolation for Vectorize is enforced two ways: each vector is keyed
`{user_hash}:{tx_id}` and its `user_hash` metadata is used as a server-side
`filter` on every query. FTS5 is trivially isolated — it lives inside each
user's own Durable Object.

**Rate limit**: 120 req/min per user (fixed 60s window). Counter lives in the
DO's SQLite (`search_rate_limit` table), so it's strongly consistent without
a KV round-trip. Over-quota responses are `429` with
`{"error":"rate_limited","retry_after_seconds":N}`. The write-path embed (one
call per turn during `/ingest/finalize`) is **not** rate-limited — turns are
naturally paced by the upstream Anthropic API.

**Dashboard UI**: the dashboard (behind CF Access) has a command palette that
calls `/api/search` and renders results with FTS5 `<mark>` snippet highlights
and a keyword/semantic/both badge per hit. Open it from anywhere with `/` or
`Cmd/Ctrl+K`; arrows navigate hits, Enter opens the selected turn, Esc closes.

Provision the Vectorize index once before first deploy:

```bash
just vectorize-create        # idempotent; creates index + user_hash metadata index
just vectorize-info          # sanity check
```

## Admin probes

A handful of endpoints let you verify the pipeline without a dashboard. All are scoped to **your own** `user_hash` (resolved from your bearer / api-key) unless explicitly overridden:

```bash
# Your stable user_hash for this deployment
curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/whoami

# Aggregate counts + token totals from your DO's SQLite
curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/stats

# All raw rows from your DO (newest first)
curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/recent

# Full-text + semantic search over your user_text / assistant_text.
# mode = fts | vector | hybrid (default). 120 req/min per user.
curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/search \
  -d '{"q":"parse_sse_usage","mode":"hybrid","limit":20}'

# Full record for one transaction — all columns including the untruncated
# user_text + assistant_text. Backs `burnage turn <tx_id>`.
curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/turn \
  -d '{"tx_id":"inflight-1776251872077-c68abdc2"}'

# Generic SQL exec — backs `burnage shell`. Optional `hash` overrides the
# target DO (used for cross-DO inspection / data migration).
curl -H "Authorization: Bearer <token>" https://your-proxy/_cm/admin/sql \
  -d '{"sql":"SELECT model, COUNT(*) AS n FROM transactions GROUP BY model"}'
```

These are bearer-gated and never cross identities unless you ask via `hash`.

## `burnage` CLI

Thin cross-platform CLI over the `/_cm/*` endpoints, installed via
`just burnage-install` (bakes your `$DOMAIN` in as the default `--url`).

```bash
burnage whoami                # your stable user_hash + email
burnage stats                 # aggregate counts + token totals
burnage recent                # recent transactions (newest first)
```

### `burnage search`

Headless wrapper over `/_cm/search`. Output auto-detects: styled table on a
tty (with `<mark>` snippet highlights rendered as bold yellow ANSI), raw JSON
when stdout is piped.

```bash
burnage search "parse_sse_usage"                 # hybrid (default)
burnage search "auth flow" --mode fts            # keyword-only (bm25)
burnage search "OAuth debugging" --mode vector   # semantic-only (cosine)
burnage search "foo" --limit 50 --format json    # override table auto-detect
burnage search "foo" -v                          # show tx_id + score + both snippets
```

Default layout is two lines per hit: a header (`match_source` badge · model
· relative time) and one snippet — prefers `asst_snip` (usually the
substantive response), falls back to `user_snip`. Box-drawing characters
(`├ ─ │ ┤ ┌ ┐ └ ┘ ┬ ┴ ┼` etc.) are stripped at **display time only**, so
replayed tool output doesn't drown the snippet — the underlying FTS5 index
still contains them, so you can still search *for* ASCII tables if needed.

`-v / --verbose` restores the detailed layout: adds the RRF score on line 1,
shows both snippets when both have content, and prints a `tx_id` + `sess`
footer suitable for piping into `burnage turn`.

### `burnage turn`

Dump one transaction's **full record** — all columns, untruncated
`user_text` + `assistant_text`, metadata header (model, UTC timestamp,
elapsed, token counts, stop reason, tools, session id, inflight state,
Anthropic `message.id`). Useful when `burnage search` has located a row and
you want to read the whole thing.

```bash
# from a search result, copy the tx_id and dump it:
burnage search "that auth bug" -v
burnage turn inflight-1776251872077-c68abdc2

# or pipe-friendly:
burnage turn <tx_id> --format json | jq '.assistant_text'
```

Text blocks preserve their original line breaks with a 2-space indent — no
wrapping, since terminals already wrap and altering content would break
`grep` over the output.

### `burnage quota`

One combined view of your claudemetry deployment, zooming out:

1. **Your DO** — turns, storage (vs the 5 GiB/DO cap), token totals, payload
   bytes, active window. Always shown; hits the proxy.
2. **Cloudflare account totals** — Workers requests/CPU, DO invocations + SQL
   storage, build minutes, each vs the Workers Paid allocation, over the
   selected window.
3. **Vectorize index** — vector count vs the 5M cap, dims, last mutation.

The CF-backed sections (2) and (3) are best-effort — they print a one-line
hint when `CF_API_TOKEN` + `CF_ACCOUNT_ID` aren't set, so the DO section
still renders usefully on its own.

```bash
burnage quota                 # default window: 30d
burnage quota month           # calendar month-to-date (UTC)
burnage quota 24h             # last 24h window for CF totals
```

### `burnage session`

```bash
burnage session end <id>      # mark a session as ended (sets ended_at)
burnage session ends          # list all recorded session end timestamps
```

### `burnage shell`

Interactive SQL REPL + headless executor over `/_cm/admin/sql`. Built on
crossterm — no readline, comfy-table, or rustyline pull-ins.

```bash
burnage shell                          # REPL against your own DO
burnage shell --hash 0203addab2792724  # REPL against another DO (admin)
burnage shell -c "SELECT COUNT(*) FROM transactions"
burnage shell -f migrate.sql
echo "SELECT model FROM transactions" | burnage shell
```

Output auto-detects: pretty table on a tty, JSON when stdout is piped. Override
with `--format {table,json,tsv}`.

REPL niceties: history at `~/.cache/burnage/shell_history`, arrow-key
navigation, Home/End/Ctrl-A/E/U/K/L, Ctrl-C cancels current input, Ctrl-D
exits on empty buffer. Dot commands: `.tables`, `.schema [name]`,
`.hash <16-hex>|-`, `.whoami`, `.quit`.

## What gets logged (console)

Each transaction emits two structured JSON log lines to `wrangler tail`:

```json
{"dir":"req","ts":...,"method":"POST","url":"...","user_hash":"0203…","session_id":"…","body_len":1530}
{"dir":"resp","ts":...,"status":200,"elapsed_ms":777,"model":"claude-opus-4-7","input_tokens":443,"output_tokens":32,"cache_read":262754,"cache_creation":951,"stop_reason":"end_turn","tx_id":"inflight-…","body_len":2472}
```

Best-effort operations on the finalize path (embedding + Vectorize upsert)
log their failure modes with a `stage` field so silent failures surface
immediately in `wrangler tail`:

```json
{"dir":"embed_err","stage":"binding","err":"…"}                   // env.AI not bound
{"dir":"embed_err","stage":"run","err":"AiError: 5006: …"}        // Workers AI rejection
{"dir":"embed_err","stage":"shape","body":{…}}                    // unexpected response shape
{"dir":"embed_err","stage":"dims","got":384,"want":768}           // dimension mismatch
{"dir":"vectorize_upsert_err","err":"…"}                          // VECTORIZE.upsert failed
```

`authorization` and `x-api-key` values are never written to logs. The raw
prompt/response bodies aren't logged either — they're parsed for metrics (and
for the `user_text` / `assistant_text` search columns) and then discarded.

## Layout

- `src/lib.rs` — fetch handler, two-phase ingest (`/ingest/start` + `/finalize`), `UserStore` Durable Object with FTS5 + RRF search orchestrator, SSE parser, Vectorize wrapper (via `js_sys::Reflect` since worker 0.8 has no first-class binding), user-hash derivation, admin probes, `/_cm/search`, `/_cm/turn`
- `wrangler.toml` — proxy worker config: DO + SQLite migration, `[ai]` binding, `[[vectorize]]` binding for the `claudemetry-turns` index, observability on
- `burnage/` — cross-platform CLI (`whoami`, `stats`, `recent`, `search`, `quota`, `session`, `shell`)
- `dashboard/` — Astro 6 + React dashboard worker, served behind Cloudflare Access
  - `src/pages/api/search.ts` — thin forwarder: CF Access email → `user_hash` → DO `/search`
  - `src/components/CommandPalette.tsx` — `/` or `Cmd/Ctrl+K` palette, mode switch, RRF-merged results (hotkey wiring lives in `Sidebar.tsx`)
- `scripts/cf-access.sh` — idempotent provisioner for the Access apps/policies (`just cf-access`)
- `justfile` — `local`, `local-tee`, `build`, `login`, `deploy-api`, `tail`, `clean`, `dashboard-dev`, `deploy-frontend`, `dashboard-tail`, `deploy-all`, `cf-access`, `vectorize-create`, `vectorize-info`, `burnage-install`

## Notes on trust

The proxy does not authenticate callers. Anyone with the deployed URL can use it as an open relay to Anthropic (they still need their own Anthropic credentials, which are forwarded as-is). If that matters for your deployment, add a shared-secret header check at the top of `fetch` in `src/lib.rs`, or put the worker behind Cloudflare Access.

The operator of the proxy has in-memory access to your raw token and prompt bodies while a request is in flight — this is unavoidable for any proxy. The code persists parsed metrics **plus the text of the last user-turn message (including `tool_result` payloads) and the assistant's text output**, so that `/_cm/search` (FTS5 + Vectorize) is useful. `tool_result` blocks often contain file contents or command output that your assistant saw — treat the DO's SQLite as sensitive accordingly. If you want stronger guarantees, the source is small and trivial to fork and run on your own Cloudflare account; disabling search is a one-line change (skip the `user_text` / `assistant_text` fields in `TransactionRecord`).

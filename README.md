# cc-proxy

A Cloudflare Worker, written in Rust, that proxies requests to the Anthropic API and logs every request and response. Point `ANTHROPIC_BASE_URL` at it (locally or deployed) to see exactly what Claude Code — or any Anthropic SDK client — sends on the wire.

Full passthrough: method, path, query, headers, and body are forwarded to `https://api.anthropic.com` unchanged. Streaming responses (SSE) are preserved — the client gets bytes immediately while the proxy captures the full body in the background via `ctx.wait_until`.

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

Point your client at whichever URL you ended up with:

```bash
export ANTHROPIC_BASE_URL=https://your-worker-url
```

View live logs with:

```bash
just tail
```

or in the Cloudflare dashboard (observability + traces are enabled in `wrangler.toml`).

### Per-user observability (multi-tenant)

Every log line includes a stable `user_hash` derived from a salted SHA-256 of the OAuth JWT `sub` claim (or the raw `x-api-key` for API-key clients). The hash survives token rotations because the `sub` is a per-account identifier that doesn't change when the Bearer wrapper refreshes.

Set the salt as a Cloudflare secret before going live so identities are scoped to your deployment:

```bash
openssl rand -hex 16 | npx wrangler secret put HASH_SALT
```

If unset, the worker falls back to a known dev salt and emits hashes that aren't safe to publish.

## What gets logged

Each transaction produces two JSON lines — one when the request is received, one when the response finishes:

```json
{"dir":"req","ts":...,"method":"POST","url":"https://api.anthropic.com/v1/messages?beta=true","headers":{...},"body":"...","body_len":1530}
{"dir":"resp","ts":...,"status":200,"elapsed_ms":777,"headers":{...},"body":"...","body_len":1572}
```

`authorization` and `x-api-key` header values are redacted in the logs. Everything else — including request bodies and full streamed response bodies — is captured verbatim.

Typical Claude Code request headers reveal:

- `user-agent: claude-cli/<version>`
- `x-claude-code-session-id: <uuid>`
- `x-stainless-*` SDK/runtime telemetry (package version, Node version, OS, arch)
- `anthropic-version`, `anthropic-beta` (active beta feature flags)

Request bodies expose the full prompt: `model`, `messages`, `system`, `tools`, `metadata`, sampling params.

## Layout

- `src/lib.rs` — the `#[event(fetch)]` handler
- `wrangler.toml` — worker config (build via `worker-build`, observability on)
- `justfile` — `local`, `local-tee`, `build`, `login`, `deploy`, `tail`, `clean`

## Notes

This proxy does not authenticate callers. Anyone with the deployed URL can use it as an open relay to Anthropic (they still need their own API key, which is forwarded as-is). If that's not what you want, add a shared-secret header check at the top of `fetch` in `src/lib.rs`, or put the worker behind Cloudflare Access.

use js_sys::{Array, Reflect, Uint8Array};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use worker::durable::State;
use worker::kv::KvStore;
use worker::*;

const UPSTREAM: &str = "https://api.anthropic.com";
const DEFAULT_SALT: &str = "burnage-dev-unset";

// ---------- Top-level fetch (proxy) ----------

#[event(fetch)]
async fn fetch(mut req: Request, env: Env, ctx: Context) -> Result<Response> {
    let start = Date::now().as_millis() as i64;
    let method = req.method();
    let url = req.url()?;
    let path = url.path().to_string();
    let query = url.query().map(|q| format!("?{}", q)).unwrap_or_default();
    let target = format!("{}{}{}", UPSTREAM, path, query);

    let salt = env
        .secret("HASH_SALT")
        .map(|s| s.to_string())
        .unwrap_or_else(|_| DEFAULT_SALT.to_string());

    let req_headers_vec: Vec<(String, String)> = req.headers().entries().collect();

    // Internal admin probes — bypass the proxy. Caller must present a Bearer
    // token / api key; we only ever query their own user_hash's DO.
    if path.starts_with("/_cm/") {
        let body = req.bytes().await.unwrap_or_default();
        return admin_route(&path, &query, &method, &body, &req_headers_vec, &salt, &env).await;
    }

    let req_body_bytes = req.bytes().await.unwrap_or_default();
    let req_body_bytes = inject_thinking_display(&req_body_bytes).unwrap_or(req_body_bytes);
    let req_body_len = req_body_bytes.len() as i64;

    // The /v1/* proxy path is the ONLY place that registers the caller as
    // "known to this deployment" (link:<email> → user_hash in KV). Admin
    // routes (/_cm/*) check that link to decide whether to respond, so a
    // stranger with a valid Anthropic token can't hit /_cm/user-count or
    // anything else until they've first proxied real traffic here.
    let identity = compute_user_hash(&req_headers_vec, &salt, &env).await;
    if let Some((hash, Some(email), _)) = &identity {
        if let Ok(kv) = env.kv("SESSION") {
            auto_link(&kv, email, hash).await;
        }
    }
    let user_hash = identity.map(|(h, _email, _legacy)| h);
    let session_id = header_value(&req_headers_vec, "x-claude-code-session-id");

    // Quick request log line (kept for tail visibility).
    let req_log = json!({
        "ts": start,
        "dir": "req",
        "user_hash": user_hash,
        "session_id": session_id,
        "method": method.to_string(),
        "url": target,
        "body_len": req_body_len,
    });
    console_log!("{}", req_log.to_string());

    // Build the outbound request: copy headers minus the hop-by-hop / CF noise.
    let out_headers = Headers::new();
    for (k, v) in &req_headers_vec {
        let lk = k.to_ascii_lowercase();
        if matches!(
            lk.as_str(),
            "host"
                | "content-length"
                | "cf-connecting-ip"
                | "cf-ipcountry"
                | "cf-ray"
                | "cf-visitor"
                | "x-forwarded-for"
                | "x-forwarded-proto"
                | "x-real-ip"
        ) {
            continue;
        }
        out_headers.append(k, v).ok();
    }

    let mut init = RequestInit::new();
    init.with_method(method.clone());
    init.with_headers(out_headers);
    if !req_body_bytes.is_empty() {
        let arr = Uint8Array::from(&req_body_bytes[..]);
        init.with_body(Some(arr.into()));
    }

    // Synthetic, stable row PK. Generated at request arrival rather than
    // using Anthropic's `message.id` (which only lands partway through the
    // SSE stream) so the dashboard key stays stable across the turn_start
    // → turn_complete WS lifecycle. The real `message.id` goes into
    // `anthropic_message_id` on finalize.
    let tx_id = format!(
        "tx-{}-{:08x}",
        start,
        js_sys::Math::random().to_bits() as u32
    );

    // Stub is !Clone, so we acquire one per wait_until branch. Cheap —
    // it's just wasm-bindgen handle lookups, not a round-trip.
    let acquire_stub = || -> Option<worker::durable::Stub> {
        let uh = user_hash.as_ref()?;
        let ns = env.durable_object("USER_STORE").ok()?;
        let id = ns.id_from_name(uh).ok()?;
        id.get_stub().ok()
    };
    let broadcast_stub = acquire_stub();
    let ingest_stub = acquire_stub();

    // Broadcast turn-start to connected WS clients so the dashboard can
    // show a spinner. This is NOT a database write — if the worker dies,
    // the virtual in-flight row simply disappears from the client. No
    // orphaned placeholder rows.
    if let Some(stub) = broadcast_stub {
        let start_tx = tx_id.clone();
        let start_session = session_id.clone();
        let start_body = req_body_bytes.clone();
        ctx.wait_until(async move {
            let ParsedRequest {
                model,
                max_tokens,
                thinking_budget,
                tools_json: _,
                tool_choice,
                user_text,
            } = parse_request_body(&start_body);
            let payload = json!({
                "tx_id": start_tx,
                "ts": start,
                "session_id": start_session,
                "model": model,
                "tool_choice": tool_choice,
                "user_text": user_text,
                "thinking_budget": thinking_budget,
                "max_tokens": max_tokens,
            });
            post_json_to_do(&stub, "/ws/turn-start", &payload).await;
        });
    }

    let upstream_req = Request::new_with_init(&target, &init)?;
    let mut resp = Fetch::Request(upstream_req).send().await?;

    // Clone for out-of-band consumption; client-bound stream stays untouched.
    let resp_for_log = resp.cloned()?;
    let status = resp.status_code() as i32;
    let method_str = method.to_string();
    let target_for_record = target.clone();
    let resp_headers_vec: Vec<(String, String)> = resp.headers().entries().collect();
    let req_body_for_parse = req_body_bytes.clone();

    ctx.wait_until(async move {
        let mut r = resp_for_log;
        let body = r.bytes().await.unwrap_or_default();
        let body_str = String::from_utf8_lossy(&body);
        let stats = parse_sse_usage(&body_str);
        let elapsed = Date::now().as_millis() as i64 - start;

        let anthropic_message_id = stats.tx_id.clone();
        let tools_json = if stats.tools.is_empty() {
            None
        } else {
            serde_json::to_string(&stats.tools).ok()
        };

        // Request-side knobs. Cheap to parse — the body is already in memory.
        // `model`/`tools` from the request are only used for the inflight
        // placeholder; here at finalize the SSE-derived values (resolved
        // model, tools actually invoked) are authoritative.
        let ParsedRequest {
            model: _,
            max_tokens,
            thinking_budget,
            tools_json: _,
            tool_choice: _,
            user_text,
        } = parse_request_body(&req_body_for_parse);
        let assistant_text = if stats.assistant_text.is_empty() {
            None
        } else {
            Some(stats.assistant_text.clone())
        };
        let thinking_text = if stats.thinking_text.is_empty() {
            None
        } else {
            Some(stats.thinking_text.clone())
        };
        let tool_calls_json = if stats.tool_calls.is_empty() {
            None
        } else {
            serde_json::to_string(&stats.tool_calls).ok()
        };
        let (rl_req_remaining, rl_req_limit, rl_tok_remaining, rl_tok_limit) =
            parse_rate_limits(&resp_headers_vec);

        let record = TransactionRecord {
            tx_id,
            ts: start,
            session_id,
            method: method_str,
            url: target_for_record,
            status,
            elapsed_ms: elapsed,
            model: stats.model,
            input_tokens: stats.input_tokens,
            output_tokens: stats.output_tokens,
            cache_read: stats.cache_read,
            cache_creation: stats.cache_creation,
            stop_reason: stats.stop_reason,
            tools_json,
            req_body_bytes: req_body_len,
            resp_body_bytes: body.len() as i64,
            cache_creation_5m: if stats.cache_creation_5m > 0 {
                Some(stats.cache_creation_5m)
            } else {
                None
            },
            cache_creation_1h: if stats.cache_creation_1h > 0 {
                Some(stats.cache_creation_1h)
            } else {
                None
            },
            thinking_budget,
            thinking_blocks: if stats.thinking_blocks > 0 {
                Some(stats.thinking_blocks)
            } else {
                None
            },
            max_tokens,
            rl_req_remaining,
            rl_req_limit,
            rl_tok_remaining,
            rl_tok_limit,
            anthropic_message_id,
            user_text,
            assistant_text,
            thinking_text,
            tool_calls_json,
        };

        // Always emit a structured response log so wrangler tail still works.
        let log = json!({
            "ts": Date::now().as_millis() as i64,
            "dir": "resp",
            "user_hash": user_hash.clone(),
            "session_id": record.session_id,
            "tx_id": record.tx_id,
            "status": status,
            "elapsed_ms": elapsed,
            "model": record.model,
            "input_tokens": record.input_tokens,
            "output_tokens": record.output_tokens,
            "cache_read": record.cache_read,
            "cache_creation": record.cache_creation,
            "stop_reason": record.stop_reason,
            "tools": record.tools_json,
            "body_len": record.resp_body_bytes,
        });
        console_log!("{}", log.to_string());

        if let Some(stub) = ingest_stub {
            post_record_to_do(&stub, "/ingest", &record).await;

            // Best-effort Vectorize upsert. Failures are logged but never
            // block the SQLite finalize, which already succeeded above.
            if let Some(uh) = user_hash.as_deref() {
                let combined = format!(
                    "{}\n---\n{}\n---\n{}",
                    record.user_text.as_deref().unwrap_or(""),
                    record.assistant_text.as_deref().unwrap_or(""),
                    record.thinking_text.as_deref().unwrap_or(""),
                );
                if combined.trim().len() > 3 {
                    match embed_text(&env, &combined).await {
                        Some(vec) => {
                            if let Err(e) = vectorize_upsert(
                                &env,
                                uh,
                                &record.tx_id,
                                record.session_id.as_deref(),
                                record.ts,
                                &vec,
                            )
                            .await
                            {
                                console_log!(
                                    "{{\"dir\":\"vectorize_upsert_err\",\"err\":\"{}\"}}",
                                    e
                                );
                            }
                        }
                        None => {
                            console_log!(
                                "{{\"dir\":\"embed_err\",\"tx_id\":\"{}\"}}",
                                record.tx_id
                            );
                        }
                    }
                }
            }
        }
    });

    Ok(resp)
}

// ---------- Admin probes ----------

async fn admin_route(
    path: &str,
    query: &str,
    method: &Method,
    body: &[u8],
    headers: &[(String, String)],
    salt: &str,
    env: &Env,
) -> Result<Response> {
    let (user_hash, email, legacy_hash) = match compute_user_hash(headers, salt, env).await {
        Some(t) => t,
        None => return Response::error("missing authorization", 401),
    };

    // Proxy-first gate: every /_cm/* response requires that the caller has
    // previously sent at least one /v1/* request through this deployment
    // (which writes `link:<email>` as a side-effect). Without that, an
    // attacker holding any valid Anthropic token could still hit admin
    // routes and see counts / try cross-DO reads. With it, the attacker
    // also needs to have previously used this specific proxy — at which
    // point they've already revealed themselves and their own data is
    // the only thing at risk. api-key callers (no email) get no link and
    // are therefore locked out of /_cm/* entirely.
    let is_registered = match email.as_deref() {
        Some(em) => caller_has_link(env, em).await,
        None => false,
    };
    if !is_registered {
        return Response::error(
            "proxy-first access required: send a /v1/* request through this deployment first",
            403,
        );
    }

    if path == "/_cm/whoami" {
        return Response::from_json(&json!({
            "user_hash": user_hash,
            "email": email,
            "legacy_hash": legacy_hash,
        }));
    }

    // "How many users does this deployment serve?" — counted by distinct
    // `link:<email>` entries in KV. This is the honest count: every
    // successfully-resolved OAuth request writes its email's link, so the
    // cardinality of that prefix is exactly "users with at least one
    // ingested turn." Avoids the CF-analytics-objectId count burnage was
    // using, which inflated with ghost DOs.
    if path == "/_cm/user-count" && method == &Method::Get {
        let kv = match env.kv("SESSION") {
            Ok(kv) => kv,
            Err(_) => return Response::error("SESSION KV not bound", 500),
        };
        let mut total: u64 = 0;
        let mut cursor: Option<String> = None;
        loop {
            let mut builder = kv.list().prefix("link:".to_string()).limit(1000);
            if let Some(c) = cursor.take() {
                builder = builder.cursor(c);
            }
            let resp = match builder.execute().await {
                Ok(r) => r,
                Err(e) => {
                    return Response::error(format!("kv list: {e:?}"), 500);
                }
            };
            total += resp.keys.len() as u64;
            if resp.list_complete {
                break;
            }
            match resp.cursor {
                Some(c) if !c.is_empty() => cursor = Some(c),
                _ => break,
            }
        }
        return Response::from_json(&json!({ "users": total }));
    }

    // One-shot merge of the caller's pre-email-flip DO (uuid-keyed) into
    // their current (email-keyed) DO. Idempotent — re-running copies nothing
    // new. Source DO's data is dropped after a successful copy so the
    // legacy DO stops showing up in CF analytics.
    if path == "/_cm/admin/migrate-legacy" && method == &Method::Post {
        let Some(legacy) = legacy_hash.as_ref() else {
            return Response::error(
                "no legacy hash available (api-key auth or token-cache miss)",
                400,
            );
        };
        if legacy == &user_hash {
            return Response::from_json(&json!({
                "copied_transactions": 0,
                "copied_session_ends": 0,
                "note": "legacy hash == current hash; no-op",
            }));
        }
        return handle_migrate_legacy(&user_hash, legacy, env).await;
    }

    // /_cm/admin/sql supports a hash override in the body for cross-DO
    // inspection + data migration. Everything else operates on the caller's
    // own DO.
    let mut target_hash = user_hash.clone();
    let mut forwarded_body: Vec<u8> = body.to_vec();

    // Search is multi-stage (FTS + Vectorize + merge) and needs env.AI +
    // env.VECTORIZE in addition to the caller's DO, so we handle it here
    // rather than in the flat DO passthrough table below.
    if path == "/_cm/search" && method == &Method::Post {
        return handle_search(&user_hash, &forwarded_body, env).await;
    }

    if path == "/_cm/admin/vectorize-backfill" && method == &Method::Post {
        return handle_vectorize_backfill(&user_hash, &forwarded_body, env).await;
    }

    let (inner_method, inner_path) = match (method, path) {
        (&Method::Get, "/_cm/recent") => (Method::Get, "/recent"),
        (&Method::Get, "/_cm/stats") => (Method::Get, "/stats"),
        (&Method::Get, "/_cm/sessions/ends") => (Method::Get, "/session/ends"),
        (&Method::Get, "/_cm/sessions/summary") => (Method::Get, "/sessions/summary"),
        (&Method::Get, "/_cm/session/turns") => (Method::Get, "/session/turns"),
        (&Method::Post, "/_cm/session/end") => (Method::Post, "/session/end"),
        (&Method::Post, "/_cm/turn") => (Method::Post, "/turn"),
        (&Method::Post, "/_cm/admin/sql") => {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&forwarded_body) {
                if let Some(h) = v.get("hash").and_then(|x| x.as_str()) {
                    if !is_hex16(h) {
                        return Response::error("hash must be 16 hex chars", 400);
                    }
                    // Cross-DO queries are gated by the ADMIN_EMAILS secret.
                    // Caller's own hash is always allowed (no privilege gain).
                    // Anything else requires the caller's email to be in the
                    // allowlist. Without ADMIN_EMAILS set, cross-DO reads are
                    // forbidden outright — safe default.
                    if h != user_hash && !is_admin_email(email.as_deref(), env) {
                        return Response::error(
                            "cross-DO admin/sql requires your email in ADMIN_EMAILS",
                            403,
                        );
                    }
                    target_hash = h.to_string();
                }
                // Re-serialize without the `hash` field so the DO sees a
                // clean `{sql, params}` body.
                if let Some(obj) = v.as_object() {
                    let mut trimmed = obj.clone();
                    trimmed.remove("hash");
                    forwarded_body =
                        serde_json::to_vec(&trimmed).unwrap_or(forwarded_body);
                }
            }
            (Method::Post, "/sql")
        }
        _ => return Response::error("unknown admin route", 404),
    };

    let ns = env.durable_object("USER_STORE")?;
    let id = ns.id_from_name(&target_hash)?;
    let stub = id.get_stub()?;

    // Preserve query string so the DO handler can read ?since= / ?id= / ?limit=.
    let inner_url = format!("https://store{}{}", inner_path, query);
    let mut init = RequestInit::new();
    init.with_method(inner_method);
    if !forwarded_body.is_empty() {
        let arr = Uint8Array::from(&forwarded_body[..]);
        init.with_body(Some(arr.into()));
        let h = Headers::new();
        h.append("content-type", "application/json").ok();
        init.with_headers(h);
    }
    let req = Request::new_with_init(&inner_url, &init)?;
    stub.fetch_with_request(req).await
}

fn is_hex16(s: &str) -> bool {
    s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

// Small helpers for DO handlers that read query params off the stub URL.
fn query_string(req: &Request, key: &str) -> Option<String> {
    let url = req.url().ok()?;
    url.query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

fn query_i64(req: &Request, key: &str) -> Option<i64> {
    query_string(req, key).and_then(|v| v.parse::<i64>().ok())
}

// Serialize a TransactionRecord and POST it to a DO endpoint. Used by
// both the pre-upstream placeholder write (/ingest/start) and the
// post-upstream finalize write (/ingest/finalize).
async fn post_record_to_do(
    stub: &worker::durable::Stub,
    path: &str,
    record: &TransactionRecord,
) {
    let body_json = match serde_json::to_string(record) {
        Ok(s) => s,
        Err(_) => return,
    };
    let arr = Uint8Array::from(body_json.as_bytes());
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(arr.into()));
    // Hostname is irrelevant — the stub routes by binding, not URL.
    let url = format!("https://store{}", path);
    if let Ok(req) = Request::new_with_init(&url, &init) {
        if let Err(e) = stub.fetch_with_request(req).await {
            console_log!(
                "{{\"dir\":\"do_ingest_err\",\"path\":\"{}\",\"err\":\"{:?}\"}}",
                path,
                e
            );
        }
    }
}

/// Fire-and-forget JSON POST to a Durable Object endpoint (no DB write
/// on the DO side for broadcast-only routes like /ws/turn-start).
async fn post_json_to_do(
    stub: &worker::durable::Stub,
    path: &str,
    payload: &serde_json::Value,
) {
    let body_json = match serde_json::to_string(payload) {
        Ok(s) => s,
        Err(_) => return,
    };
    let arr = Uint8Array::from(body_json.as_bytes());
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(arr.into()));
    let url = format!("https://store{}", path);
    if let Ok(req) = Request::new_with_init(&url, &init) {
        let _ = stub.fetch_with_request(req).await;
    }
}

// ---------- Per-user Durable Object with SQLite ----------

#[durable_object]
pub struct UserStore {
    state: State,
    env: Env,
    initialized: std::cell::Cell<bool>,
}

impl DurableObject for UserStore {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            initialized: std::cell::Cell::new(false),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        self.ensure_init();
        let url = req.url()?;
        let path = url.path().to_string();
        match (req.method(), path.as_str()) {
            (Method::Get, "/ws") => self.handle_ws_upgrade().await,
            (Method::Post, "/ws/turn-start") => self.broadcast_turn_start(&mut req).await,
            (Method::Post, "/ingest") => self.ingest(&mut req).await,
            (Method::Post, "/session/end") => self.end_session(&mut req).await,
            (Method::Get, "/session/ends") => self.session_ends().await,
            (Method::Get, "/recent") => self.recent(&req).await,
            (Method::Get, "/stats") => self.stats(&req).await,
            (Method::Get, "/sessions/summary") => self.sessions_summary().await,
            (Method::Get, "/session/turns") => self.session_turns(&req).await,
            (Method::Get, "/in_flight") => self.in_flight_turns(&req).await,
            (Method::Post, "/sql") => self.sql_exec(&mut req).await,
            (Method::Post, "/search/fts") => self.search_fts(&mut req).await,
            (Method::Post, "/search/hydrate") => self.search_hydrate(&mut req).await,
            (Method::Post, "/search") => self.search(&mut req).await,
            (Method::Post, "/turn") => self.fetch_turn(&mut req).await,
            (Method::Post, "/vectorize/backfill") => self.vectorize_backfill(&mut req).await,
            (Method::Post, "/migrate-in") => self.migrate_in(&mut req).await,
            _ => Response::error("not found", 404),
        }
    }

    async fn websocket_message(
        &self,
        _ws: WebSocket,
        _message: WebSocketIncomingMessage,
    ) -> Result<()> {
        // No client→server messages needed yet.
        Ok(())
    }

    async fn websocket_close(
        &self,
        _ws: WebSocket,
        _code: usize,
        _reason: String,
        _was_clean: bool,
    ) -> Result<()> {
        // Runtime auto-removes the socket from get_websockets().
        Ok(())
    }

    async fn websocket_error(&self, _ws: WebSocket, _error: worker::Error) -> Result<()> {
        Ok(())
    }
}

impl UserStore {
    fn ensure_init(&self) {
        if self.initialized.get() {
            return;
        }
        let sql = self.state.storage().sql();
        let _ = sql.exec(
            "CREATE TABLE IF NOT EXISTS transactions (
                tx_id TEXT PRIMARY KEY,
                ts INTEGER NOT NULL,
                session_id TEXT,
                method TEXT,
                url TEXT,
                status INTEGER,
                elapsed_ms INTEGER,
                model TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cache_read INTEGER,
                cache_creation INTEGER,
                stop_reason TEXT,
                tools_json TEXT,
                req_body_bytes INTEGER,
                resp_body_bytes INTEGER
            )",
            None,
        );
        let _ = sql.exec(
            "CREATE INDEX IF NOT EXISTS idx_ts ON transactions(ts DESC)",
            None,
        );
        let _ = sql.exec(
            "CREATE INDEX IF NOT EXISTS idx_session ON transactions(session_id, ts)",
            None,
        );
        let _ = sql.exec(
            "CREATE TABLE IF NOT EXISTS session_ends (
                session_id TEXT PRIMARY KEY,
                ended_at INTEGER NOT NULL
            )",
            None,
        );
        // Additive column migrations. SQLite errors if a column already
        // exists; we swallow via `let _` so the call is idempotent.
        let new_cols: &[(&str, &str)] = &[
            ("cache_creation_5m", "INTEGER"),
            ("cache_creation_1h", "INTEGER"),
            ("thinking_budget", "INTEGER"),
            ("thinking_blocks", "INTEGER"),
            ("max_tokens", "INTEGER"),
            ("rl_req_remaining", "INTEGER"),
            ("rl_req_limit", "INTEGER"),
            ("rl_tok_remaining", "INTEGER"),
            ("rl_tok_limit", "INTEGER"),
            // Anthropic's `message.id` from message_start. Row PK is a
            // synthetic `tx-<ts>-<rand>` generated at request arrival so the
            // dashboard key stays stable across the turn_start → turn_complete
            // WS lifecycle (Anthropic's message.id only lands mid-stream).
            ("anthropic_message_id", "TEXT"),
            // Free-text search columns. Populated on finalize: the last
            // user-role message's text (incl. tool_result content) and the
            // assistant's text_delta stream output.
            ("user_text", "TEXT"),
            ("assistant_text", "TEXT"),
            // Full thinking content from thinking_delta SSE events.
            ("thinking_text", "TEXT"),
            // JSON array of {name, id, input} for each tool call in the turn.
            ("tool_calls_json", "TEXT"),
        ];
        for (name, typ) in new_cols {
            let _ = sql.exec(
                &format!("ALTER TABLE transactions ADD COLUMN {} {}", name, typ),
                None,
            );
        }

        // FTS5 search index over user_text + assistant_text. External-content
        // mode keeps tokens in a separate table while referencing `transactions`
        // rows by rowid. `porter` stemming lets "parsing" match "parse";
        // `unicode61` handles non-ASCII. `tx_id TEXT PRIMARY KEY` does NOT
        // alias rowid, so the implicit integer rowid is safe to use here.
        let _ = sql.exec(
            "CREATE VIRTUAL TABLE IF NOT EXISTS transactions_fts USING fts5(
                user_text, assistant_text,
                content='transactions', content_rowid='rowid',
                tokenize='porter unicode61'
            )",
            None,
        );
        let _ = sql.exec(
            "CREATE TRIGGER IF NOT EXISTS transactions_ai AFTER INSERT ON transactions BEGIN
                INSERT INTO transactions_fts(rowid, user_text, assistant_text)
                VALUES (new.rowid, new.user_text, new.assistant_text);
            END",
            None,
        );
        let _ = sql.exec(
            "CREATE TRIGGER IF NOT EXISTS transactions_ad AFTER DELETE ON transactions BEGIN
                INSERT INTO transactions_fts(transactions_fts, rowid, user_text, assistant_text)
                VALUES ('delete', old.rowid, old.user_text, old.assistant_text);
            END",
            None,
        );
        let _ = sql.exec(
            "CREATE TRIGGER IF NOT EXISTS transactions_au AFTER UPDATE ON transactions BEGIN
                INSERT INTO transactions_fts(transactions_fts, rowid, user_text, assistant_text)
                VALUES ('delete', old.rowid, old.user_text, old.assistant_text);
                INSERT INTO transactions_fts(rowid, user_text, assistant_text)
                VALUES (new.rowid, new.user_text, new.assistant_text);
            END",
            None,
        );
        // One-shot migration: drop the legacy `in_flight` column and its
        // partial index. Post-WebSocket refactor the column is never written
        // to 1, so it and the orphan sweep below were dead. `let _ =`
        // because the second run of this init path (after DO restart) will
        // see "no such column" — that's expected and fine.
        let _ = sql.exec("DROP INDEX IF EXISTS idx_in_flight", None);
        let _ = sql.exec("ALTER TABLE transactions DROP COLUMN in_flight", None);

        // In-flight turns are tracked here so a page refresh during an
        // active turn can re-render the spinner. Rows are inserted on
        // /ws/turn-start and deleted on /ingest. The stale-sweep below
        // purges anything older than 10 min in case the proxy crashed
        // between turn-start and ingest.
        let _ = sql.exec(
            "CREATE TABLE IF NOT EXISTS in_flight_turns (
                tx_id TEXT PRIMARY KEY,
                session_id TEXT,
                ts INTEGER NOT NULL,
                model TEXT,
                tool_choice TEXT,
                thinking_budget INTEGER,
                max_tokens INTEGER
            )",
            None,
        );
        let stale_cutoff = (Date::now().as_millis() as i64) - 10 * 60_000;
        let _ = sql.exec(
            "DELETE FROM in_flight_turns WHERE ts < ?",
            Some(vec![stale_cutoff.into()]),
        );

        // Maintained aggregate — one row per (session_id, model). The DO
        // recomputes the affected session's rows in `write_row` on every
        // insert/update, so this stays O(sessions) to read even as the
        // transactions table grows without bound. Polled every 5 s by the
        // sidebar, so this is the single biggest read-side win.
        let summary_is_new = matches!(
            sql.exec(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='session_summaries'",
                None,
            )
            .and_then(|c| c.to_array::<serde_json::Value>()),
            Ok(v) if v.is_empty(),
        );
        let _ = sql.exec(
            "CREATE TABLE IF NOT EXISTS session_summaries (
                session_id TEXT NOT NULL,
                model TEXT,
                turns INTEGER NOT NULL,
                first_ts INTEGER NOT NULL,
                last_ts INTEGER NOT NULL,
                input_tokens INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0,
                cache_read INTEGER NOT NULL DEFAULT 0,
                cache_creation INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (session_id, model)
            )",
            None,
        );
        // First-time backfill: if the table is brand new, seed it from the
        // existing transactions. O(turns) one-off; all subsequent writes
        // maintain it incrementally.
        // Skip rows with model IS NULL — these are auxiliary requests
        // (count_tokens, etc.) that carry no tokens and are filtered out
        // of the turn list. Counting them inflates the session's "turns"
        // header vs. what's actually rendered.
        if summary_is_new {
            let _ = sql.exec(
                "INSERT INTO session_summaries
                   (session_id, model, turns, first_ts, last_ts,
                    input_tokens, output_tokens, cache_read, cache_creation)
                 SELECT session_id, model,
                        COUNT(*), MIN(ts), MAX(ts),
                        COALESCE(SUM(input_tokens), 0),
                        COALESCE(SUM(output_tokens), 0),
                        COALESCE(SUM(cache_read), 0),
                        COALESCE(SUM(cache_creation), 0)
                 FROM transactions
                 WHERE session_id IS NOT NULL AND model IS NOT NULL
                 GROUP BY session_id, model",
                None,
            );
        }
        // Self-heal: earlier versions of refresh_session_summary included
        // model-NULL aux rows, leaving phantom "1 turn / No turns loaded"
        // entries in the dashboard. Cheap one-shot sweep on every init
        // keeps the table consistent with the new filter.
        let _ = sql.exec(
            "DELETE FROM session_summaries WHERE model IS NULL",
            None,
        );
        self.initialized.set(true);
    }

    async fn session_ends(&self) -> Result<Response> {
        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT session_id, ended_at FROM session_ends",
            None,
        )?;
        let rows: Vec<serde_json::Value> = cursor.to_array()?;
        let mut map = serde_json::Map::new();
        for r in rows {
            let (Some(sid), Some(ended)) = (
                r.get("session_id").and_then(|v| v.as_str()),
                r.get("ended_at").and_then(|v| v.as_i64()),
            ) else {
                continue;
            };
            map.insert(sid.to_string(), json!(ended));
        }
        Response::from_json(&serde_json::Value::Object(map))
    }

    // ---------- WebSocket ----------

    async fn handle_ws_upgrade(&self) -> Result<Response> {
        let pair = WebSocketPair::new()?;
        self.state.accept_web_socket(&pair.server);
        Response::from_websocket(pair.client)
    }

    fn broadcast(&self, msg: &serde_json::Value) {
        let text = msg.to_string();
        for ws in self.state.get_websockets() {
            let _ = ws.send_with_str(&text);
        }
    }

    async fn broadcast_turn_start(&self, req: &mut Request) -> Result<Response> {
        let payload: serde_json::Value = req.json().await?;

        // Persist so a fresh page load (refresh during an active turn) can
        // hydrate the spinner instead of waiting for the next WS event.
        let tx_id = payload.get("tx_id").and_then(|v| v.as_str()).unwrap_or("");
        if !tx_id.is_empty() {
            let session_id = payload.get("session_id").and_then(|v| v.as_str()).map(String::from);
            let ts = payload.get("ts").and_then(|v| v.as_i64()).unwrap_or_else(|| Date::now().as_millis() as i64);
            let model = payload.get("model").and_then(|v| v.as_str()).map(String::from);
            let tool_choice = payload.get("tool_choice").and_then(|v| v.as_str()).map(String::from);
            let thinking_budget = payload.get("thinking_budget").and_then(|v| v.as_i64());
            let max_tokens = payload.get("max_tokens").and_then(|v| v.as_i64());
            let _ = self.state.storage().sql().exec(
                "INSERT OR REPLACE INTO in_flight_turns
                 (tx_id, session_id, ts, model, tool_choice, thinking_budget, max_tokens)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                Some(vec![
                    tx_id.to_string().into(),
                    session_id.into(),
                    ts.into(),
                    model.into(),
                    tool_choice.into(),
                    thinking_budget.into(),
                    max_tokens.into(),
                ]),
            );
        }

        self.broadcast(&json!({
            "type": "turn_start",
            "data": payload,
        }));
        Response::ok("ok")
    }

    async fn in_flight_turns(&self, req: &Request) -> Result<Response> {
        // Opportunistic GC: drop orphans from prior turns whose /ingest never
        // landed (worker timeout, abort, etc). Otherwise the dashboard shows
        // ghost spinners until the next DO init runs the same cleanup.
        let cutoff = Date::now().as_millis() as i64 - 10 * 60_000;
        let _ = self.state.storage().sql().exec(
            "DELETE FROM in_flight_turns WHERE ts < ?",
            Some(vec![cutoff.into()]),
        );

        let url = req.url()?;
        let session_id = url.query_pairs().find(|(k, _)| k == "session_id").map(|(_, v)| v.into_owned());
        let sql = self.state.storage().sql();
        let cursor = if let Some(sid) = session_id {
            sql.exec(
                "SELECT tx_id, session_id, ts, model, tool_choice, thinking_budget, max_tokens
                 FROM in_flight_turns WHERE session_id = ? ORDER BY ts DESC",
                Some(vec![sid.into()]),
            )?
        } else {
            sql.exec(
                "SELECT tx_id, session_id, ts, model, tool_choice, thinking_budget, max_tokens
                 FROM in_flight_turns ORDER BY ts DESC",
                None,
            )?
        };
        let rows: Vec<serde_json::Value> = cursor.to_array()?;
        Response::from_json(&rows)
    }

    // ---------- Sessions ----------

    async fn end_session(&self, req: &mut Request) -> Result<Response> {
        #[derive(Deserialize)]
        struct Body {
            session_id: String,
        }
        let b: Body = match req.json().await {
            Ok(b) => b,
            Err(_) => return Response::error("invalid body", 400),
        };
        if b.session_id.is_empty() {
            return Response::error("session_id required", 400);
        }
        let ended_at = Date::now().as_millis() as i64;
        self.state.storage().sql().exec(
            "INSERT OR REPLACE INTO session_ends (session_id, ended_at) VALUES (?, ?)",
            Some(vec![b.session_id.clone().into(), ended_at.into()]),
        )?;
        self.broadcast(&json!({
            "type": "session_end",
            "data": { "session_id": b.session_id, "ended_at": ended_at }
        }));
        Response::from_json(&json!({ "session_id": b.session_id, "ended_at": ended_at }))
    }

    async fn ingest(&self, req: &mut Request) -> Result<Response> {
        let r: TransactionRecord = req.json().await?;
        self.insert_or_replace(&r)?;

        // Clear the in-flight placeholder so a refresh after completion
        // doesn't show a stale spinner.
        let _ = self.state.storage().sql().exec(
            "DELETE FROM in_flight_turns WHERE tx_id = ?",
            Some(vec![r.tx_id.clone().into()]),
        );

        // Broadcast turn_complete to connected WS clients (list-view fields
        // only — user_text/assistant_text are large and not needed).
        self.broadcast(&json!({
            "type": "turn_complete",
            "data": {
                "tx_id": r.tx_id,
                "ts": r.ts,
                "session_id": r.session_id,
                "method": r.method,
                "url": r.url,
                "model": r.model,
                "status": r.status,
                "elapsed_ms": r.elapsed_ms,
                "input_tokens": r.input_tokens,
                "output_tokens": r.output_tokens,
                "cache_read": r.cache_read,
                "cache_creation": r.cache_creation,
                "stop_reason": r.stop_reason,
                "tools_json": r.tools_json,
                "req_body_bytes": r.req_body_bytes,
                "resp_body_bytes": r.resp_body_bytes,
                "cache_creation_5m": r.cache_creation_5m,
                "cache_creation_1h": r.cache_creation_1h,
                "thinking_budget": r.thinking_budget,
                "thinking_blocks": r.thinking_blocks,
                "max_tokens": r.max_tokens,
                "rl_req_remaining": r.rl_req_remaining,
                "rl_req_limit": r.rl_req_limit,
                "rl_tok_remaining": r.rl_tok_remaining,
                "rl_tok_limit": r.rl_tok_limit,
                "anthropic_message_id": r.anthropic_message_id,
                "has_text": if r.assistant_text.as_deref().unwrap_or("").is_empty() { 0 } else { 1 },
            }
        }));

        Response::ok("ok")
    }

    fn insert_or_replace(&self, r: &TransactionRecord) -> Result<()> {
        self.write_row("INSERT OR REPLACE", r)
    }

    fn write_row(&self, verb: &str, r: &TransactionRecord) -> Result<()> {
        let sql = self.state.storage().sql();
        let stmt = format!(
            "{verb} INTO transactions
             (tx_id, ts, session_id, method, url, status, elapsed_ms,
              model, input_tokens, output_tokens, cache_read, cache_creation,
              stop_reason, tools_json, req_body_bytes, resp_body_bytes,
              cache_creation_5m, cache_creation_1h, thinking_budget, thinking_blocks,
              max_tokens, rl_req_remaining, rl_req_limit, rl_tok_remaining, rl_tok_limit,
              anthropic_message_id,
              user_text, assistant_text, thinking_text, tool_calls_json)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        sql.exec(
            &stmt,
            Some(vec![
                r.tx_id.clone().into(),
                r.ts.into(),
                r.session_id.clone().into(),
                r.method.clone().into(),
                r.url.clone().into(),
                (r.status as i64).into(),
                r.elapsed_ms.into(),
                r.model.clone().into(),
                r.input_tokens.into(),
                r.output_tokens.into(),
                r.cache_read.into(),
                r.cache_creation.into(),
                r.stop_reason.clone().into(),
                r.tools_json.clone().into(),
                r.req_body_bytes.into(),
                r.resp_body_bytes.into(),
                r.cache_creation_5m.into(),
                r.cache_creation_1h.into(),
                r.thinking_budget.into(),
                r.thinking_blocks.into(),
                r.max_tokens.into(),
                r.rl_req_remaining.into(),
                r.rl_req_limit.into(),
                r.rl_tok_remaining.into(),
                r.rl_tok_limit.into(),
                r.anthropic_message_id.clone().into(),
                r.user_text.clone().into(),
                r.assistant_text.clone().into(),
                r.thinking_text.clone().into(),
                r.tool_calls_json.clone().into(),
            ]),
        )?;

        // Maintain the per-session aggregate. Scoped to this session only
        // (not a global re-scan), so cost is O(turns-in-this-session) —
        // typically tens of rows, sub-millisecond. Skipped for unlinked
        // rows; those are invisible to the session view anyway.
        if let Some(sid) = r.session_id.as_ref() {
            self.refresh_session_summary(sid)?;
        }
        Ok(())
    }

    // DELETE + re-aggregate for one session. Cheap (bounded by
    // turns-in-session via idx_session) and keeps the materialized table
    // exactly in sync with the raw transactions table — no delta math,
    // no drift on retries.
    fn refresh_session_summary(&self, session_id: &str) -> Result<()> {
        let sql = self.state.storage().sql();
        sql.exec(
            "DELETE FROM session_summaries WHERE session_id = ?",
            Some(vec![session_id.into()]),
        )?;
        sql.exec(
            "INSERT INTO session_summaries
               (session_id, model, turns, first_ts, last_ts,
                input_tokens, output_tokens, cache_read, cache_creation)
             SELECT session_id, model,
                    COUNT(*), MIN(ts), MAX(ts),
                    COALESCE(SUM(input_tokens), 0),
                    COALESCE(SUM(output_tokens), 0),
                    COALESCE(SUM(cache_read), 0),
                    COALESCE(SUM(cache_creation), 0)
             FROM transactions
             WHERE session_id = ? AND model IS NOT NULL
             GROUP BY session_id, model",
            Some(vec![session_id.into()]),
        )?;
        Ok(())
    }

    async fn recent(&self, req: &Request) -> Result<Response> {
        let since = query_i64(req, "since").unwrap_or(0);
        let sql = self.state.storage().sql();
        // List-view columns only: excludes user_text / assistant_text
        // — those are fetched on demand via /turn for the detail view.
        // `has_text` flags turns that produced a non-empty text content
        // block (vs pure tool_use responses) so the dashboard can badge
        // them without shipping the whole body.
        let cursor = sql.exec(
            "SELECT tx_id, ts, session_id, method, url, model, status, elapsed_ms,
                    input_tokens, output_tokens, cache_read, cache_creation,
                    stop_reason, tools_json, req_body_bytes, resp_body_bytes,
                    cache_creation_5m, cache_creation_1h,
                    thinking_budget, thinking_blocks, max_tokens,
                    rl_req_remaining, rl_req_limit,
                    rl_tok_remaining, rl_tok_limit,
                    anthropic_message_id,
                    CASE WHEN LENGTH(COALESCE(assistant_text,'')) > 0 THEN 1 ELSE 0 END AS has_text
             FROM transactions WHERE ts >= ? ORDER BY ts DESC",
            Some(vec![since.into()]),
        )?;
        let rows: Vec<serde_json::Value> = cursor.to_array()?;
        Response::from_json(&rows)
    }

    async fn stats(&self, req: &Request) -> Result<Response> {
        let since = query_i64(req, "since").unwrap_or(0);
        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT
                COUNT(*) AS turns,
                COALESCE(SUM(input_tokens), 0) AS input_tokens,
                COALESCE(SUM(output_tokens), 0) AS output_tokens,
                COALESCE(SUM(cache_read), 0) AS cache_read,
                COALESCE(SUM(cache_creation), 0) AS cache_creation,
                COALESCE(SUM(req_body_bytes), 0) AS req_bytes,
                COALESCE(SUM(resp_body_bytes), 0) AS resp_bytes,
                MIN(ts) AS first_ts,
                MAX(ts) AS last_ts,
                COUNT(DISTINCT session_id) AS sessions,
                COALESCE(SUM(elapsed_ms), 0) AS total_elapsed_ms,
                COALESCE(SUM(cache_creation_5m), 0) AS cache_creation_5m,
                COALESCE(SUM(cache_creation_1h), 0) AS cache_creation_1h,
                COALESCE(SUM(
                    CASE model
                        WHEN 'claude-opus-4-7' THEN (input_tokens*5.0 + output_tokens*25.0 + cache_read*0.5 + cache_creation*6.25) / 1000000.0
                        WHEN 'claude-opus-4-6' THEN (input_tokens*5.0 + output_tokens*25.0 + cache_read*0.5 + cache_creation*6.25) / 1000000.0
                        WHEN 'claude-opus-4-5' THEN (input_tokens*5.0 + output_tokens*25.0 + cache_read*0.5 + cache_creation*6.25) / 1000000.0
                        WHEN 'claude-opus-4-1' THEN (input_tokens*15.0 + output_tokens*75.0 + cache_read*1.5 + cache_creation*18.75) / 1000000.0
                        WHEN 'claude-opus-4' THEN (input_tokens*15.0 + output_tokens*75.0 + cache_read*1.5 + cache_creation*18.75) / 1000000.0
                        WHEN 'claude-sonnet-4-6' THEN (input_tokens*3.0 + output_tokens*15.0 + cache_read*0.3 + cache_creation*3.75) / 1000000.0
                        WHEN 'claude-sonnet-4-5' THEN (input_tokens*3.0 + output_tokens*15.0 + cache_read*0.3 + cache_creation*3.75) / 1000000.0
                        WHEN 'claude-sonnet-4' THEN (input_tokens*3.0 + output_tokens*15.0 + cache_read*0.3 + cache_creation*3.75) / 1000000.0
                        WHEN 'claude-sonnet-4-5-20241022' THEN (input_tokens*3.0 + output_tokens*15.0 + cache_read*0.3 + cache_creation*3.75) / 1000000.0
                        WHEN 'claude-3-5-sonnet-20241022' THEN (input_tokens*3.0 + output_tokens*15.0 + cache_read*0.3 + cache_creation*3.75) / 1000000.0
                        WHEN 'claude-3-5-sonnet-20240620' THEN (input_tokens*3.0 + output_tokens*15.0 + cache_read*0.3 + cache_creation*3.75) / 1000000.0
                        WHEN 'claude-haiku-4-5-20251001' THEN (input_tokens*1.0 + output_tokens*5.0 + cache_read*0.1 + cache_creation*1.25) / 1000000.0
                        WHEN 'claude-haiku-4-5' THEN (input_tokens*1.0 + output_tokens*5.0 + cache_read*0.1 + cache_creation*1.25) / 1000000.0
                        WHEN 'claude-3-5-haiku-20241022' THEN (input_tokens*1.0 + output_tokens*5.0 + cache_read*0.1 + cache_creation*1.25) / 1000000.0
                        WHEN 'claude-3-opus-20240229' THEN (input_tokens*15.0 + output_tokens*75.0 + cache_read*1.5 + cache_creation*18.75) / 1000000.0
                        WHEN 'claude-3-haiku-20240307' THEN (input_tokens*0.25 + output_tokens*1.25 + cache_read*0.03 + cache_creation*0.3) / 1000000.0
                        ELSE 0
                    END
                ), 0) AS estimated_cost_usd
             FROM transactions WHERE ts >= ?",
            Some(vec![since.into()]),
        )?;
        let rows: Vec<serde_json::Value> = cursor.to_array()?;
        let mut summary = rows.into_iter().next().unwrap_or(json!({}));
        if let Some(obj) = summary.as_object_mut() {
            obj.insert("storage_bytes".into(), json!(sql.database_size() as i64));
        }
        Response::from_json(&summary)
    }

    // Per-session aggregate, one row per (session_id, model). Read is a
    // plain scan of the maintained `session_summaries` table — O(sessions),
    // independent of turn count. Write path refreshes this table for the
    // affected session on every transaction insert/update.
    async fn sessions_summary(&self) -> Result<Response> {
        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT session_id, model, turns, first_ts, last_ts,
                    input_tokens, output_tokens, cache_read, cache_creation
             FROM session_summaries
             ORDER BY session_id, turns DESC",
            None,
        )?;
        let rows: Vec<serde_json::Value> = cursor.to_array()?;
        Response::from_json(&rows)
    }

    // Turns for a single session, same trimmed column set as /recent.
    // Fetched eagerly for active sessions and on-demand when the user
    // expands a collapsed session in the recent-turns table.
    async fn session_turns(&self, req: &Request) -> Result<Response> {
        let Some(id) = query_string(req, "id") else {
            return Response::error("missing id", 400);
        };
        if id.is_empty() {
            return Response::error("missing id", 400);
        }
        let limit = query_i64(req, "limit").unwrap_or(1000).clamp(1, 5000);
        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT tx_id, ts, session_id, method, url, model, status, elapsed_ms,
                    input_tokens, output_tokens, cache_read, cache_creation,
                    stop_reason, tools_json, req_body_bytes, resp_body_bytes,
                    cache_creation_5m, cache_creation_1h,
                    thinking_budget, thinking_blocks, max_tokens,
                    rl_req_remaining, rl_req_limit,
                    rl_tok_remaining, rl_tok_limit,
                    anthropic_message_id,
                    CASE WHEN LENGTH(COALESCE(assistant_text,'')) > 0 THEN 1 ELSE 0 END AS has_text
             FROM transactions
             WHERE session_id = ? ORDER BY ts DESC LIMIT ?",
            Some(vec![id.into(), limit.into()]),
        )?;
        let rows: Vec<serde_json::Value> = cursor.to_array()?;
        Response::from_json(&rows)
    }

    // Fetch a single turn's complete record — all columns including full
    // user_text + assistant_text. Backs `burnage turn <tx_id>`.
    async fn fetch_turn(&self, req: &mut Request) -> Result<Response> {
        #[derive(Deserialize)]
        struct Body {
            tx_id: String,
        }
        let b: Body = match req.json().await {
            Ok(b) => b,
            Err(_) => return Response::error("invalid body", 400),
        };
        if b.tx_id.trim().is_empty() {
            return Response::error("missing tx_id", 400);
        }
        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT * FROM transactions WHERE tx_id = ? LIMIT 1",
            Some(vec![b.tx_id.clone().into()]),
        )?;
        let rows: Vec<serde_json::Value> = cursor.to_array().unwrap_or_default();
        match rows.into_iter().next() {
            Some(row) => Response::from_json(&row),
            None => Response::error("not found", 404),
        }
    }

    // FTS5 full-text search over user_text + assistant_text. `snippet()` emits
    // a short window around the match with <mark> highlights; `bm25()` scores
    // lower-is-better. Returned rows include the negated bm25 as `score` so
    // higher-is-better sorting works alongside Vectorize cosine scores.
    //
    // Includes a correlated subquery for the session's most-recent turn
    // timestamp — the UI shows this instead of the per-turn ts so matches in
    // still-active sessions display as recent.
    fn fts_search_rows(&self, q: &str, limit: i64) -> Result<Vec<serde_json::Value>> {
        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT t.tx_id, t.ts, t.session_id, t.model,
                    snippet(transactions_fts, 0, '<mark>', '</mark>', '…', 10) AS user_snip,
                    snippet(transactions_fts, 1, '<mark>', '</mark>', '…', 10) AS asst_snip,
                    -bm25(transactions_fts) AS score,
                    (SELECT MAX(s.last_ts) FROM session_summaries s
                     WHERE s.session_id = t.session_id) AS session_last_ts
             FROM transactions_fts
             JOIN transactions t ON t.rowid = transactions_fts.rowid
             WHERE transactions_fts MATCH ?
             ORDER BY bm25(transactions_fts) ASC
             LIMIT ?",
            Some(vec![q.into(), limit.into()]),
        )?;
        Ok(cursor.to_array().unwrap_or_default())
    }

    // Hydrate a set of tx_ids (from Vectorize) into rows with raw text
    // prefixes. Window size (2000 chars) is sized so the Rust-side
    // `window_snippet` helper can find a query-token match within a
    // realistic portion of the turn. The final displayed snippet is capped
    // to ~220 chars in post-processing.
    fn hydrate_rows(&self, tx_ids: &[String]) -> Result<Vec<serde_json::Value>> {
        if tx_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<String> = tx_ids.iter().take(200).cloned().collect();
        let placeholders = vec!["?"; ids.len()].join(",");
        let query = format!(
            "SELECT t.tx_id, t.ts, t.session_id, t.model,
                    substr(COALESCE(t.user_text, ''), 1, 2000) AS user_snip,
                    substr(COALESCE(t.assistant_text, ''), 1, 2000) AS asst_snip,
                    (SELECT MAX(s.last_ts) FROM session_summaries s
                     WHERE s.session_id = t.session_id) AS session_last_ts
             FROM transactions t WHERE t.tx_id IN ({})",
            placeholders
        );
        let params: Vec<SqlStorageValue> = ids.into_iter().map(SqlStorageValue::from).collect();
        let cursor = self.state.storage().sql().exec(&query, Some(params))?;
        Ok(cursor.to_array().unwrap_or_default())
    }

    // HTTP handler for /search/fts — thin wrapper over fts_search_rows.
    // Kept as a public DO route so `burnage shell` can hit FTS directly.
    async fn search_fts(&self, req: &mut Request) -> Result<Response> {
        #[derive(Deserialize)]
        struct Body {
            q: String,
            #[serde(default)]
            limit: Option<i64>,
        }
        let b: Body = match req.json().await {
            Ok(b) => b,
            Err(_) => return Response::error("invalid body", 400),
        };
        if b.q.trim().is_empty() {
            return Response::error("missing q", 400);
        }
        let limit = b.limit.unwrap_or(20).clamp(1, 100);
        match self.fts_search_rows(&b.q, limit) {
            Ok(rows) => Response::from_json(&rows),
            Err(e) => sql_error_response(&format!("fts: {e}")),
        }
    }

    async fn search_hydrate(&self, req: &mut Request) -> Result<Response> {
        #[derive(Deserialize)]
        struct Body {
            tx_ids: Vec<String>,
        }
        let b: Body = match req.json().await {
            Ok(b) => b,
            Err(_) => return Response::error("invalid body", 400),
        };
        let rows = self.hydrate_rows(&b.tx_ids)?;
        Response::from_json(&rows)
    }

    // Orchestrator: runs FTS + Vectorize internally and merges via RRF.
    // Entry point for both the proxy's /_cm/search and the dashboard's
    // /api/search — having it in one place keeps the TS side a dumb pipe.
    //
    // Rate limit lives here so both entrypoints are covered by one
    // implementation. DO storage gives us a strongly-consistent counter
    // for free — no KV eventual-consistency race on the bucket.
    async fn search(&self, req: &mut Request) -> Result<Response> {
        #[derive(Deserialize)]
        struct Body {
            q: String,
            #[serde(default)]
            mode: Option<String>,
            #[serde(default)]
            limit: Option<i64>,
            /// Caller-supplied user_hash. Required only for `mode=vector|hybrid`
            /// because Vectorize needs it for the `{prefix}:` id strip and the
            /// metadata filter. The DO itself doesn't know its own name.
            #[serde(default)]
            user_hash: Option<String>,
        }
        let b: Body = match req.json().await {
            Ok(b) => b,
            Err(_) => return Response::error("invalid body", 400),
        };
        let q_raw = b.q.trim().to_string();
        if q_raw.is_empty() {
            return Response::error("missing q", 400);
        }
        let mode = b.mode.as_deref().unwrap_or("hybrid").to_string();
        let limit = b.limit.unwrap_or(20).clamp(1, 100);

        if let Err(resp) = self.check_search_rate_limit() {
            return resp;
        }

        // Server-side <2-char guard — client enforces this too, but don't
        // trust the client to always do it. bm25 on a single char is noise.
        if q_raw.chars().count() < 2 {
            let empty: Vec<SearchHit> = Vec::new();
            return Response::from_json(&json!({ "mode": mode, "results": empty }));
        }

        // Sanitize for FTS5: quote-wrap each whitespace-split token so
        // FTS5's MATCH parser doesn't interpret `-`, `AND`, `:`, etc. as
        // operators. `future-me` → `"future-me"` (phrase query over the
        // tokenized form `future me`, adjacent). `session_id` → `"session_id"`
        // (phrase over `session id`, adjacent). Queries like `a NOT b` become
        // `"a" "NOT" "b"` — three AND'd literal tokens.
        let fts_q = sanitize_fts_query(&q_raw);
        // Raw tokens (no quotes) for Rust-side snippet windowing on vector
        // hits — we want to find the user's literal query terms in the text.
        let tokens = query_tokens(&q_raw);

        match mode.as_str() {
            "fts" => {
                let rows = if fts_q.is_empty() {
                    Vec::new()
                } else {
                    self.fts_search_rows(&fts_q, limit).unwrap_or_default()
                };
                let hits = hits_from_rows(rows, "fts");
                Response::from_json(&json!({ "mode": "fts", "results": hits }))
            }
            "vector" => {
                // mode=vector is an explicit user choice — no cosine floor
                // applied, the caller accepts whatever the embedder returns.
                let hits = self
                    .vector_search(&q_raw, &tokens, limit, b.user_hash.as_deref())
                    .await
                    .unwrap_or_default();
                Response::from_json(&json!({ "mode": "vector", "results": hits }))
            }
            _ => {
                let fts_rows = if fts_q.is_empty() {
                    Vec::new()
                } else {
                    self.fts_search_rows(&fts_q, limit).unwrap_or_default()
                };
                let fts_hits = hits_from_rows(fts_rows, "fts");
                let vec_hits_raw = self
                    .vector_search(&q_raw, &tokens, limit, b.user_hash.as_deref())
                    .await
                    .unwrap_or_default();
                // Cosine floor on vector hits. Below 0.65 is noise for this
                // embedder — gibberish queries top out around 0.61, real
                // semantic matches sit 0.68+. Apply regardless of whether
                // FTS returned hits: if a vector hit is noise, it shouldn't
                // contribute to RRF just because FTS also found something
                // (otherwise a spurious vector rank-1 ties with the real
                // FTS rank-1 at `1/61`, and the UI can show the noise
                // first). Revisit once we have query logs.
                let vec_hits: Vec<SearchHit> = vec_hits_raw
                    .into_iter()
                    .filter(|h| h.score >= 0.65)
                    .collect();
                let merged = reciprocal_rank_fusion(fts_hits, vec_hits, limit as usize);
                Response::from_json(&json!({ "mode": "hybrid", "results": merged }))
            }
        }
    }

    // Vector branch: embed the query via Workers AI, hit Vectorize scoped to
    // the caller's user_hash namespace, hydrate the returned tx_ids. Returns
    // an empty vec (never an error) when user_hash is missing — semantic
    // search simply isn't available in that case, which is a
    // degraded-but-functional state (FTS still works).
    //
    // `tokens` is the raw-query token list used for snippet windowing —
    // vector hits have no FTS match positions, so we find the first query
    // token lexically and emit a ±window-of-chars snippet with <mark>s
    // matching the FTS visual style. Falls back to the head prefix when no
    // token appears (true semantic-only match).
    async fn vector_search(
        &self,
        q: &str,
        tokens: &[String],
        limit: i64,
        user_hash: Option<&str>,
    ) -> std::result::Result<Vec<SearchHit>, String> {
        let Some(user_hash) = user_hash else {
            return Ok(Vec::new());
        };
        let vec = embed_text(&self.env, q).await.ok_or("embed failed")?;
        let matches = vectorize_query(&self.env, user_hash, &vec, limit as usize).await?;
        if matches.is_empty() {
            return Ok(Vec::new());
        }
        let tx_ids: Vec<String> = matches.iter().map(|(id, _)| id.clone()).collect();
        let rows = self.hydrate_rows(&tx_ids).map_err(|e| format!("hydrate: {e}"))?;
        let mut hits = hits_from_rows(rows, "vector");
        let score_by_id: std::collections::HashMap<String, f64> = matches.into_iter().collect();
        for h in &mut hits {
            if let Some(&s) = score_by_id.get(&h.tx_id) {
                h.score = s;
            }
            // Replace the 2000-char raw prefix with a windowed snippet.
            if let Some(user) = h.user_snip.as_deref() {
                h.user_snip = Some(window_snippet(user, tokens, SNIPPET_CHARS));
            }
            if let Some(asst) = h.asst_snip.as_deref() {
                h.asst_snip = Some(window_snippet(asst, tokens, SNIPPET_CHARS));
            }
        }
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        Ok(hits)
    }

    // Paginated re-embed/re-upsert of the caller's historical turns into the
    // Vectorize namespace. Pulls rows with ts < before_ts (DESC order), stops
    // at batch_size, and returns `next_before_ts` so clients can loop until
    // `done: true`. Embedding + upsert are both idempotent on tx_id, so re-
    // running a partially-completed batch is safe.
    async fn vectorize_backfill(&self, req: &mut Request) -> Result<Response> {
        #[derive(Deserialize)]
        struct Body {
            user_hash: String,
            #[serde(default)]
            batch_size: Option<i64>,
            #[serde(default)]
            before_ts: Option<i64>,
            #[serde(default)]
            embed_concurrency: Option<usize>,
            #[serde(default)]
            embed_stagger_ms: Option<u64>,
        }
        let b: Body = match req.json().await {
            Ok(b) => b,
            Err(_) => return Response::error("invalid body", 400),
        };
        if b.user_hash.trim().is_empty() {
            return Response::error("missing user_hash", 400);
        }
        let user_hash = b.user_hash;
        let batch_size = b.batch_size.unwrap_or(50).clamp(1, 200);
        let before_ts = b.before_ts.unwrap_or(i64::MAX);
        let embed_concurrency = b.embed_concurrency.unwrap_or(16).clamp(1, 64);
        let embed_stagger_ms = b.embed_stagger_ms.unwrap_or(0).min(1000);

        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT tx_id, session_id, ts, user_text, assistant_text, thinking_text
             FROM transactions
             WHERE ts < ?
               AND (length(COALESCE(user_text, '')) + length(COALESCE(assistant_text, '')) + length(COALESCE(thinking_text, ''))) > 3
             ORDER BY ts DESC
             LIMIT ?",
            Some(vec![before_ts.into(), batch_size.into()]),
        )?;
        let rows: Vec<serde_json::Value> = cursor.to_array().unwrap_or_default();

        // Total eligible rows for progress tracking. Cheap — indexed scan on
        // `ts` with a length() predicate; runs once per batch (~1ms).
        let total_rows: i64 = {
            let c = sql.exec(
                "SELECT COUNT(*) AS n FROM transactions
                 WHERE (length(COALESCE(user_text, '')) + length(COALESCE(assistant_text, '')) + length(COALESCE(thinking_text, ''))) > 3",
                None,
            )?;
            let arr: Vec<serde_json::Value> = c.to_array().unwrap_or_default();
            arr.first()
                .and_then(|v| v.get("n"))
                .and_then(|x| x.as_i64())
                .unwrap_or(0)
        };

        let scanned = rows.len() as i64;

        // NDJSON streaming response. The handler returns immediately with a
        // receiver stream as the body; the actual work runs on a spawned
        // task that writes one JSON line per event as it happens. Live
        // feedback at the CLI without waiting for the whole batch to finish.
        //
        // Event shapes:
        //   {"type":"row","tx_id":..,"ts":..,"status":"skipped_empty","text_len":..}
        //   {"type":"row","tx_id":..,"ts":..,"status":"embed_ok","text_len":..,"embed_ms":..}
        //   {"type":"row","tx_id":..,"ts":..,"status":"embed_err","text_len":..,"embed_ms":..}
        //   {"type":"end","scanned":..,"upserted":..,"skipped_empty":..,"embed_errors":..,
        //    "upsert_errors":..,"upsert_err":Option<str>,"oldest_ts":..,"next_before_ts":..,
        //    "done":..,"total_rows":..,"batch_upsert_ms":..}
        //
        // Bulk upsert is all-or-nothing (Vectorize semantics), so we don't
        // emit per-row upsert events — the `end` event's upsert_errors +
        // upsert_err fields cover it.
        let (tx, rx) = futures_channel::mpsc::unbounded::<std::result::Result<Vec<u8>, worker::Error>>();
        let rx: futures_channel::mpsc::UnboundedReceiver<std::result::Result<Vec<u8>, worker::Error>> = rx;

        struct PreEmbed {
            tx_id: String,
            session_id: Option<String>,
            ts: i64,
            text_len: i64,
            combined: String,
        }
        struct Pending {
            tx_id: String,
            session_id: Option<String>,
            ts: i64,
            values: Vec<f32>,
        }

        // Sync pre-pass: compute oldest_ts, emit skipped_empty events now
        // (cheap), collect the rest into to_embed for the async phase.
        let mut skipped_empty = 0i64;
        let mut oldest_ts = before_ts;
        let mut to_embed: Vec<PreEmbed> = Vec::with_capacity(rows.len());
        for row in &rows {
            let tx_id = row
                .get("tx_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let session_id = row
                .get("session_id")
                .and_then(|v| v.as_str())
                .map(String::from);
            let ts = row.get("ts").and_then(|v| v.as_i64()).unwrap_or(0);
            if ts < oldest_ts {
                oldest_ts = ts;
            }
            let ut = row.get("user_text").and_then(|v| v.as_str()).unwrap_or("");
            let at = row
                .get("assistant_text")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tt = row
                .get("thinking_text")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let combined = format!("{}\n---\n{}\n---\n{}", ut, at, tt);
            let text_len = combined.len() as i64;

            if combined.trim().len() <= 3 {
                skipped_empty += 1;
                let ev = json!({
                    "type": "row",
                    "tx_id": tx_id,
                    "ts": ts,
                    "status": "skipped_empty",
                    "text_len": text_len,
                });
                let _ = tx.unbounded_send(Ok(format!("{ev}\n").into_bytes()));
                continue;
            }
            to_embed.push(PreEmbed {
                tx_id,
                session_id,
                ts,
                text_len,
                combined,
            });
        }

        // Spawn the async embed + upsert phase. Has to run on the local task
        // queue (not block the handler) so the runtime can flush the
        // receiver stream to the client while work proceeds. tx moves in;
        // when the spawned task ends and drops tx, rx's stream closes and
        // the response body completes.
        let env = self.env.clone();
        let user_hash_for_task = user_hash.clone();
        wasm_bindgen_futures::spawn_local(async move {
            use futures_util::StreamExt;

            let mut pending: Vec<Pending> = Vec::with_capacity(to_embed.len());
            let mut embed_errors = 0i64;

            // Parallel embeds. As each completes (in arbitrary order thanks
            // to buffer_unordered), emit an event to the client stream.
            // embed_stagger_ms > 0 delays each task's start by i*stagger_ms
            // from spawn, spreading out request issuance across the AI gateway
            // (burst-rate mitigation; doesn't help against per-model concurrent
            // caps, which is what buffer_unordered handles).
            let mut stream = futures_util::stream::iter(to_embed.into_iter().enumerate().map(|(i, pe)| {
                let env = env.clone();
                async move {
                    if embed_stagger_ms > 0 && i > 0 {
                        let wait = std::time::Duration::from_millis(i as u64 * embed_stagger_ms);
                        worker::Delay::from(wait).await;
                    }
                    let start = Date::now().as_millis() as i64;
                    let result = embed_text(&env, &pe.combined).await;
                    let embed_ms = Date::now().as_millis() as i64 - start;
                    (pe, result, embed_ms)
                }
            }))
            .buffer_unordered(embed_concurrency);

            while let Some((pe, result, embed_ms)) = stream.next().await {
                match result {
                    Some(values) => {
                        let ev = json!({
                            "type": "row",
                            "tx_id": pe.tx_id,
                            "ts": pe.ts,
                            "status": "embed_ok",
                            "text_len": pe.text_len,
                            "embed_ms": embed_ms,
                        });
                        let _ = tx.unbounded_send(Ok(format!("{ev}\n").into_bytes()));
                        pending.push(Pending {
                            tx_id: pe.tx_id,
                            session_id: pe.session_id,
                            ts: pe.ts,
                            values,
                        });
                    }
                    None => {
                        embed_errors += 1;
                        let ev = json!({
                            "type": "row",
                            "tx_id": pe.tx_id,
                            "ts": pe.ts,
                            "status": "embed_err",
                            "text_len": pe.text_len,
                            "embed_ms": embed_ms,
                        });
                        let _ = tx.unbounded_send(Ok(format!("{ev}\n").into_bytes()));
                    }
                }
            }

            // Single bulk upsert of all successfully-embedded rows.
            let batch_upsert_start = Date::now().as_millis() as i64;
            let batch_items: Vec<BackfillVector<'_>> = pending
                .iter()
                .map(|p| BackfillVector {
                    tx_id: &p.tx_id,
                    session_id: p.session_id.as_deref(),
                    ts: p.ts,
                    values: p.values.clone(),
                })
                .collect();
            let batch_upsert_result =
                vectorize_upsert_many(&env, &user_hash_for_task, &batch_items).await;
            let batch_upsert_ms = Date::now().as_millis() as i64 - batch_upsert_start;

            let (upserted, upsert_errors, upsert_err_msg) = match &batch_upsert_result {
                Ok(()) => (pending.len() as i64, 0i64, serde_json::Value::Null),
                Err(e) => (
                    0i64,
                    pending.len() as i64,
                    serde_json::Value::String(e.clone()),
                ),
            };

            let done = scanned < batch_size;
            let next_before_ts = if done {
                serde_json::Value::Null
            } else {
                json!(oldest_ts)
            };
            let end_event = json!({
                "type": "end",
                "scanned": scanned,
                "upserted": upserted,
                "skipped_empty": skipped_empty,
                "embed_errors": embed_errors,
                "upsert_errors": upsert_errors,
                "upsert_err": upsert_err_msg,
                "oldest_ts": oldest_ts,
                "next_before_ts": next_before_ts,
                "done": done,
                "total_rows": total_rows,
                "batch_upsert_ms": batch_upsert_ms,
            });
            let _ = tx.unbounded_send(Ok(format!("{end_event}\n").into_bytes()));
            // tx dropped here → rx stream closes → response body completes.
        });

        let resp = Response::from_stream(rx)?;
        Ok(resp
            .with_headers({
                let h = Headers::new();
                let _ = h.set("content-type", "application/x-ndjson");
                h
            }))
    }

    // One-shot merge of a source DO's data into self. Used by
    // /_cm/admin/migrate-legacy to fold the pre-email-flip uuid-keyed DO
    // into the caller's current email-keyed DO. Idempotent via INSERT OR
    // IGNORE on PK (tx_id for transactions, session_id for session_ends).
    // After a successful copy, drops the source's tables so the legacy DO
    // stops consuming storage (and stops showing up in CF analytics once
    // its last invocation ages out of the 30d window).
    async fn migrate_in(&self, req: &mut Request) -> Result<Response> {
        #[derive(Deserialize)]
        struct Body {
            source_hash: String,
        }
        let body: Body = match req.json().await {
            Ok(b) => b,
            Err(_) => return Response::error("invalid body", 400),
        };
        if body.source_hash.trim().is_empty() {
            return Response::error("source_hash required", 400);
        }

        let ns = self.env.durable_object("USER_STORE")?;
        let source = ns.id_from_name(&body.source_hash)?.get_stub()?;

        let txns = fetch_rows_from_stub(&source, "SELECT * FROM transactions").await?;
        let copied_transactions = self.insert_rows("transactions", &txns)?;

        let ends = fetch_rows_from_stub(&source, "SELECT * FROM session_ends").await?;
        let copied_session_ends = self.insert_rows("session_ends", &ends)?;

        // Zero the legacy DO's storage. FTS virtual table + triggers get
        // torn down with `transactions`. search_rate_limit is ephemeral
        // counter state; drop it too.
        for drop_stmt in [
            "DROP TABLE IF EXISTS transactions_fts",
            "DROP TABLE IF EXISTS transactions",
            "DROP TABLE IF EXISTS session_ends",
            "DROP TABLE IF EXISTS search_rate_limit",
        ] {
            let _ = fetch_rows_from_stub(&source, drop_stmt).await;
        }

        Response::from_json(&json!({
            "source_hash": body.source_hash,
            "copied_transactions": copied_transactions,
            "copied_session_ends": copied_session_ends,
            "total_source_rows": txns.len() + ends.len(),
        }))
    }

    // Dynamic INSERT OR IGNORE for migrated rows. Column list is taken
    // from the row object itself (sqlite-backed /sql preserves projection
    // order), so schema additions since the legacy DO was written are
    // simply missing keys — they stay NULL in the target, which matches
    // what a fresh row with a new column would look like anyway.
    fn insert_rows(&self, table: &str, rows: &[serde_json::Value]) -> Result<i64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let sql = self.state.storage().sql();
        let mut inserted = 0i64;
        for row in rows {
            let Some(obj) = row.as_object() else {
                continue;
            };
            let cols: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
            if cols.is_empty() {
                continue;
            }
            let placeholders = vec!["?"; cols.len()].join(",");
            let col_list = cols.join(",");
            let stmt = format!(
                "INSERT OR IGNORE INTO {} ({}) VALUES ({})",
                table, col_list, placeholders
            );
            let params: Vec<SqlStorageValue> =
                cols.iter().map(|c| json_to_sql_value(&obj[*c])).collect();
            if sql.exec(&stmt, Some(params)).is_ok() {
                inserted += 1;
            }
        }
        Ok(inserted)
    }

    // Fixed 60s window, 120 requests/min/user. Counter lives in the DO's
    // SQLite so it's strongly consistent without a KV round-trip. Buckets
    // older than 5 min are cleaned up opportunistically on each check.
    fn check_search_rate_limit(&self) -> std::result::Result<(), Result<Response>> {
        const LIMIT_PER_MIN: i64 = 120;
        let sql = self.state.storage().sql();
        let _ = sql.exec(
            "CREATE TABLE IF NOT EXISTS search_rate_limit (
                bucket INTEGER PRIMARY KEY,
                count INTEGER NOT NULL
            )",
            None,
        );
        let now_ms = Date::now().as_millis() as i64;
        let bucket = now_ms / 60_000;
        // Opportunistic GC of stale buckets. Cheap — table stays tiny.
        let _ = sql.exec(
            "DELETE FROM search_rate_limit WHERE bucket < ?",
            Some(vec![(bucket - 5).into()]),
        );
        let _ = sql.exec(
            "INSERT INTO search_rate_limit(bucket, count) VALUES (?, 1)
             ON CONFLICT(bucket) DO UPDATE SET count = count + 1",
            Some(vec![bucket.into()]),
        );
        let cur = match sql.exec(
            "SELECT count FROM search_rate_limit WHERE bucket = ?",
            Some(vec![bucket.into()]),
        ) {
            Ok(c) => c
                .to_array::<serde_json::Value>()
                .ok()
                .and_then(|rs| rs.into_iter().next())
                .and_then(|r| r.get("count").and_then(|v| v.as_i64()))
                .unwrap_or(0),
            Err(_) => 0,
        };
        if cur > LIMIT_PER_MIN {
            let retry_after = 60 - ((now_ms % 60_000) / 1000) as i64;
            return Err(Response::from_json(&json!({
                "error": "rate_limited",
                "limit_per_min": LIMIT_PER_MIN,
                "retry_after_seconds": retry_after,
            }))
            .map(|r| r.with_status(429)));
        }
        Ok(())
    }

    // Generic SQL execution endpoint. Powers `burnage shell` and any other
    // admin tooling. Auth is enforced upstream in `admin_route`; this handler
    // trusts that whoever reached it owns this DO (or the proxy operator
    // owns the deployment, single-tenant-by-default).
    async fn sql_exec(&self, req: &mut Request) -> Result<Response> {
        #[derive(Deserialize)]
        struct Body {
            sql: String,
            #[serde(default)]
            params: Vec<serde_json::Value>,
        }
        let body: Body = match req.json().await {
            Ok(b) => b,
            Err(_) => return sql_error_response("invalid body — expected {sql, params?}"),
        };
        if body.sql.trim().is_empty() {
            return sql_error_response("sql cannot be empty");
        }

        let started = Date::now().as_millis() as i64;
        let sql = self.state.storage().sql();

        let params: Option<Vec<SqlStorageValue>> = if body.params.is_empty() {
            None
        } else {
            Some(body.params.iter().map(json_to_sql_value).collect())
        };

        let cursor = match sql.exec(&body.sql, params) {
            Ok(c) => c,
            Err(e) => return sql_error_response(&format!("{e}")),
        };
        let rows: Vec<serde_json::Value> = match cursor.to_array() {
            Ok(r) => r,
            Err(e) => return sql_error_response(&format!("{e}")),
        };

        // Column order from the first row's object keys. SQLite + worker-rs
        // preserve insertion order so this matches SELECT projection.
        let columns: Vec<String> = rows
            .first()
            .and_then(|r| r.as_object())
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();

        // For DML, follow up with `SELECT changes()` so the client gets a
        // useful "affected" count instead of 0.
        let first_kw = body
            .sql
            .trim_start()
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        let affected: i64 = if matches!(
            first_kw.as_str(),
            "INSERT" | "UPDATE" | "DELETE" | "REPLACE"
        ) {
            sql.exec("SELECT changes() AS n", None)
                .ok()
                .and_then(|c| c.to_array::<serde_json::Value>().ok())
                .and_then(|rs| rs.into_iter().next())
                .and_then(|r| r.get("n").and_then(|v| v.as_i64()))
                .unwrap_or(0)
        } else {
            rows.len() as i64
        };

        let took_ms = Date::now().as_millis() as i64 - started;
        Response::from_json(&json!({
            "columns": columns,
            "rows": rows,
            "affected": affected,
            "took_ms": took_ms,
        }))
    }
}

fn sql_error_response(msg: &str) -> Result<Response> {
    let r = Response::from_json(&json!({ "error": msg }))?;
    Ok(r.with_status(400))
}

fn json_to_sql_value(v: &serde_json::Value) -> SqlStorageValue {
    match v {
        serde_json::Value::Null => SqlStorageValue::Null,
        serde_json::Value::Bool(b) => SqlStorageValue::Boolean(*b),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(SqlStorageValue::Integer)
            .or_else(|| n.as_f64().map(SqlStorageValue::Float))
            .unwrap_or(SqlStorageValue::Null),
        serde_json::Value::String(s) => SqlStorageValue::String(s.clone()),
        // SQLite has no native array/object — stringify so callers can store
        // JSON blobs in TEXT columns.
        _ => SqlStorageValue::String(v.to_string()),
    }
}

// ---------- Wire types ----------

#[derive(Serialize, Deserialize, Default)]
struct TransactionRecord {
    tx_id: String,
    ts: i64,
    session_id: Option<String>,
    method: String,
    url: String,
    status: i32,
    elapsed_ms: i64,
    model: Option<String>,
    input_tokens: i64,
    output_tokens: i64,
    cache_read: i64,
    cache_creation: i64,
    stop_reason: Option<String>,
    tools_json: Option<String>,
    req_body_bytes: i64,
    resp_body_bytes: i64,
    #[serde(default)]
    cache_creation_5m: Option<i64>,
    #[serde(default)]
    cache_creation_1h: Option<i64>,
    #[serde(default)]
    thinking_budget: Option<i64>,
    #[serde(default)]
    thinking_blocks: Option<i64>,
    #[serde(default)]
    max_tokens: Option<i64>,
    #[serde(default)]
    rl_req_remaining: Option<i64>,
    #[serde(default)]
    rl_req_limit: Option<i64>,
    #[serde(default)]
    rl_tok_remaining: Option<i64>,
    #[serde(default)]
    rl_tok_limit: Option<i64>,
    #[serde(default)]
    anthropic_message_id: Option<String>,
    /// Text of the caller's last `user` message — new prompt + tool_result
    /// payloads. Indexed by FTS5 + embedded for Vectorize.
    #[serde(default)]
    user_text: Option<String>,
    /// Concatenated `text_delta` payloads from the assistant's SSE stream.
    #[serde(default)]
    assistant_text: Option<String>,
    /// Concatenated `thinking_delta` payloads — the model's extended thinking.
    #[serde(default)]
    thinking_text: Option<String>,
    /// JSON array of `{name, id, input}` for each tool call invoked in the turn.
    #[serde(default)]
    tool_calls_json: Option<String>,
}

// ---------- SSE parsing ----------

#[derive(Serialize)]
struct ToolCall {
    name: String,
    id: String,
    input: String,
}

#[derive(Default)]
struct SseStats {
    tx_id: Option<String>,
    model: Option<String>,
    input_tokens: i64,
    output_tokens: i64,
    cache_read: i64,
    cache_creation: i64,
    cache_creation_5m: i64,
    cache_creation_1h: i64,
    stop_reason: Option<String>,
    tools: Vec<String>,
    thinking_blocks: i64,
    assistant_text: String,
    thinking_text: String,
    /// Structured tool calls with name, id, and full input JSON.
    tool_calls: Vec<ToolCall>,
    /// Maps SSE block index → index in `tool_calls` for input accumulation.
    tool_block_map: Vec<(usize, usize)>,
}

fn parse_sse_usage(body: &str) -> SseStats {
    let mut stats = SseStats::default();
    for line in body.lines() {
        let line = line.trim();
        if !line.starts_with("data:") {
            continue;
        }
        let payload = line[5..].trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        let evt: serde_json::Value = match serde_json::from_str(payload) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let t = evt.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match t {
            "message_start" => {
                if let Some(m) = evt.get("message") {
                    stats.model = m.get("model").and_then(|v| v.as_str()).map(String::from);
                    stats.tx_id = m.get("id").and_then(|v| v.as_str()).map(String::from);
                    if let Some(u) = m.get("usage") {
                        merge_usage(&mut stats, u);
                    }
                }
            }
            "message_delta" => {
                if let Some(u) = evt.get("usage") {
                    merge_usage(&mut stats, u);
                }
                if let Some(d) = evt.get("delta") {
                    if let Some(s) = d.get("stop_reason").and_then(|v| v.as_str()) {
                        stats.stop_reason = Some(s.to_string());
                    }
                }
            }
            "content_block_start" => {
                let block_idx = evt.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                if let Some(cb) = evt.get("content_block") {
                    match cb.get("type").and_then(|v| v.as_str()) {
                        Some("tool_use") => {
                            let name = cb.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let id = cb.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            stats.tools.push(name.clone());
                            let tc_idx = stats.tool_calls.len();
                            stats.tool_calls.push(ToolCall { name, id, input: String::new() });
                            stats.tool_block_map.push((block_idx, tc_idx));
                        }
                        Some("thinking") => {
                            stats.thinking_blocks += 1;
                        }
                        _ => {}
                    }
                }
            }
            "content_block_delta" => {
                let block_idx = evt.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                if let Some(d) = evt.get("delta") {
                    match d.get("type").and_then(|v| v.as_str()) {
                        Some("text_delta") => {
                            if let Some(t) = d.get("text").and_then(|v| v.as_str()) {
                                stats.assistant_text.push_str(t);
                            }
                        }
                        Some("thinking_delta") => {
                            if let Some(t) = d.get("thinking").and_then(|v| v.as_str()) {
                                stats.thinking_text.push_str(t);
                            }
                        }
                        Some("input_json_delta") => {
                            if let Some(t) = d.get("partial_json").and_then(|v| v.as_str()) {
                                // Find the tool call for this block index.
                                if let Some((_, tc_idx)) = stats.tool_block_map.iter().find(|(bi, _)| *bi == block_idx) {
                                    if let Some(tc) = stats.tool_calls.get_mut(*tc_idx) {
                                        tc.input.push_str(t);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    stats
}

fn merge_usage(s: &mut SseStats, u: &serde_json::Value) {
    if let Some(v) = u.get("input_tokens").and_then(|v| v.as_i64()) {
        s.input_tokens = v;
    }
    if let Some(v) = u.get("output_tokens").and_then(|v| v.as_i64()) {
        s.output_tokens = v;
    }
    if let Some(v) = u.get("cache_read_input_tokens").and_then(|v| v.as_i64()) {
        s.cache_read = v;
    }
    if let Some(v) = u.get("cache_creation_input_tokens").and_then(|v| v.as_i64()) {
        s.cache_creation = v;
    }
    // Granular cache TTL split — present on newer API responses only.
    if let Some(cc) = u.get("cache_creation").and_then(|v| v.as_object()) {
        if let Some(v) = cc.get("ephemeral_5m_input_tokens").and_then(|v| v.as_i64()) {
            s.cache_creation_5m = v;
        }
        if let Some(v) = cc.get("ephemeral_1h_input_tokens").and_then(|v| v.as_i64()) {
            s.cache_creation_1h = v;
        }
    }
}

// Maximum characters we persist per text column. Typical turns are < 10 KB;
// this cap catches pathological file dumps from tool_result blocks without
// letting a single DO grow unbounded.
// No cap — DO SQLite's 10 GB limit is the only bound.
// The truncate_text function is kept as a no-op safety valve
// in case we ever need to re-enable capping.

#[derive(Default)]
struct ParsedRequest {
    /// `model` from the request body. Canonical form comes from the SSE
    /// `message_start` event; this is the caller-supplied alias (e.g.
    /// `claude-opus-4-6`) which is still useful while the turn is in
    /// flight and no SSE frames have arrived yet.
    model: Option<String>,
    max_tokens: Option<i64>,
    thinking_budget: Option<i64>,
    /// JSON array of tool names DECLARED in the request. Kept in the struct
    /// so it can be re-enabled in the broadcast if needed; currently only
    /// `tool_choice` is sent for in-flight rows.
    #[allow(dead_code)]
    tools_json: Option<String>,
    /// Compact representation of the request's `tool_choice` field:
    /// "auto", "any", or "tool:<name>" for forced tool use.
    tool_choice: Option<String>,
    /// Concatenated text of the last user-role message's content blocks:
    /// `text` blocks verbatim, and `tool_result` blocks' string/array text.
    /// `image` blocks are skipped. Truncated to `TEXT_COL_CAP` chars.
    user_text: Option<String>,
}

fn uses_adaptive_thinking(model: &str) -> bool {
    model.starts_with("claude-opus-4-7")
        || model.starts_with("claude-mythos")
}

fn inject_thinking_display(body: &[u8]) -> Option<Vec<u8>> {
    let mut v: serde_json::Value = serde_json::from_slice(body).ok()?;
    let model = v.get("model")?.as_str()?;
    if !uses_adaptive_thinking(model) {
        return None;
    }
    let thinking = v
        .as_object_mut()?
        .entry("thinking")
        .or_insert_with(|| json!({"type": "adaptive"}));
    let obj = thinking.as_object_mut()?;
    if !obj.contains_key("display") {
        obj.insert("display".into(), json!("summarized"));
    }
    serde_json::to_vec(&v).ok()
}

fn parse_request_body(bytes: &[u8]) -> ParsedRequest {
    let v: serde_json::Value = match serde_json::from_slice(bytes) {
        Ok(v) => v,
        Err(_) => return ParsedRequest::default(),
    };
    let model = v
        .get("model")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    let max_tokens = v.get("max_tokens").and_then(|x| x.as_i64());
    let thinking_budget = v
        .get("thinking")
        .and_then(|t| t.as_object())
        .filter(|o| o.get("type").and_then(|x| x.as_str()) == Some("enabled"))
        .and_then(|o| o.get("budget_tokens").and_then(|x| x.as_i64()));

    let tools_json = v
        .get("tools")
        .and_then(|t| t.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(String::from))
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .and_then(|v| serde_json::to_string(&v).ok());

    let tool_choice = v.get("tool_choice").and_then(|tc| {
        let ty = tc.get("type").and_then(|t| t.as_str())?;
        match ty {
            "auto" => Some("auto".to_string()),
            "any" => Some("any".to_string()),
            "tool" => {
                let name = tc.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                Some(format!("tool:{name}"))
            }
            _ => None,
        }
    });

    // Find the LAST user-role message — that's the one carrying the turn's
    // new content. Earlier entries are the replayed conversation prefix and
    // are already captured on their own turn's row.
    let user_text = v
        .get("messages")
        .and_then(|m| m.as_array())
        .and_then(|arr| {
            arr.iter()
                .rev()
                .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("user"))
        })
        .map(|m| extract_user_message_text(m.get("content")))
        .filter(|s: &String| !s.is_empty())
        .map(truncate_text);

    ParsedRequest {
        model,
        max_tokens,
        thinking_budget,
        tools_json,
        tool_choice,
        user_text,
    }
}

// `content` is either a string (short-form) or an array of typed blocks.
fn extract_user_message_text(content: Option<&serde_json::Value>) -> String {
    let Some(content) = content else {
        return String::new();
    };
    if let Some(s) = content.as_str() {
        return s.to_string();
    }
    let Some(blocks) = content.as_array() else {
        return String::new();
    };
    let mut out = String::new();
    for block in blocks {
        let t = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match t {
            "text" => {
                if let Some(s) = block.get("text").and_then(|v| v.as_str()) {
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str(s);
                }
            }
            "tool_result" => {
                // Caller opted into tool_result capture. `content` here is
                // the output the assistant saw (file reads, bash stdout,
                // etc.) — can contain secrets; we don't scrub.
                let inner = block.get("content");
                let extracted = match inner {
                    Some(v) if v.is_string() => v.as_str().unwrap_or("").to_string(),
                    Some(v) if v.is_array() => {
                        // Array of nested blocks — pull out `text` entries.
                        let mut acc = String::new();
                        for b in v.as_array().unwrap() {
                            if let Some(t) = b.get("text").and_then(|x| x.as_str()) {
                                if !acc.is_empty() {
                                    acc.push('\n');
                                }
                                acc.push_str(t);
                            }
                        }
                        acc
                    }
                    _ => String::new(),
                };
                if !extracted.is_empty() {
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str(&extracted);
                }
            }
            // image, document, etc. — skip.
            _ => {}
        }
    }
    out
}

fn truncate_text(s: String) -> String {
    s
}

// Anthropic rate-limit headers come back as plain integers in decimal.
// We read the few that matter and ignore the reset timestamps for now.
fn parse_rate_limits(
    headers: &[(String, String)],
) -> (Option<i64>, Option<i64>, Option<i64>, Option<i64>) {
    let g = |name: &str| -> Option<i64> { header_value(headers, name)?.parse::<i64>().ok() };
    (
        g("anthropic-ratelimit-requests-remaining"),
        g("anthropic-ratelimit-requests-limit"),
        g("anthropic-ratelimit-input-tokens-remaining")
            .or_else(|| g("anthropic-ratelimit-tokens-remaining")),
        g("anthropic-ratelimit-input-tokens-limit")
            .or_else(|| g("anthropic-ratelimit-tokens-limit")),
    )
}

// ---------- Workers AI + Vectorize ----------

const EMBED_MODEL: &str = "@cf/qwen/qwen3-embedding-0.6b";
const EMBED_DIMS: usize = 1024;
// qwen3-embedding-0.6b context is 8192 tokens on CF (~32K chars).
// Truncate from the front so the tail stays in the embedding.
const EMBED_INPUT_CAP: usize = 30_000;

/// Thin `JsValue` wrapper that goes through `env.get_binding` without
/// requiring a specific JS constructor name. Worker 0.8 ships with an `Ai`
/// binding but no first-class Vectorize type, so we drive the binding via
/// `js_sys::Reflect`.
#[repr(transparent)]
struct VectorizeBinding(JsValue);

impl AsRef<JsValue> for VectorizeBinding {
    fn as_ref(&self) -> &JsValue {
        &self.0
    }
}

impl From<VectorizeBinding> for JsValue {
    fn from(b: VectorizeBinding) -> Self {
        b.0
    }
}

impl JsCast for VectorizeBinding {
    fn instanceof(_: &JsValue) -> bool {
        true
    }
    fn unchecked_from_js(val: JsValue) -> Self {
        Self(val)
    }
    fn unchecked_from_js_ref(val: &JsValue) -> &Self {
        unsafe { &*(val as *const JsValue as *const Self) }
    }
}

impl EnvBinding for VectorizeBinding {
    // Actual class varies in the runtime; default `get` matches on this
    // string, so we override below to accept whatever the binding is.
    const TYPE_NAME: &'static str = "Vectorize";
    fn get(val: JsValue) -> Result<Self> {
        if val.is_undefined() {
            Err("VECTORIZE binding is undefined".into())
        } else {
            Ok(Self(val))
        }
    }
}

/// Call a JS method by name that returns a Promise, await it.
async fn call_js_method(
    this: &JsValue,
    name: &str,
    args: &[JsValue],
) -> std::result::Result<JsValue, JsValue> {
    let f = Reflect::get(this, &JsValue::from_str(name))?;
    let f: js_sys::Function = f.dyn_into()?;
    let args_arr = Array::new();
    for a in args {
        args_arr.push(a);
    }
    let ret = f.apply(this, &args_arr)?;
    let promise: js_sys::Promise = ret.dyn_into()?;
    JsFuture::from(promise).await
}

/// Call Workers AI to produce a 768-dim embedding for `text`. Returns None
/// on any error — callers treat Vectorize indexing as best-effort. Each
/// failure mode logs a distinct `embed_err` sub-kind so the root cause is
/// visible in `wrangler tail` without recompiling.
async fn embed_text(env: &Env, text: &str) -> Option<Vec<f32>> {
    if text.is_empty() {
        return None;
    }
    let ai = match env.ai("AI") {
        Ok(a) => a,
        Err(e) => {
            console_log!(
                "{{\"dir\":\"embed_err\",\"stage\":\"binding\",\"err\":\"{}\"}}",
                e
            );
            return None;
        }
    };
    let trimmed = if text.len() <= EMBED_INPUT_CAP {
        text
    } else {
        let start = text.len() - EMBED_INPUT_CAP;
        let mut s = start;
        while s < text.len() && !text.is_char_boundary(s) {
            s += 1;
        }
        &text[s..]
    };
    // Workers AI expects a plain JS object. `serde_json::Value::Object` gets
    // serialized as a JS Map by serde-wasm-bindgen (worker-rs wraps that
    // under the hood), which the AiError: 5006 validator silently rejects.
    // A #[derive(Serialize)] struct serializes as a plain object, which
    // matches the model's schema.
    #[derive(Serialize)]
    struct EmbedInput<'a> {
        text: Vec<&'a str>,
    }
    let input = EmbedInput { text: vec![trimmed] };
    let out: serde_json::Value = match ai.run(EMBED_MODEL, input).await {
        Ok(v) => v,
        Err(e) => {
            console_log!(
                "{{\"dir\":\"embed_err\",\"stage\":\"run\",\"err\":\"{:?}\"}}",
                e
            );
            return None;
        }
    };
    let row = match out.get("data").and_then(|d| d.as_array()).and_then(|a| a.first()).and_then(|f| f.as_array()) {
        Some(r) => r,
        None => {
            console_log!(
                "{{\"dir\":\"embed_err\",\"stage\":\"shape\",\"body\":{}}}",
                out
            );
            return None;
        }
    };
    if row.len() != EMBED_DIMS {
        console_log!(
            "{{\"dir\":\"embed_err\",\"stage\":\"dims\",\"got\":{},\"want\":{}}}",
            row.len(),
            EMBED_DIMS
        );
        return None;
    }
    Some(
        row.iter()
            .map(|v| v.as_f64().unwrap_or(0.0) as f32)
            .collect(),
    )
}

/// Batch upsert for backfill and any other caller with multiple vectors
/// ready at once. Vectorize accepts an array of records in a single JS
/// call, so this amortizes the ~400ms round-trip across the whole batch.
pub struct BackfillVector<'a> {
    pub tx_id: &'a str,
    pub session_id: Option<&'a str>,
    pub ts: i64,
    pub values: Vec<f32>,
}

async fn vectorize_upsert_many(
    env: &Env,
    user_hash: &str,
    items: &[BackfillVector<'_>],
) -> std::result::Result<(), String> {
    if items.is_empty() {
        return Ok(());
    }
    let vec_binding = env
        .get_binding::<VectorizeBinding>("VECTORIZE")
        .map_err(|e| format!("binding: {e}"))?;

    #[derive(Serialize)]
    struct Metadata<'a> {
        session_id: &'a str,
        ts: i64,
    }
    #[derive(Serialize)]
    struct VectorRecord<'a> {
        id: String,
        values: &'a [f32],
        namespace: &'a str,
        metadata: Metadata<'a>,
    }

    let records: Vec<VectorRecord<'_>> = items
        .iter()
        .map(|it| VectorRecord {
            id: format!("{}:{}", user_hash, it.tx_id),
            values: &it.values,
            namespace: user_hash,
            metadata: Metadata {
                session_id: it.session_id.unwrap_or(""),
                ts: it.ts,
            },
        })
        .collect();

    let arg = serde_wasm_bindgen::to_value(&records).map_err(|e| format!("encode: {e}"))?;
    call_js_method(&vec_binding.0, "upsert", &[arg])
        .await
        .map(|_| ())
        .map_err(|e| format!("upsert: {:?}", e))
}

async fn vectorize_upsert(
    env: &Env,
    user_hash: &str,
    tx_id: &str,
    session_id: Option<&str>,
    ts: i64,
    values: &[f32],
) -> std::result::Result<(), String> {
    let vec_binding = env
        .get_binding::<VectorizeBinding>("VECTORIZE")
        .map_err(|e| format!("binding: {e}"))?;

    // Per-user `namespace` is the hard isolation boundary — queries against
    // one namespace physically cannot see vectors in another. The id prefix
    // is kept as a belt-and-suspenders uniqueness guard (vector IDs share a
    // keyspace within the index). Use #[derive(Serialize)] structs so
    // serde-wasm-bindgen emits plain JS objects (not Maps) — Vectorize's
    // validator chokes on the latter.
    #[derive(Serialize)]
    struct Metadata<'a> {
        session_id: &'a str,
        ts: i64,
    }
    #[derive(Serialize)]
    struct VectorRecord<'a> {
        id: String,
        values: &'a [f32],
        namespace: &'a str,
        metadata: Metadata<'a>,
    }
    let record = VectorRecord {
        id: format!("{}:{}", user_hash, tx_id),
        values,
        namespace: user_hash,
        metadata: Metadata {
            session_id: session_id.unwrap_or(""),
            ts,
        },
    };
    let vectors = vec![record];
    let arg = serde_wasm_bindgen::to_value(&vectors).map_err(|e| format!("encode: {e}"))?;

    call_js_method(&vec_binding.0, "upsert", &[arg])
        .await
        .map(|_| ())
        .map_err(|e| format!("upsert: {:?}", e))
}

/// Query Vectorize for top-K nearest vectors under the caller's user_hash.
async fn vectorize_query(
    env: &Env,
    user_hash: &str,
    query_vec: &[f32],
    top_k: usize,
) -> std::result::Result<Vec<(String, f64)>, String> {
    let vec_binding = env
        .get_binding::<VectorizeBinding>("VECTORIZE")
        .map_err(|e| format!("binding: {e}"))?;

    let values: Vec<f64> = query_vec.iter().map(|&x| x as f64).collect();
    let q_arg = serde_wasm_bindgen::to_value(&values).map_err(|e| format!("encode q: {e}"))?;
    #[derive(Serialize)]
    struct QueryOpts<'a> {
        #[serde(rename = "topK")]
        top_k: usize,
        namespace: &'a str,
        #[serde(rename = "returnMetadata")]
        return_metadata: &'static str,
    }
    let opts = QueryOpts {
        top_k,
        namespace: user_hash,
        return_metadata: "all",
    };
    let opts_arg = serde_wasm_bindgen::to_value(&opts).map_err(|e| format!("encode opts: {e}"))?;

    let raw = call_js_method(&vec_binding.0, "query", &[q_arg, opts_arg])
        .await
        .map_err(|e| format!("query: {:?}", e))?;

    // Response shape: { matches: [{id, score, metadata?}], count }
    let parsed: serde_json::Value = serde_wasm_bindgen::from_value(raw)
        .map_err(|e| format!("decode: {e}"))?;
    let matches = parsed
        .get("matches")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::with_capacity(matches.len());
    for m in matches {
        let Some(id) = m.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        // Strip the user_hash prefix so callers see the raw tx_id.
        let tx_id = id
            .strip_prefix(&format!("{}:", user_hash))
            .unwrap_or(id)
            .to_string();
        let score = m.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        out.push((tx_id, score));
    }
    Ok(out)
}

// ---------- /_cm/search ----------

/// Max chars rendered in a search result snippet. Also sets the padding budget
/// for the windowing helper.
const SNIPPET_CHARS: usize = 220;

/// Wrap each whitespace-split token from the user's query in double quotes so
/// FTS5's MATCH parser treats them as literal phrases instead of operators.
///
/// Rationale: FTS5 parses `-`/`AND`/`OR`/`NOT`/`:`/`^` as operators in the
/// unquoted grammar. `future-me` becomes `future NOT me`, which returns
/// nothing because "me" is in nearly every turn. `session_id` mostly works
/// (implicit AND of `session` and `id` after tokenization) but can interact
/// badly with adjacent operator chars. Quoting each token forces phrase-query
/// semantics, which pass through the tokenizer normally but skip operator
/// parsing. Internal embedded `"` chars are replaced with spaces before
/// wrapping, so we can't produce malformed MATCH syntax.
fn sanitize_fts_query(q: &str) -> String {
    query_tokens(q)
        .into_iter()
        .map(|tok| format!("\"{}\"", tok))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Split the query on whitespace, strip embedded `"` (would break our own
/// quoting), drop any tokens shorter than 2 chars (FTS5 signal-to-noise and
/// windowing stability). Returns the raw token list; `sanitize_fts_query`
/// wraps these for MATCH, `vector_search` uses them for snippet windowing.
fn query_tokens(q: &str) -> Vec<String> {
    q.split_whitespace()
        .flat_map(|tok| {
            tok.replace('"', " ")
                .split_whitespace()
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .filter(|tok| tok.chars().count() >= 2)
        .collect()
}

/// Produce a ~`total_chars`-char snippet centered on the first occurrence of
/// any query token in `text`. Uses `<mark>`/`</mark>` around the matched
/// span to match the visual style of FTS5's `snippet()` output. Falls back
/// to a head prefix (no `<mark>`) when no token is lexically present — this
/// is the true-semantic-match case.
///
/// ASCII-case-insensitive only: `to_ascii_lowercase` preserves byte offsets
/// so the byte position from `find()` is valid in the original text. For
/// non-ASCII content the match is case-sensitive — acceptable for this use.
fn window_snippet(text: &str, tokens: &[String], total_chars: usize) -> String {
    if text.is_empty() {
        return String::new();
    }
    let text_lower = text.to_ascii_lowercase();
    for token in tokens {
        let tok_lower = token.to_ascii_lowercase();
        if let Some(byte_pos) = text_lower.find(&tok_lower) {
            return make_window(text, byte_pos, tok_lower.len(), total_chars);
        }
    }
    head_snippet(text, total_chars)
}

/// Build the windowed output around a byte-offset match. Walks char_indices
/// to stay on UTF-8 boundaries and counts characters (not bytes) toward the
/// budget, so multi-byte content doesn't blow up the rendered length.
fn make_window(text: &str, match_byte_pos: usize, match_byte_len: usize, total_chars: usize) -> String {
    let match_byte_end = match_byte_pos + match_byte_len;
    // Map byte offsets to char indices.
    let char_start = text[..match_byte_pos].chars().count();
    let char_end = text[..match_byte_end].chars().count();
    let match_chars = char_end - char_start;
    let text_chars: Vec<char> = text.chars().collect();
    let total = text_chars.len();
    if match_chars >= total_chars {
        return head_snippet(text, total_chars);
    }
    let pad = (total_chars - match_chars) / 2;
    let window_start = char_start.saturating_sub(pad);
    let window_end = (char_end + pad).min(total);
    let mut out = String::new();
    if window_start > 0 {
        out.push('…');
    }
    out.extend(text_chars[window_start..char_start].iter());
    out.push_str("<mark>");
    out.extend(text_chars[char_start..char_end].iter());
    out.push_str("</mark>");
    out.extend(text_chars[char_end..window_end].iter());
    if window_end < total {
        out.push('…');
    }
    out
}

/// Fallback snippet: first `n` chars of `text`, with an ellipsis if truncated.
fn head_snippet(text: &str, n: usize) -> String {
    let total = text.chars().count();
    if total <= n {
        text.to_string()
    } else {
        let mut out: String = text.chars().take(n).collect();
        out.push('…');
        out
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct SearchHit {
    tx_id: String,
    ts: i64,
    // Most-recent ts across all turns in this session, from session_summaries.
    // The UI prefers this over `ts` so a match in a still-active session
    // displays as recent. Falls back to `ts` on the frontend when None
    // (orphan rows without a session_summaries entry).
    #[serde(skip_serializing_if = "Option::is_none")]
    session_last_ts: Option<i64>,
    session_id: Option<String>,
    model: Option<String>,
    user_snip: Option<String>,
    asst_snip: Option<String>,
    #[serde(default)]
    score: f64,
    match_source: String,
}

// Thin forwarder: all orchestration lives in UserStore::search (DO-side).
// We augment the caller's body with their resolved user_hash so the DO can
// talk to Vectorize with the right id-prefix + metadata filter.
async fn handle_search(user_hash: &str, body: &[u8], env: &Env) -> Result<Response> {
    let mut v: serde_json::Value = serde_json::from_slice(body).unwrap_or(json!({}));
    if let Some(obj) = v.as_object_mut() {
        obj.insert("user_hash".into(), json!(user_hash));
    } else {
        v = json!({ "user_hash": user_hash });
    }
    let augmented = serde_json::to_vec(&v).unwrap_or_default();

    let ns = env.durable_object("USER_STORE")?;
    let stub = ns.id_from_name(user_hash)?.get_stub()?;

    let arr = Uint8Array::from(&augmented[..]);
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(arr.into()));
    let headers = Headers::new();
    headers.append("content-type", "application/json").ok();
    init.with_headers(headers);
    let req = Request::new_with_init("https://store/search", &init)?;
    stub.fetch_with_request(req).await
}

// Forwarder for the one-off re-embed/re-upsert of historical turns. Body
// mirrors the DO's `/vectorize/backfill` schema (`batch_size`, `before_ts`);
// we inject the resolved `user_hash` the same way `handle_search` does.
async fn handle_vectorize_backfill(user_hash: &str, body: &[u8], env: &Env) -> Result<Response> {
    let mut v: serde_json::Value = serde_json::from_slice(body).unwrap_or(json!({}));
    if let Some(obj) = v.as_object_mut() {
        obj.insert("user_hash".into(), json!(user_hash));
    } else {
        v = json!({ "user_hash": user_hash });
    }
    let augmented = serde_json::to_vec(&v).unwrap_or_default();

    let ns = env.durable_object("USER_STORE")?;
    let stub = ns.id_from_name(user_hash)?.get_stub()?;

    let arr = Uint8Array::from(&augmented[..]);
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(arr.into()));
    let headers = Headers::new();
    headers.append("content-type", "application/json").ok();
    init.with_headers(headers);
    let req = Request::new_with_init("https://store/vectorize/backfill", &init)?;
    stub.fetch_with_request(req).await
}

// POST {sql} to a stub's /sql handler and return the `rows` array from
// the response. Used by migrate_in to read out the legacy DO's tables.
async fn fetch_rows_from_stub(
    stub: &worker::durable::Stub,
    sql_query: &str,
) -> Result<Vec<serde_json::Value>> {
    let body = json!({ "sql": sql_query });
    let body_bytes = serde_json::to_vec(&body).unwrap_or_default();
    let arr = Uint8Array::from(&body_bytes[..]);
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(arr.into()));
    let h = Headers::new();
    h.append("content-type", "application/json").ok();
    init.with_headers(h);
    let req = Request::new_with_init("https://store/sql", &init)?;
    let mut resp = stub.fetch_with_request(req).await?;
    let v: serde_json::Value = resp.json().await?;
    Ok(v.get("rows")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default())
}

// Thin forwarder for /_cm/admin/migrate-legacy. The proxy has already
// resolved target_hash (email-keyed, the caller's current identity) and
// source_hash (the cached uuid-keyed hash from pre-email-flip). We fire the
// copy at the *target* DO so it writes into itself via a DO-to-DO fetch
// against the source — no cross-instance state lock issues, and all the
// schema is already initialized there.
async fn handle_migrate_legacy(
    target_hash: &str,
    source_hash: &str,
    env: &Env,
) -> Result<Response> {
    let body = json!({ "source_hash": source_hash });
    let body_bytes = serde_json::to_vec(&body).unwrap_or_default();

    let ns = env.durable_object("USER_STORE")?;
    let stub = ns.id_from_name(target_hash)?.get_stub()?;

    let arr = Uint8Array::from(&body_bytes[..]);
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(arr.into()));
    let headers = Headers::new();
    headers.append("content-type", "application/json").ok();
    init.with_headers(headers);
    let req = Request::new_with_init("https://store/migrate-in", &init)?;
    stub.fetch_with_request(req).await
}

fn hits_from_rows(rows: Vec<serde_json::Value>, source: &str) -> Vec<SearchHit> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let hit = SearchHit {
            tx_id: row
                .get("tx_id")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string(),
            ts: row.get("ts").and_then(|x| x.as_i64()).unwrap_or_default(),
            session_last_ts: row.get("session_last_ts").and_then(|x| x.as_i64()),
            session_id: row
                .get("session_id")
                .and_then(|x| x.as_str())
                .map(String::from),
            model: row.get("model").and_then(|x| x.as_str()).map(String::from),
            user_snip: row
                .get("user_snip")
                .and_then(|x| x.as_str())
                .map(String::from),
            asst_snip: row
                .get("asst_snip")
                .and_then(|x| x.as_str())
                .map(String::from),
            score: row.get("score").and_then(|x| x.as_f64()).unwrap_or(0.0),
            match_source: source.to_string(),
        };
        if !hit.tx_id.is_empty() {
            out.push(hit);
        }
    }
    out
}

/// RRF: score = Σ 1 / (k + rank). k=60 is the standard constant (Cormack et al. 2009).
fn reciprocal_rank_fusion(
    fts: Vec<SearchHit>,
    vec: Vec<SearchHit>,
    limit: usize,
) -> Vec<SearchHit> {
    const K: f64 = 60.0;
    let mut by_id: std::collections::HashMap<String, (SearchHit, f64, bool, bool)> =
        std::collections::HashMap::new();

    for (rank, hit) in fts.into_iter().enumerate() {
        let key = hit.tx_id.clone();
        let contrib = 1.0 / (K + (rank + 1) as f64);
        by_id
            .entry(key)
            .and_modify(|(_, s, fts_seen, _)| {
                *s += contrib;
                *fts_seen = true;
            })
            .or_insert((hit, contrib, true, false));
    }
    for (rank, hit) in vec.into_iter().enumerate() {
        let key = hit.tx_id.clone();
        let contrib = 1.0 / (K + (rank + 1) as f64);
        by_id
            .entry(key)
            .and_modify(|(existing, s, _, vec_seen)| {
                *s += contrib;
                *vec_seen = true;
                if existing.user_snip.is_none() {
                    existing.user_snip = hit.user_snip.clone();
                }
                if existing.asst_snip.is_none() {
                    existing.asst_snip = hit.asst_snip.clone();
                }
            })
            .or_insert((hit, contrib, false, true));
    }

    let mut merged: Vec<SearchHit> = by_id
        .into_values()
        .map(|(mut h, score, fts_seen, vec_seen)| {
            h.score = score;
            h.match_source = match (fts_seen, vec_seen) {
                (true, true) => "both".into(),
                (true, false) => "fts".into(),
                (false, true) => "vector".into(),
                _ => "unknown".into(),
            };
            h
        })
        .collect();
    merged.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    merged.truncate(limit);
    merged
}

// ---------- Identity ----------

fn header_value(entries: &[(String, String)], name: &str) -> Option<String> {
    entries
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

// Resolve the caller's stable user_hash.
//
// OAuth bearers rotate every few hours, so hashing the raw token produces a
// new user_hash on every refresh. Instead we resolve the bearer to Anthropic's
// `account.uuid` (immutable per-account) via the /api/oauth/profile endpoint,
// cache the token → uuid/email mapping in KV (1h TTL, matches the bearer's own
// lifetime), and hash the uuid. Result: user_hash is stable across refreshes,
// devices, and sessions.
//
// Side-effect: resolving the profile also writes `link:<email> = user_hash`
// so the dashboard (CF-Access-authenticated by the same email) auto-scopes
// without any manual setup step.
//
// API keys hash directly — they're already stable identifiers.
// Returns (hash, email, legacy_uuid_hash). legacy_uuid_hash is the
// pre-email-flip hash — exposed so /_cm/admin/migrate-legacy can find
// the caller's old DO without asking them to dig it up manually.
async fn compute_user_hash(
    entries: &[(String, String)],
    salt: &str,
    env: &Env,
) -> Option<(String, Option<String>, Option<String>)> {
    if let Some(raw_auth) = header_value(entries, "authorization") {
        let token = raw_auth
            .strip_prefix("Bearer ")
            .unwrap_or(&raw_auth)
            .to_string();
        return resolve_oauth_hash(&token, salt, env).await;
    }
    if let Some(api_key) = header_value(entries, "x-api-key") {
        return Some((hash_identity(salt, "apikey:", &api_key), None, None));
    }
    None
}

// Comma-separated email allowlist for privileged operations — currently
// just the cross-DO `hash` override on /_cm/admin/sql. Read fresh from the
// secret each check so a rotation lands without a redeploy. Unset or empty
// means "no one" (safe default — no request grants cross-DO access).
fn is_admin_email(email: Option<&str>, env: &Env) -> bool {
    let Some(email) = email else {
        return false;
    };
    let caller = email.trim().to_ascii_lowercase();
    if caller.is_empty() {
        return false;
    }
    let Ok(raw) = env.secret("ADMIN_EMAILS").map(|s| s.to_string()) else {
        return false;
    };
    raw.split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .any(|allowed| allowed == caller)
}

fn hash_identity(salt: &str, prefix: &str, id: &str) -> String {
    let mut h = Sha256::new();
    h.update(salt.as_bytes());
    h.update(prefix.as_bytes());
    h.update(id.as_bytes());
    hex::encode(&h.finalize()[..8])
}

// Returns None when profile + KV are both unavailable. The old code hashed
// the raw bearer in that case as a "degraded" fallback, but since tokens
// rotate, every transient failure spawned a fresh ghost DO. Returning None
// lets the proxy still passthrough to Anthropic (Claude Code keeps working)
// and silently skips ingestion for the duration of the blip — next
// successful resolve auto-rejoins the user's real DO.
async fn resolve_oauth_hash(
    token: &str,
    salt: &str,
    env: &Env,
) -> Option<(String, Option<String>, Option<String>)> {
    let token_id = hash_identity(salt, "tok:", token);
    let cache_key = format!("tok:{}", token_id);

    // KV hit: {uuid}|{email}. One round-trip to KV, skip the profile fetch.
    // We keep uuid in the cache value for forensics even though the hash now
    // derives from email — email is the durable anchor (stable across the
    // obscure "account merged / uuid rekeyed" edge case, and it's what the
    // dashboard's Google SSO sees), uuid is just there for debugging.
    if let Ok(kv) = env.kv("SESSION") {
        if let Ok(Some(cached)) = kv.get(&cache_key).text().await {
            if let Some((uuid, email)) = cached.split_once('|') {
                let hash = hash_identity(salt, "email:", email);
                let legacy = hash_identity(salt, "uuid:", uuid);
                return Some((hash, Some(email.to_string()), Some(legacy)));
            }
        }

        // KV miss: fetch profile, cache, hash.
        if let Some((uuid, email)) = fetch_anthropic_profile(token).await {
            let value = format!("{}|{}", uuid, email);
            if let Ok(builder) = kv.put(&cache_key, value) {
                let _ = builder.expiration_ttl(3600).execute().await;
            }
            let hash = hash_identity(salt, "email:", &email);
            let legacy = hash_identity(salt, "uuid:", &uuid);
            return Some((hash, Some(email), Some(legacy)));
        }
    }

    console_log!(
        "{{\"dir\":\"hash_skip\",\"reason\":\"profile_or_kv_unavailable\"}}"
    );
    None
}

async fn fetch_anthropic_profile(token: &str) -> Option<(String, String)> {
    let headers = Headers::new();
    headers
        .append("authorization", &format!("Bearer {}", token))
        .ok()?;
    headers.append("anthropic-beta", "oauth-2025-04-20").ok();
    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    init.with_headers(headers);
    let req = Request::new_with_init(
        "https://api.anthropic.com/api/oauth/profile",
        &init,
    )
    .ok()?;
    let mut resp = Fetch::Request(req).send().await.ok()?;
    if resp.status_code() != 200 {
        return None;
    }
    let v: serde_json::Value = resp.json().await.ok()?;
    let uuid = v.pointer("/account/uuid").and_then(|x| x.as_str())?;
    let email = v.pointer("/account/email").and_then(|x| x.as_str())?;
    Some((uuid.to_string(), email.trim().to_lowercase()))
}

async fn auto_link(kv: &KvStore, email: &str, hash: &str) {
    let email_norm = email.trim().to_lowercase();
    if email_norm.is_empty() {
        return;
    }
    let key = format!("link:{}", email_norm);
    // Skip the write if the link already points at this hash. KV writes are
    // cheap but visible in metrics; idempotence is free.
    if let Ok(Some(existing)) = kv.get(&key).text().await {
        if existing == hash {
            return;
        }
    }
    if let Ok(builder) = kv.put(&key, hash) {
        let _ = builder.execute().await;
    }
}

// "Has this caller's email ever been registered via a /v1/* proxy hit?"
// The `link:<email>` key is written by auto_link only from the main proxy
// path, so existence here == "they've sent real traffic to this
// deployment." Admin routes gate on this.
async fn caller_has_link(env: &Env, email: &str) -> bool {
    let email_norm = email.trim().to_lowercase();
    if email_norm.is_empty() {
        return false;
    }
    let Ok(kv) = env.kv("SESSION") else {
        return false;
    };
    let key = format!("link:{}", email_norm);
    matches!(kv.get(&key).text().await, Ok(Some(_)))
}

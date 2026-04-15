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
const DEFAULT_SALT: &str = "claudemetry-dev-unset";

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
        return admin_route(&path, &method, &body, &req_headers_vec, &salt, &env).await;
    }

    let req_body_bytes = req.bytes().await.unwrap_or_default();
    let req_body_len = req_body_bytes.len() as i64;

    let user_hash = compute_user_hash(&req_headers_vec, &salt, &env)
        .await
        .map(|(h, _email)| h);
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

    // Synthetic, stable row PK: carried through placeholder → finalize so
    // the dashboard keys don't flap when the turn completes. Anthropic's
    // message.id (once we see it in the SSE stream) goes into
    // `anthropic_message_id` instead.
    let placeholder_tx_id = format!(
        "inflight-{}-{:08x}",
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
    let placeholder_stub = acquire_stub();
    let finalize_stub = acquire_stub();

    // Fire the placeholder write BEFORE awaiting upstream. Runs in the
    // background alongside the upstream fetch, so the dashboard sees an
    // in_flight=1 row within one DO round-trip of the request arriving —
    // even if the upstream takes 30s to finish streaming.
    if let Some(stub) = placeholder_stub {
        let placeholder = TransactionRecord {
            tx_id: placeholder_tx_id.clone(),
            ts: start,
            session_id: session_id.clone(),
            method: method.to_string(),
            url: target.clone(),
            req_body_bytes: req_body_len,
            in_flight: Some(1),
            ..Default::default()
        };
        ctx.wait_until(async move {
            post_record_to_do(&stub, "/ingest/start", &placeholder).await;
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
        let ParsedRequest {
            max_tokens,
            thinking_budget,
            user_text,
        } = parse_request_body(&req_body_for_parse);
        let assistant_text = if stats.assistant_text.is_empty() {
            None
        } else {
            Some(stats.assistant_text.clone())
        };
        let (rl_req_remaining, rl_req_limit, rl_tok_remaining, rl_tok_limit) =
            parse_rate_limits(&resp_headers_vec);

        let record = TransactionRecord {
            tx_id: placeholder_tx_id,
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
            in_flight: Some(0),
            anthropic_message_id,
            user_text,
            assistant_text,
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

        if let Some(stub) = finalize_stub {
            post_record_to_do(&stub, "/ingest/finalize", &record).await;

            // Best-effort Vectorize upsert. Failures are logged but never
            // block the SQLite finalize, which already succeeded above.
            if let Some(uh) = user_hash.as_deref() {
                let combined = format!(
                    "{}\n---\n{}",
                    record.user_text.as_deref().unwrap_or(""),
                    record.assistant_text.as_deref().unwrap_or(""),
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
    method: &Method,
    body: &[u8],
    headers: &[(String, String)],
    salt: &str,
    env: &Env,
) -> Result<Response> {
    let (user_hash, email) = match compute_user_hash(headers, salt, env).await {
        Some(pair) => pair,
        None => return Response::error("missing authorization", 401),
    };

    if path == "/_cm/whoami" {
        return Response::from_json(&json!({
            "user_hash": user_hash,
            "email": email,
        }));
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

    // Backfill re-embeds every finalized turn in the caller's DO and upserts
    // it under `namespace=<user_hash>`. Needed one-off when vectors predate
    // the namespace migration; safe to re-run (upsert is idempotent on id).
    if path == "/_cm/admin/vectorize-backfill" && method == &Method::Post {
        return handle_vectorize_backfill(&user_hash, &forwarded_body, env).await;
    }

    let (inner_method, inner_path) = match (method, path) {
        (&Method::Get, "/_cm/recent") => (Method::Get, "/recent"),
        (&Method::Get, "/_cm/stats") => (Method::Get, "/stats"),
        (&Method::Get, "/_cm/sessions/ends") => (Method::Get, "/session/ends"),
        (&Method::Post, "/_cm/session/end") => (Method::Post, "/session/end"),
        (&Method::Post, "/_cm/turn") => (Method::Post, "/turn"),
        (&Method::Post, "/_cm/admin/sql") => {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&forwarded_body) {
                if let Some(h) = v.get("hash").and_then(|x| x.as_str()) {
                    if !is_hex16(h) {
                        return Response::error("hash must be 16 hex chars", 400);
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

    let inner_url = format!("https://store{}", inner_path);
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
            (Method::Post, "/ingest") => self.ingest(&mut req).await,
            (Method::Post, "/ingest/start") => self.ingest_start(&mut req).await,
            (Method::Post, "/ingest/finalize") => self.ingest_finalize(&mut req).await,
            (Method::Post, "/session/end") => self.end_session(&mut req).await,
            (Method::Get, "/session/ends") => self.session_ends().await,
            (Method::Get, "/recent") => self.recent().await,
            (Method::Get, "/stats") => self.stats().await,
            (Method::Post, "/sql") => self.sql_exec(&mut req).await,
            (Method::Post, "/search/fts") => self.search_fts(&mut req).await,
            (Method::Post, "/search/hydrate") => self.search_hydrate(&mut req).await,
            (Method::Post, "/search") => self.search(&mut req).await,
            (Method::Post, "/turn") => self.fetch_turn(&mut req).await,
            (Method::Post, "/vectorize/backfill") => self.vectorize_backfill(&mut req).await,
            _ => Response::error("not found", 404),
        }
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
            // 1 while the proxy's still waiting on the upstream SSE stream.
            // Flipped to 0 by /ingest/finalize. Dashboard renders a spinner
            // while this is 1 and suppresses metric columns (they're all 0).
            ("in_flight", "INTEGER"),
            // Anthropic's `message.id` from message_start. Row PK is now a
            // synthetic `inflight-<ts>-<rand>` so that placeholder → finalize
            // doesn't mutate the PK (which the dashboard keys rows by).
            ("anthropic_message_id", "TEXT"),
            // Free-text search columns. Populated on finalize: the last
            // user-role message's text (incl. tool_result content) and the
            // assistant's text_delta stream output.
            ("user_text", "TEXT"),
            ("assistant_text", "TEXT"),
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
        // Partial index keeps the per-init stale-sweep UPDATE cheap: it only
        // touches rows that are actually in flight, which is ~0 at rest.
        let _ = sql.exec(
            "CREATE INDEX IF NOT EXISTS idx_in_flight ON transactions(ts) WHERE in_flight = 1",
            None,
        );
        // Stale sweep: if a worker was evicted between /ingest/start and
        // /ingest/finalize, the placeholder row would stay in_flight=1
        // forever. On each fresh DO instance we flip anything older than
        // 5 min to an error terminal state.
        let cutoff = (Date::now().as_millis() as i64) - 300_000;
        let _ = sql.exec(
            "UPDATE transactions
             SET in_flight = 0,
                 stop_reason = COALESCE(stop_reason, 'error')
             WHERE in_flight = 1 AND ts < ?",
            Some(vec![cutoff.into()]),
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
        Response::from_json(&json!({ "session_id": b.session_id, "ended_at": ended_at }))
    }

    async fn ingest(&self, req: &mut Request) -> Result<Response> {
        let r: TransactionRecord = req.json().await?;
        self.insert_or_replace(&r)?;
        Response::ok("ok")
    }

    // Placeholder write fired from the proxy before the upstream fetch even
    // returns. Row carries in_flight=1 and zeros/nulls for metric columns
    // until /ingest/finalize lands.
    async fn ingest_start(&self, req: &mut Request) -> Result<Response> {
        let mut r: TransactionRecord = req.json().await?;
        // Proxy sets in_flight=1, but belt-and-braces.
        r.in_flight = Some(1);
        // INSERT OR IGNORE so a retried /start doesn't clobber a finalize
        // that somehow raced ahead of it.
        self.insert_or_ignore(&r)?;
        Response::ok("ok")
    }

    // Finalize: overwrite the placeholder in-place (same synthetic tx_id).
    // If the placeholder somehow never landed (worker eviction between
    // /start and /finalize), INSERT OR REPLACE creates the row from
    // scratch with the full payload.
    async fn ingest_finalize(&self, req: &mut Request) -> Result<Response> {
        let mut r: TransactionRecord = req.json().await?;
        r.in_flight = Some(0);
        self.insert_or_replace(&r)?;
        Response::ok("ok")
    }

    fn insert_or_replace(&self, r: &TransactionRecord) -> Result<()> {
        self.write_row("INSERT OR REPLACE", r)
    }

    fn insert_or_ignore(&self, r: &TransactionRecord) -> Result<()> {
        self.write_row("INSERT OR IGNORE", r)
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
              in_flight, anthropic_message_id,
              user_text, assistant_text)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
                r.in_flight.into(),
                r.anthropic_message_id.clone().into(),
                r.user_text.clone().into(),
                r.assistant_text.clone().into(),
            ]),
        )?;
        Ok(())
    }

    async fn recent(&self) -> Result<Response> {
        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT tx_id, ts, session_id, method, url, model, status, elapsed_ms,
                    input_tokens, output_tokens, cache_read, cache_creation,
                    stop_reason, tools_json, req_body_bytes, resp_body_bytes,
                    cache_creation_5m, cache_creation_1h,
                    thinking_budget, thinking_blocks, max_tokens,
                    rl_req_remaining, rl_req_limit,
                    rl_tok_remaining, rl_tok_limit,
                    in_flight, anthropic_message_id,
                    user_text, assistant_text
             FROM transactions ORDER BY ts DESC",
            None,
        )?;
        let rows: Vec<serde_json::Value> = cursor.to_array()?;
        Response::from_json(&rows)
    }

    async fn stats(&self) -> Result<Response> {
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
                MAX(ts) AS last_ts
             FROM transactions",
            None,
        )?;
        let rows: Vec<serde_json::Value> = cursor.to_array()?;
        let mut summary = rows.into_iter().next().unwrap_or(json!({}));
        if let Some(obj) = summary.as_object_mut() {
            obj.insert("storage_bytes".into(), json!(sql.database_size() as i64));
        }
        Response::from_json(&summary)
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
    fn fts_search_rows(&self, q: &str, limit: i64) -> Result<Vec<serde_json::Value>> {
        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT t.tx_id, t.ts, t.session_id, t.model,
                    snippet(transactions_fts, 0, '<mark>', '</mark>', '…', 10) AS user_snip,
                    snippet(transactions_fts, 1, '<mark>', '</mark>', '…', 10) AS asst_snip,
                    -bm25(transactions_fts) AS score
             FROM transactions_fts
             JOIN transactions t ON t.rowid = transactions_fts.rowid
             WHERE transactions_fts MATCH ?
             ORDER BY bm25(transactions_fts) ASC
             LIMIT ?",
            Some(vec![q.into(), limit.into()]),
        )?;
        Ok(cursor.to_array().unwrap_or_default())
    }

    // Hydrate a set of tx_ids (from Vectorize) into rows with plain-substring
    // snippets. No MATCH means no `snippet()` highlights — callers that want
    // highlights should use fts_search_rows.
    fn hydrate_rows(&self, tx_ids: &[String]) -> Result<Vec<serde_json::Value>> {
        if tx_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<String> = tx_ids.iter().take(200).cloned().collect();
        let placeholders = vec!["?"; ids.len()].join(",");
        let query = format!(
            "SELECT tx_id, ts, session_id, model,
                    substr(COALESCE(user_text, ''), 1, 200) AS user_snip,
                    substr(COALESCE(assistant_text, ''), 1, 200) AS asst_snip
             FROM transactions WHERE tx_id IN ({})",
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
        if b.q.trim().is_empty() {
            return Response::error("missing q", 400);
        }
        let mode = b.mode.as_deref().unwrap_or("hybrid");
        let limit = b.limit.unwrap_or(20).clamp(1, 100);

        if let Err(resp) = self.check_search_rate_limit() {
            return resp;
        }

        match mode {
            "fts" => {
                let rows = self
                    .fts_search_rows(&b.q, limit)
                    .unwrap_or_default();
                let hits = hits_from_rows(rows, "fts");
                Response::from_json(&json!({ "mode": "fts", "results": hits }))
            }
            "vector" => {
                let hits = self
                    .vector_search(&b.q, limit, b.user_hash.as_deref())
                    .await
                    .unwrap_or_default();
                Response::from_json(&json!({ "mode": "vector", "results": hits }))
            }
            _ => {
                let fts_rows = self.fts_search_rows(&b.q, limit).unwrap_or_default();
                let fts_hits = hits_from_rows(fts_rows, "fts");
                let vec_hits = self
                    .vector_search(&b.q, limit, b.user_hash.as_deref())
                    .await
                    .unwrap_or_default();
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
    async fn vector_search(
        &self,
        q: &str,
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

        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT tx_id, session_id, ts, user_text, assistant_text
             FROM transactions
             WHERE ts < ?
               AND (length(COALESCE(user_text, '')) + length(COALESCE(assistant_text, ''))) > 3
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
                 WHERE (length(COALESCE(user_text, '')) + length(COALESCE(assistant_text, ''))) > 3",
                None,
            )?;
            let arr: Vec<serde_json::Value> = c.to_array().unwrap_or_default();
            arr.first()
                .and_then(|v| v.get("n"))
                .and_then(|x| x.as_i64())
                .unwrap_or(0)
        };

        let scanned = rows.len() as i64;
        let mut skipped_empty = 0i64;
        let mut embed_errors = 0i64;
        let mut oldest_ts = before_ts;
        let mut processed: Vec<serde_json::Value> = Vec::with_capacity(rows.len());

        // Phase 1: sequential embeds. Workers AI is fast (~50ms per call), but
        // running in parallel risks rate-limit errors we'd then have to retry.
        // The real cost savings come from phase 2.
        struct Pending {
            tx_id: String,
            session_id: Option<String>,
            ts: i64,
            text_len: i64,
            embed_ms: i64,
            values: Vec<f32>,
        }
        let mut pending: Vec<Pending> = Vec::with_capacity(rows.len());

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
            let combined = format!("{}\n---\n{}", ut, at);
            let text_len = combined.len() as i64;

            if combined.trim().len() <= 3 {
                skipped_empty += 1;
                processed.push(json!({
                    "tx_id": tx_id,
                    "ts": ts,
                    "status": "skipped_empty",
                    "text_len": text_len,
                }));
                continue;
            }
            let embed_start = Date::now().as_millis() as i64;
            let embed_result = embed_text(&self.env, &combined).await;
            let embed_ms = Date::now().as_millis() as i64 - embed_start;
            match embed_result {
                Some(values) => pending.push(Pending {
                    tx_id,
                    session_id,
                    ts,
                    text_len,
                    embed_ms,
                    values,
                }),
                None => {
                    embed_errors += 1;
                    processed.push(json!({
                        "tx_id": tx_id,
                        "ts": ts,
                        "status": "embed_err",
                        "text_len": text_len,
                        "embed_ms": embed_ms,
                    }));
                }
            }
        }

        // Phase 2: single batched upsert. One Vectorize round-trip for up to
        // batch_size records — the old per-row path was spending ~450ms per
        // upsert, which dominated the batch wall-clock time.
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
        let batch_upsert_result = vectorize_upsert_many(&self.env, &user_hash, &batch_items).await;
        let batch_upsert_ms = Date::now().as_millis() as i64 - batch_upsert_start;

        let (upserted, upsert_errors, upsert_err_msg) = match &batch_upsert_result {
            Ok(()) => (pending.len() as i64, 0i64, None),
            Err(e) => (0i64, pending.len() as i64, Some(e.clone())),
        };
        for p in &pending {
            let status = if batch_upsert_result.is_ok() {
                "upserted"
            } else {
                "upsert_err"
            };
            let mut entry = json!({
                "tx_id": p.tx_id,
                "ts": p.ts,
                "status": status,
                "text_len": p.text_len,
                "embed_ms": p.embed_ms,
            });
            if let (Some(msg), Some(obj)) = (upsert_err_msg.as_deref(), entry.as_object_mut()) {
                obj.insert("err".into(), json!(msg));
            }
            processed.push(entry);
        }

        // "done" when the SELECT returned less than a full batch — no older
        // rows remain to page over. Caller re-invokes with next_before_ts
        // until this flips true.
        let done = scanned < batch_size;
        let next_before_ts = if done {
            serde_json::Value::Null
        } else {
            json!(oldest_ts)
        };

        Response::from_json(&json!({
            "scanned": scanned,
            "upserted": upserted,
            "skipped_empty": skipped_empty,
            "embed_errors": embed_errors,
            "upsert_errors": upsert_errors,
            "oldest_ts": oldest_ts,
            "next_before_ts": next_before_ts,
            "done": done,
            "rows": processed,
            "total_rows": total_rows,
            "batch_upsert_ms": batch_upsert_ms,
        }))
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
    in_flight: Option<i64>,
    #[serde(default)]
    anthropic_message_id: Option<String>,
    /// Text of the caller's last `user` message — new prompt + tool_result
    /// payloads. Indexed by FTS5 + embedded for Vectorize.
    #[serde(default)]
    user_text: Option<String>,
    /// Concatenated `text_delta` payloads from the assistant's SSE stream.
    #[serde(default)]
    assistant_text: Option<String>,
}

// ---------- SSE parsing ----------

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
    /// Concatenation of every `text_delta` payload from the stream — the
    /// assistant's user-facing prose. `thinking_delta` (private reasoning)
    /// and `input_json_delta` (tool args) are intentionally skipped.
    assistant_text: String,
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
                if let Some(cb) = evt.get("content_block") {
                    match cb.get("type").and_then(|v| v.as_str()) {
                        Some("tool_use") => {
                            if let Some(n) = cb.get("name").and_then(|v| v.as_str()) {
                                stats.tools.push(n.to_string());
                            }
                        }
                        Some("thinking") => {
                            stats.thinking_blocks += 1;
                        }
                        _ => {}
                    }
                }
            }
            "content_block_delta" => {
                // Only accumulate plain text output — tool-call JSON and
                // private thinking deltas are excluded from the search index.
                if let Some(d) = evt.get("delta") {
                    if d.get("type").and_then(|v| v.as_str()) == Some("text_delta") {
                        if let Some(t) = d.get("text").and_then(|v| v.as_str()) {
                            if stats.assistant_text.len() + t.len() <= TEXT_COL_CAP {
                                stats.assistant_text.push_str(t);
                            } else if stats.assistant_text.len() < TEXT_COL_CAP {
                                let remaining = TEXT_COL_CAP - stats.assistant_text.len();
                                let mut end = remaining;
                                while end > 0 && !t.is_char_boundary(end) {
                                    end -= 1;
                                }
                                stats.assistant_text.push_str(&t[..end]);
                            }
                        }
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
const TEXT_COL_CAP: usize = 256 * 1024;

#[derive(Default)]
struct ParsedRequest {
    max_tokens: Option<i64>,
    thinking_budget: Option<i64>,
    /// Concatenated text of the last user-role message's content blocks:
    /// `text` blocks verbatim, and `tool_result` blocks' string/array text.
    /// `image` blocks are skipped. Truncated to `TEXT_COL_CAP` chars.
    user_text: Option<String>,
}

fn parse_request_body(bytes: &[u8]) -> ParsedRequest {
    let v: serde_json::Value = match serde_json::from_slice(bytes) {
        Ok(v) => v,
        Err(_) => return ParsedRequest::default(),
    };
    let max_tokens = v.get("max_tokens").and_then(|x| x.as_i64());
    let thinking_budget = v
        .get("thinking")
        .and_then(|t| t.as_object())
        .filter(|o| o.get("type").and_then(|x| x.as_str()) == Some("enabled"))
        .and_then(|o| o.get("budget_tokens").and_then(|x| x.as_i64()));

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
        max_tokens,
        thinking_budget,
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

fn truncate_text(mut s: String) -> String {
    if s.len() > TEXT_COL_CAP {
        s.truncate(TEXT_COL_CAP);
        // Back up to a char boundary so we never leave a split codepoint.
        while !s.is_char_boundary(s.len()) {
            s.pop();
        }
    }
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

const EMBED_MODEL: &str = "@cf/baai/bge-base-en-v1.5";
const EMBED_DIMS: usize = 768;
// bge-base-en-v1.5 context is ~512 tokens (~2000 chars rule of thumb).
// We truncate from the front so the tail — usually the most recent
// prompt + the assistant's summary — stays in the embedding.
const EMBED_INPUT_CAP: usize = 2000;

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
    // matches the model's {text: string} schema.
    #[derive(Serialize)]
    struct EmbedInput<'a> {
        text: &'a str,
    }
    let input = EmbedInput { text: trimmed };
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

#[derive(Debug, Deserialize, Serialize, Clone)]
struct SearchHit {
    tx_id: String,
    ts: i64,
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
async fn compute_user_hash(
    entries: &[(String, String)],
    salt: &str,
    env: &Env,
) -> Option<(String, Option<String>)> {
    if let Some(raw_auth) = header_value(entries, "authorization") {
        let token = raw_auth
            .strip_prefix("Bearer ")
            .unwrap_or(&raw_auth)
            .to_string();
        return Some(resolve_oauth_hash(&token, salt, env).await);
    }
    if let Some(api_key) = header_value(entries, "x-api-key") {
        return Some((hash_identity(salt, "apikey:", &api_key), None));
    }
    None
}

fn hash_identity(salt: &str, prefix: &str, id: &str) -> String {
    let mut h = Sha256::new();
    h.update(salt.as_bytes());
    h.update(prefix.as_bytes());
    h.update(id.as_bytes());
    hex::encode(&h.finalize()[..8])
}

async fn resolve_oauth_hash(token: &str, salt: &str, env: &Env) -> (String, Option<String>) {
    // token_id identifies "this bearer value" without exposing the bearer in
    // KV keys. Salted so different deployments never share KV state.
    let token_id = hash_identity(salt, "tok:", token);
    let cache_key = format!("tok:{}", token_id);

    // KV hit: {uuid}|{email}. One round-trip to KV, skip the profile fetch.
    if let Ok(kv) = env.kv("SESSION") {
        if let Ok(Some(cached)) = kv.get(&cache_key).text().await {
            if let Some((uuid, email)) = cached.split_once('|') {
                let hash = hash_identity(salt, "uuid:", uuid);
                auto_link(&kv, email, &hash).await;
                return (hash, Some(email.to_string()));
            }
        }

        // KV miss: fetch profile, cache, hash.
        if let Some((uuid, email)) = fetch_anthropic_profile(token).await {
            let value = format!("{}|{}", uuid, email);
            if let Ok(builder) = kv.put(&cache_key, value) {
                let _ = builder.expiration_ttl(3600).execute().await;
            }
            let hash = hash_identity(salt, "uuid:", &uuid);
            auto_link(&kv, &email, &hash).await;
            return (hash, Some(email));
        }
    }

    // Degraded fallback — raw-token hash. Prefix keeps it in a disjoint
    // namespace from uuid hashes so a future successful resolve rejoins the
    // stable identity cleanly. Rare: only fires on KV outages or profile
    // endpoint failures.
    console_log!(
        "{{\"dir\":\"hash_fallback\",\"reason\":\"profile_or_kv_unavailable\"}}"
    );
    (hash_identity(salt, "raw:", token), None)
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

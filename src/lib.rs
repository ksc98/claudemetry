use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use js_sys::Uint8Array;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use worker::durable::State;
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
        return admin_route(&path, &req_headers_vec, &salt, &env).await;
    }

    let req_body_bytes = req.bytes().await.unwrap_or_default();
    let req_body_len = req_body_bytes.len() as i64;

    let user_hash = compute_user_hash(&req_headers_vec, &salt);
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

    let upstream_req = Request::new_with_init(&target, &init)?;
    let mut resp = Fetch::Request(upstream_req).send().await?;

    // Clone for out-of-band consumption; client-bound stream stays untouched.
    let resp_for_log = resp.cloned()?;
    let status = resp.status_code() as i32;
    let method_str = method.to_string();
    let target_for_record = target.clone();

    // Acquire the per-user DO stub up front so the future is Send-clean.
    let stub = user_hash.as_ref().and_then(|uh| {
        let ns = env.durable_object("USER_STORE").ok()?;
        let id = ns.id_from_name(uh).ok()?;
        id.get_stub().ok()
    });

    ctx.wait_until(async move {
        let mut r = resp_for_log;
        let body = r.bytes().await.unwrap_or_default();
        let body_str = String::from_utf8_lossy(&body);
        let stats = parse_sse_usage(&body_str);
        let elapsed = Date::now().as_millis() as i64 - start;

        let tx_id = stats
            .tx_id
            .clone()
            .unwrap_or_else(|| format!("{}-{:08x}", start, js_sys::Math::random().to_bits() as u32));
        let tools_json = if stats.tools.is_empty() {
            None
        } else {
            serde_json::to_string(&stats.tools).ok()
        };

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
        };

        // Always emit a structured response log so wrangler tail still works.
        let log = json!({
            "ts": Date::now().as_millis() as i64,
            "dir": "resp",
            "user_hash": user_hash,
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

        if let Some(stub) = stub {
            let body_json = serde_json::to_string(&record).unwrap_or_default();
            let arr = Uint8Array::from(body_json.as_bytes());
            let mut init = RequestInit::new();
            init.with_method(Method::Post);
            init.with_body(Some(arr.into()));
            // Hostname here is irrelevant — the stub routes by binding, not URL.
            if let Ok(req) = Request::new_with_init("https://store/ingest", &init) {
                if let Err(e) = stub.fetch_with_request(req).await {
                    console_log!("{{\"dir\":\"do_ingest_err\",\"err\":\"{:?}\"}}", e);
                }
            }
        }
    });

    Ok(resp)
}

// ---------- Admin probes ----------

async fn admin_route(
    path: &str,
    headers: &[(String, String)],
    salt: &str,
    env: &Env,
) -> Result<Response> {
    let user_hash = match compute_user_hash(headers, salt) {
        Some(h) => h,
        None => return Response::error("missing authorization", 401),
    };

    if path == "/_cm/whoami" {
        return Response::from_json(&json!({ "user_hash": user_hash }));
    }

    let ns = env.durable_object("USER_STORE")?;
    let id = ns.id_from_name(&user_hash)?;
    let stub = id.get_stub()?;

    let inner_path = match path {
        "/_cm/recent" => "/recent",
        "/_cm/stats" => "/stats",
        _ => return Response::error("unknown admin route", 404),
    };
    let inner_url = format!("https://store{}", inner_path);
    let req = Request::new(&inner_url, Method::Get)?;
    stub.fetch_with_request(req).await
}

// ---------- Per-user Durable Object with SQLite ----------

#[durable_object]
pub struct UserStore {
    state: State,
    _env: Env,
    initialized: std::cell::Cell<bool>,
}

impl DurableObject for UserStore {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            _env: env,
            initialized: std::cell::Cell::new(false),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        self.ensure_init();
        let url = req.url()?;
        let path = url.path().to_string();
        match (req.method(), path.as_str()) {
            (Method::Post, "/ingest") => self.ingest(&mut req).await,
            (Method::Get, "/recent") => self.recent().await,
            (Method::Get, "/stats") => self.stats().await,
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
        self.initialized.set(true);
    }

    async fn ingest(&self, req: &mut Request) -> Result<Response> {
        let r: TransactionRecord = req.json().await?;
        let sql = self.state.storage().sql();
        sql.exec(
            "INSERT OR REPLACE INTO transactions
             (tx_id, ts, session_id, method, url, status, elapsed_ms,
              model, input_tokens, output_tokens, cache_read, cache_creation,
              stop_reason, tools_json, req_body_bytes, resp_body_bytes)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            Some(vec![
                r.tx_id.into(),
                r.ts.into(),
                r.session_id.into(),
                r.method.into(),
                r.url.into(),
                (r.status as i64).into(),
                r.elapsed_ms.into(),
                r.model.into(),
                r.input_tokens.into(),
                r.output_tokens.into(),
                r.cache_read.into(),
                r.cache_creation.into(),
                r.stop_reason.into(),
                r.tools_json.into(),
                r.req_body_bytes.into(),
                r.resp_body_bytes.into(),
            ]),
        )?;
        Response::ok("ok")
    }

    async fn recent(&self) -> Result<Response> {
        let sql = self.state.storage().sql();
        let cursor = sql.exec(
            "SELECT tx_id, ts, session_id, model, status, elapsed_ms,
                    input_tokens, output_tokens, cache_read, cache_creation,
                    stop_reason, tools_json, req_body_bytes, resp_body_bytes
             FROM transactions ORDER BY ts DESC LIMIT 100",
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
                MIN(ts) AS first_ts,
                MAX(ts) AS last_ts
             FROM transactions",
            None,
        )?;
        let rows: Vec<serde_json::Value> = cursor.to_array()?;
        let summary = rows.into_iter().next().unwrap_or(json!({}));
        Response::from_json(&summary)
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
    stop_reason: Option<String>,
    tools: Vec<String>,
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
                    if cb.get("type").and_then(|v| v.as_str()) == Some("tool_use") {
                        if let Some(n) = cb.get("name").and_then(|v| v.as_str()) {
                            stats.tools.push(n.to_string());
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
}

// ---------- Identity ----------

fn header_value(entries: &[(String, String)], name: &str) -> Option<String> {
    entries
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

fn compute_user_hash(entries: &[(String, String)], salt: &str) -> Option<String> {
    let auth = header_value(entries, "authorization");
    let api_key = header_value(entries, "x-api-key");

    let identity: Option<String> = if let Some(a) = auth.as_deref() {
        let token = a.strip_prefix("Bearer ").unwrap_or(a);
        extract_jwt_sub(token).or_else(|| Some(token.to_string()))
    } else {
        api_key
    };

    identity.map(|id| {
        let mut h = Sha256::new();
        h.update(salt.as_bytes());
        h.update(id.as_bytes());
        hex::encode(&h.finalize()[..8])
    })
}

fn extract_jwt_sub(token: &str) -> Option<String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    claims
        .get("sub")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

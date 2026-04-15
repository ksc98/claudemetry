use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use js_sys::Uint8Array;
use serde_json::json;
use sha2::{Digest, Sha256};
use worker::*;

const UPSTREAM: &str = "https://api.anthropic.com";
const DEFAULT_SALT: &str = "claudemetry-dev-unset";

#[event(fetch)]
async fn fetch(mut req: Request, env: Env, ctx: Context) -> Result<Response> {
    let start = Date::now().as_millis();
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
    let req_body_bytes = req.bytes().await.unwrap_or_default();
    let req_body_preview = String::from_utf8_lossy(&req_body_bytes).to_string();

    let user_hash = compute_user_hash(&req_headers_vec, &salt);
    let session_id = header_value(&req_headers_vec, "x-claude-code-session-id");

    let req_log = json!({
        "ts": start,
        "dir": "req",
        "user_hash": user_hash,
        "session_id": session_id,
        "method": method.to_string(),
        "url": target,
        "headers": headers_to_object(&req_headers_vec),
        "body": req_body_preview,
        "body_len": req_body_bytes.len(),
    });
    console_log!("{}", req_log.to_string());

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

    let resp_for_log = resp.cloned()?;
    let status = resp.status_code();
    let resp_headers_vec: Vec<(String, String)> = resp.headers().entries().collect();
    let log_user_hash = user_hash.clone();
    let log_session_id = session_id.clone();

    ctx.wait_until(async move {
        let mut r = resp_for_log;
        let body = r.bytes().await.unwrap_or_default();
        let body_str = String::from_utf8_lossy(&body).to_string();
        let elapsed = Date::now().as_millis() - start;
        let log = json!({
            "ts": Date::now().as_millis(),
            "dir": "resp",
            "user_hash": log_user_hash,
            "session_id": log_session_id,
            "status": status,
            "elapsed_ms": elapsed,
            "headers": headers_to_object(&resp_headers_vec),
            "body": body_str,
            "body_len": body.len(),
        });
        console_log!("{}", log.to_string());
    });

    Ok(resp)
}

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

fn headers_to_object(entries: &[(String, String)]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (k, v) in entries {
        let lk = k.to_ascii_lowercase();
        let vv = if matches!(lk.as_str(), "x-api-key" | "authorization") {
            redact(v)
        } else {
            v.clone()
        };
        map.insert(k.clone(), serde_json::Value::String(vv));
    }
    serde_json::Value::Object(map)
}

fn redact(s: &str) -> String {
    if s.len() <= 12 {
        "***".to_string()
    } else {
        format!("{}…{}", &s[..6], &s[s.len() - 4..])
    }
}

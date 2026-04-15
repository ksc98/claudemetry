use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::io::IsTerminal;

use crate::quota::{
    bar, format_pct, human_bytes, human_count, Style, BAR_WIDTH, DO_STORAGE_LIMIT,
};

const CC_PROXY_SCRIPT: &str = "cc-proxy";

pub fn do_run(base: &str, token: &str) -> Result<()> {
    let stats = gh_get(base, token, "/_cm/stats")?;
    let whoami = gh_get(base, token, "/_cm/whoami")?;

    let user_hash = whoami
        .get("user_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");
    let turns = i64_at(&stats, "turns").max(0) as u64;
    let storage_bytes = i64_at(&stats, "storage_bytes").max(0) as u64;
    let first_ms = i64_at(&stats, "first_ts");
    let last_ms = i64_at(&stats, "last_ts");
    let input_tok = i64_at(&stats, "input_tokens").max(0) as u64;
    let output_tok = i64_at(&stats, "output_tokens").max(0) as u64;
    let cache_read = i64_at(&stats, "cache_read").max(0) as u64;
    let cache_creation = i64_at(&stats, "cache_creation").max(0) as u64;
    let req_bytes = i64_at(&stats, "req_bytes").max(0) as u64;
    let resp_bytes = i64_at(&stats, "resp_bytes").max(0) as u64;

    let sty = Style::new(std::io::stdout().is_terminal());

    let user_count = fetch_user_count(CC_PROXY_SCRIPT).unwrap_or(None);
    let header = match user_count {
        Some(n) => format!("DURABLE OBJECT ({n} user{})", if n == 1 { "" } else { "s" }),
        None => "DURABLE OBJECT".into(),
    };
    println!("{}", sty.header(&header));

    let storage_frac = storage_bytes as f64 / DO_STORAGE_LIMIT;
    let cap_str = format!(
        "{} / {}",
        human_bytes(storage_bytes),
        human_bytes(DO_STORAGE_LIMIT as u64)
    );
    let pct_str = format_pct(storage_frac);
    let bar_str = bar(&sty, storage_frac, BAR_WIDTH);

    let turns_str = human_count(turns);
    let window = format_window(first_ms, last_ms);
    let rate = compute_rate(turns, first_ms, last_ms);

    let rows: Vec<(&str, String)> = vec![
        ("user", sty.bold(user_hash)),
        ("turns", sty.bold(&turns_str)),
        ("storage", format!("{cap_str}  {bar_str}  {pct_str}")),
        ("pages", format_pages(&sty, storage_bytes, turns)),
        ("window", window),
        ("rate", rate),
    ];
    print_rows(&sty, &rows);

    println!();
    println!("{}", sty.header("TOKENS"));
    let token_rows: Vec<(&str, String)> = vec![
        ("input", human_count(input_tok)),
        ("output", human_count(output_tok)),
        ("cache read", human_count(cache_read)),
        ("cache create", human_count(cache_creation)),
    ];
    print_rows(&sty, &token_rows);

    println!();
    println!("{}", sty.header("PAYLOAD BYTES"));
    let payload_rows: Vec<(&str, String)> = vec![
        ("request", human_bytes(req_bytes)),
        ("response", human_bytes(resp_bytes)),
        ("total", human_bytes(req_bytes + resp_bytes)),
    ];
    print_rows(&sty, &payload_rows);

    Ok(())
}

const PAGE_SIZE: u64 = 4096;

fn format_pages(sty: &Style, storage_bytes: u64, turns: u64) -> String {
    if storage_bytes == 0 {
        return "—".into();
    }
    let pages = storage_bytes / PAGE_SIZE;
    if pages == 0 {
        return "—".into();
    }
    let rows_per_page = turns as f64 / pages as f64;
    let body = format!("{pages} × 4 KB  (~{rows_per_page:.1} rows/page)");
    sty.dim(&body)
}

fn print_rows(sty: &Style, rows: &[(&str, String)]) {
    let label_w = rows.iter().map(|(l, _)| l.len()).max().unwrap_or(0);
    for (label, val) in rows {
        let pad = " ".repeat(label_w - label.len());
        println!("  {}{}  {}", sty.dim(label), pad, val);
    }
}

fn compute_rate(turns: u64, first_ms: i64, last_ms: i64) -> String {
    if turns == 0 || first_ms <= 0 || last_ms <= 0 || last_ms <= first_ms {
        return "—".into();
    }
    let hours = (last_ms - first_ms) as f64 / 3_600_000.0;
    if hours < 1.0 / 60.0 {
        return format!("{} turns in < 1 min", turns);
    }
    let per_hr = turns as f64 / hours;
    format!("{:.1} turns/hr", per_hr)
}

fn format_window(first_ms: i64, last_ms: i64) -> String {
    if first_ms <= 0 || last_ms <= 0 {
        return "—".into();
    }
    let first = fmt_utc(first_ms);
    let last = fmt_utc(last_ms);
    let span_ms = (last_ms - first_ms).max(0) as u64;
    let span = fmt_span(span_ms);
    format!("{first} → {last}  ({span})")
}

fn fmt_utc(ms: i64) -> String {
    let dt: DateTime<Utc> = Utc.timestamp_millis_opt(ms).single().unwrap_or_else(Utc::now);
    dt.format("%Y-%m-%d %H:%M UTC").to_string()
}

fn fmt_span(ms: u64) -> String {
    let secs = ms / 1000;
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

fn i64_at(v: &Value, key: &str) -> i64 {
    v.get(key)
        .and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64)))
        .unwrap_or(0)
}

/// Distinct DO objectIds (= distinct users) for the given script in the last 30 days.
/// Returns None if CF_API_TOKEN or CF_ACCOUNT_ID is not set.
fn fetch_user_count(script: &str) -> Result<Option<u64>> {
    let Ok(token) = std::env::var("CF_API_TOKEN") else {
        return Ok(None);
    };
    let Ok(acct) = std::env::var("CF_ACCOUNT_ID") else {
        return Ok(None);
    };
    let now = Utc::now();
    let from = (now - Duration::days(30))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let to = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let q = r#"query U($acct:String!,$from:Time!,$to:Time!,$script:String!){
      viewer{accounts(filter:{accountTag:$acct}){
        durableObjectsInvocationsAdaptiveGroups(
          filter:{datetime_geq:$from,datetime_leq:$to,scriptName:$script},
          limit:10000
        ){
          dimensions{objectId}
        }
      }}
    }"#;
    let body = json!({
        "query": q,
        "variables": { "acct": acct, "from": from, "to": to, "script": script }
    });
    let res = ureq::post("https://api.cloudflare.com/client/v4/graphql")
        .set("Authorization", &format!("Bearer {token}"))
        .set("Content-Type", "application/json")
        .send_json(body);
    let text = match res {
        Ok(r) => r.into_string().context("reading cf response")?,
        Err(_) => return Ok(None),
    };
    let v: Value = serde_json::from_str(&text).context("parsing cf response")?;
    let rows = v
        .pointer("/data/viewer/accounts/0/durableObjectsInvocationsAdaptiveGroups")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for r in rows {
        if let Some(id) = r
            .pointer("/dimensions/objectId")
            .and_then(|x| x.as_str())
        {
            if !id.is_empty() {
                seen.insert(id.to_string());
            }
        }
    }
    Ok(Some(seen.len() as u64))
}

/// Append a Vectorize summary to the `burnage quota` top-down view. Requires
/// CF_API_TOKEN (with Vectorize: Read) + CF_ACCOUNT_ID — without them we print
/// a one-line hint. Never errors — the broader summary stays useful even if
/// this section fails.
pub fn vectorize_summary(index_name: &str) {
    let sty = Style::new(std::io::stdout().is_terminal());
    println!("{}", sty.header(&format!("VECTORIZE ({index_name})")));

    let token = match std::env::var("CF_API_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            println!(
                "  {}",
                sty.dim("set CF_API_TOKEN + CF_ACCOUNT_ID for index stats")
            );
            return;
        }
    };
    let acct = match std::env::var("CF_ACCOUNT_ID") {
        Ok(a) => a,
        Err(_) => {
            println!(
                "  {}",
                sty.dim("set CF_API_TOKEN + CF_ACCOUNT_ID for index stats")
            );
            return;
        }
    };

    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{acct}/vectorize/v2/indexes/{index_name}/info"
    );
    let res = ureq::get(&url)
        .set("Authorization", &format!("Bearer {token}"))
        .call();
    let text = match res {
        Ok(r) => match r.into_string() {
            Ok(t) => t,
            Err(e) => {
                println!("  {}", sty.dim(&format!("read error: {e}")));
                return;
            }
        },
        Err(ureq::Error::Status(code, r)) => {
            let b = r.into_string().unwrap_or_default();
            println!("  {}", sty.dim(&format!("HTTP {code}: {b}")));
            return;
        }
        Err(e) => {
            println!("  {}", sty.dim(&format!("fetch error: {e}")));
            return;
        }
    };
    let v: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            println!("  {}", sty.dim(&format!("parse error: {e}")));
            return;
        }
    };
    let result = v
        .get("result")
        .cloned()
        .unwrap_or(Value::Null);
    let vectors = i64_at(&result, "vectorCount").max(0) as u64;
    let dims = i64_at(&result, "dimensions").max(0) as u64;
    let mutation = result
        .get("processedUpToMutation")
        .and_then(|x| x.as_str())
        .unwrap_or("—")
        .to_string();
    let processed_at = result
        .get("processedUpToDatetime")
        .and_then(|x| x.as_str())
        .unwrap_or("—")
        .to_string();

    // https://developers.cloudflare.com/vectorize/platform/limits/
    const VEC_CAP: f64 = 10_000_000.0;
    let frac = vectors as f64 / VEC_CAP;
    let cap_str = format!("{} / 10M", human_count(vectors));
    let bar_str = bar(&sty, frac, BAR_WIDTH);
    let pct_str = format_pct(frac);

    let rows: Vec<(&str, String)> = vec![
        ("vectors", format!("{cap_str}  {bar_str}  {pct_str}")),
        ("dims", sty.bold(&dims.to_string())),
        ("last mut", sty.dim(&mutation)),
        ("updated", sty.dim(&processed_at)),
    ];
    print_rows(&sty, &rows);
}

fn gh_get(base: &str, token: &str, path: &str) -> Result<Value> {
    let url = format!("{}{}", base.trim_end_matches('/'), path);
    let res = ureq::get(&url)
        .set("Authorization", &format!("Bearer {token}"))
        .call();
    let text = match res {
        Ok(r) => r.into_string().context("reading response")?,
        Err(ureq::Error::Status(code, r)) => {
            let b = r.into_string().unwrap_or_default();
            return Err(anyhow!("{path}: HTTP {code}: {b}"));
        }
        Err(e) => return Err(anyhow!("{path}: {e}")),
    };
    serde_json::from_str::<Value>(&text).with_context(|| format!("parsing JSON from {path}"))
}

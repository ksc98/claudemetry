// `burnage turn <tx_id>` — dump a single transaction's full record,
// including untruncated user_text + assistant_text.
//
// Table output (on a tty) shows a metadata header plus the two text blocks
// with a thin separator. JSON output (piped, or `--format json`) is the raw
// row from the DO — all columns, no reformatting.

use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeZone, Utc};
use crossterm::style::Stylize;
use serde_json::Value;
use std::io::{self, IsTerminal};

use crate::search::Format;

pub struct TurnOpts {
    pub base: String,
    pub token: String,
    pub tx_id: String,
    pub format: Option<Format>,
}

pub fn run(opts: TurnOpts) -> Result<()> {
    let stdout_tty = io::stdout().is_terminal();
    let fmt = opts
        .format
        .unwrap_or(if stdout_tty { Format::Table } else { Format::Json });

    let url = format!("{}/_cm/turn", opts.base.trim_end_matches('/'));
    let body = serde_json::json!({ "tx_id": opts.tx_id });
    let auth = format!("Bearer {}", opts.token);
    let resp = ureq::post(&url).set("Authorization", &auth).send_json(body);
    let text = match resp {
        Ok(r) => r.into_string()?,
        Err(ureq::Error::Status(404, _)) => {
            return Err(anyhow!("tx_id not found: {}", opts.tx_id));
        }
        Err(ureq::Error::Status(code, r)) => {
            let raw = r.into_string().unwrap_or_default();
            return Err(anyhow!("HTTP {code}: {raw}"));
        }
        Err(e) => return Err(anyhow!(e)),
    };

    let row: Value =
        serde_json::from_str(&text).map_err(|e| anyhow!("parse {text}: {e}"))?;

    match fmt {
        Format::Json => println!("{}", serde_json::to_string_pretty(&row)?),
        Format::Table => render_table(&row),
    }
    Ok(())
}

fn render_table(r: &Value) {
    let tx_id = str_at(r, "tx_id").unwrap_or_else(|| "-".into());
    let ts = i64_at(r, "ts");
    let session = str_at(r, "session_id").unwrap_or_else(|| "-".into());
    let model = str_at(r, "model").unwrap_or_else(|| "-".into());
    let status = i64_at(r, "status");
    let elapsed_ms = i64_at(r, "elapsed_ms");
    let input_tok = i64_at(r, "input_tokens");
    let output_tok = i64_at(r, "output_tokens");
    let cache_read = i64_at(r, "cache_read");
    let cache_creation = i64_at(r, "cache_creation");
    let stop_reason = str_at(r, "stop_reason").unwrap_or_else(|| "-".into());
    let tools_json = str_at(r, "tools_json").unwrap_or_default();
    let anthropic_id = str_at(r, "anthropic_message_id").unwrap_or_else(|| "-".into());
    let user_text = str_at(r, "user_text").unwrap_or_default();
    let assistant_text = str_at(r, "assistant_text").unwrap_or_default();

    // Header line: tx id, model, when, elapsed.
    println!("{} {}", "turn".dark_grey(), tx_id.bold());
    let when = fmt_ts(ts);
    println!(
        "  {}  {}  {}  {}",
        model.cyan(),
        when.dark_grey(),
        fmt_duration(elapsed_ms).dark_grey(),
        format!("status {status}").dark_grey(),
    );
    // Token row.
    println!(
        "  {}  {}  {}  {}",
        format!("in {}", fmt_int(input_tok)).dark_grey(),
        format!("out {}", fmt_int(output_tok)).dark_grey(),
        format!("cache r {}", fmt_int(cache_read)).dark_grey(),
        format!("cache w {}", fmt_int(cache_creation)).dark_grey(),
    );
    // Session + stop reason + tools + inflight state.
    let mut extras: Vec<String> = Vec::new();
    extras.push(format!("session {session}"));
    if stop_reason != "-" {
        extras.push(format!("stop {stop_reason}"));
    }
    if anthropic_id != "-" {
        extras.push(format!("msg_id {anthropic_id}"));
    }
    if !tools_json.is_empty() && tools_json != "null" {
        if let Ok(v) = serde_json::from_str::<Value>(&tools_json) {
            if let Some(arr) = v.as_array() {
                let names: Vec<String> = arr
                    .iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect();
                if !names.is_empty() {
                    extras.push(format!("tools {}", names.join(",")));
                }
            }
        }
    }
    println!("  {}", extras.join("  ").dark_grey());

    print_section("user", &user_text);
    print_section("assistant", &assistant_text);
}

fn print_section(label: &str, body: &str) {
    println!();
    let size = format!("({})", fmt_bytes(body.len()));
    println!(
        "{} {} {}",
        "─".dark_grey(),
        label.yellow().bold(),
        size.dark_grey(),
    );
    if body.is_empty() {
        println!("  {}", "(empty)".dark_grey());
    } else {
        // Preserve the original line breaks; indent each line two spaces for
        // readability. No hard wrap — the terminal can do its own wrapping
        // and we shouldn't alter content the caller might want to grep.
        for line in body.lines() {
            println!("  {line}");
        }
    }
}

fn str_at(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str().map(String::from))
}

fn i64_at(v: &Value, key: &str) -> i64 {
    v.get(key)
        .and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64)))
        .unwrap_or(0)
}

fn fmt_ts(ms: i64) -> String {
    if ms <= 0 {
        return "-".into();
    }
    let dt: DateTime<Utc> = Utc
        .timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now);
    dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

fn fmt_duration(ms: i64) -> String {
    if ms <= 0 {
        return "-".into();
    }
    if ms < 1000 {
        format!("{ms}ms")
    } else {
        format!("{:.2}s", ms as f64 / 1000.0)
    }
}

fn fmt_int(n: i64) -> String {
    // Insert thousands separators without pulling in a formatter crate.
    let s = n.to_string();
    let neg = s.starts_with('-');
    let raw = if neg { &s[1..] } else { &s };
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in raw.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    let forward: String = out.chars().rev().collect();
    if neg {
        format!("-{forward}")
    } else {
        forward
    }
}

fn fmt_bytes(n: usize) -> String {
    if n < 1024 {
        format!("{n} B")
    } else if n < 1024 * 1024 {
        format!("{:.1} KB", n as f64 / 1024.0)
    } else {
        format!("{:.2} MB", n as f64 / (1024.0 * 1024.0))
    }
}

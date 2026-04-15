// `burnage vectorize-backfill` — re-embed every historical turn and upsert
// it into the caller's Vectorize namespace. One-off for vectors that predate
// the namespace migration; safe to re-run.

use anyhow::{anyhow, Result};
use crossterm::style::Stylize;
use serde_json::Value;
use std::io::Write;

pub struct BackfillOpts {
    pub base: String,
    pub token: String,
    pub batch_size: i64,
}

pub fn run(opts: BackfillOpts) -> Result<()> {
    let url = format!(
        "{}/_cm/admin/vectorize-backfill",
        opts.base.trim_end_matches('/')
    );
    let auth = format!("Bearer {}", opts.token);

    let mut before_ts: Option<i64> = None;
    let mut totals = Totals::default();
    let mut page = 0;

    loop {
        page += 1;
        let mut body = serde_json::json!({ "batch_size": opts.batch_size });
        if let Some(ts) = before_ts {
            body["before_ts"] = serde_json::json!(ts);
        }

        // Each batch does batch_size sequential Workers AI embeds + upserts,
        // so a full batch can take 15–30s before we see the response. Print
        // a pre-flight line (no newline, flushed) so the user sees that
        // we're blocked on the server, not hung.
        let pre = format!("  {} requesting…", format!("batch {page}").dark_grey());
        print!("\r{pre}");
        let _ = std::io::stdout().flush();

        let resp = ureq::post(&url).set("Authorization", &auth).send_json(body);
        let text = match resp {
            Ok(r) => r.into_string()?,
            Err(ureq::Error::Status(code, r)) => {
                let raw = r.into_string().unwrap_or_default();
                return Err(anyhow!("HTTP {code}: {raw}"));
            }
            Err(e) => return Err(anyhow!(e)),
        };

        let v: Value =
            serde_json::from_str(&text).map_err(|e| anyhow!("parse {text}: {e}"))?;
        let scanned = i64_at(&v, "scanned");
        let upserted = i64_at(&v, "upserted");
        let skipped = i64_at(&v, "skipped_empty");
        let embed_err = i64_at(&v, "embed_errors");
        let upsert_err = i64_at(&v, "upsert_errors");
        let done = v.get("done").and_then(|x| x.as_bool()).unwrap_or(true);
        let empty_rows: Vec<Value> = Vec::new();
        let rows = v
            .get("rows")
            .and_then(|x| x.as_array())
            .unwrap_or(&empty_rows);

        totals.scanned += scanned;
        totals.upserted += upserted;
        totals.skipped += skipped;
        totals.embed_err += embed_err;
        totals.upsert_err += upsert_err;

        // Clear the pre-flight "requesting…" line, then print the batch
        // header followed by one line per row with timings.
        print!("\r\x1b[2K");
        println!(
            "  {} scanned={} upserted={} skipped={} embed_err={} upsert_err={} ({:.1}s)",
            format!("batch {page}").dark_grey(),
            scanned,
            upserted,
            skipped,
            embed_err,
            upsert_err,
            (rows
                .iter()
                .map(|r| i64_at(r, "embed_ms") + i64_at(r, "upsert_ms"))
                .sum::<i64>() as f64)
                / 1000.0,
        );
        for r in rows {
            let tx_id = r.get("tx_id").and_then(|x| x.as_str()).unwrap_or("");
            let status = r.get("status").and_then(|x| x.as_str()).unwrap_or("");
            let embed_ms = i64_at(r, "embed_ms");
            let upsert_ms = i64_at(r, "upsert_ms");
            let text_len = i64_at(r, "text_len");
            let marker = match status {
                "upserted" => "✓".green().to_string(),
                "skipped_empty" => "·".dark_grey().to_string(),
                _ => "⚠".red().to_string(),
            };
            let timing = match status {
                "upserted" => format!("embed {}ms  upsert {}ms", embed_ms, upsert_ms),
                "embed_err" => format!("embed {}ms  [embed failed]", embed_ms).red().to_string(),
                "upsert_err" => {
                    let err = r.get("err").and_then(|x| x.as_str()).unwrap_or("?");
                    format!("embed {}ms  upsert {}ms  [{}]", embed_ms, upsert_ms, err)
                        .red()
                        .to_string()
                }
                "skipped_empty" => "empty".dark_grey().to_string(),
                other => other.to_string(),
            };
            println!(
                "    {} {} {}  {}",
                marker,
                short_tx(tx_id).dark_grey(),
                format!("{} B", fmt_int(text_len)).dark_grey(),
                timing,
            );
        }

        if done {
            break;
        }
        // Server returned a full batch, so older rows remain. Advance the
        // cursor to the oldest ts we just processed — strict `<` on the next
        // query excludes the last-processed row.
        let next = v
            .get("next_before_ts")
            .and_then(|x| x.as_i64())
            .ok_or_else(|| anyhow!("missing next_before_ts in response: {v}"))?;
        before_ts = Some(next);
    }

    println!();
    println!(
        "{} scanned={} upserted={} skipped={} embed_err={} upsert_err={}",
        "total".yellow().bold(),
        totals.scanned,
        totals.upserted,
        totals.skipped,
        totals.embed_err,
        totals.upsert_err,
    );
    Ok(())
}

#[derive(Default)]
struct Totals {
    scanned: i64,
    upserted: i64,
    skipped: i64,
    embed_err: i64,
    upsert_err: i64,
}

fn i64_at(v: &Value, key: &str) -> i64 {
    v.get(key)
        .and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64)))
        .unwrap_or(0)
}

// `inflight-1776290872285-abc123ef` is 33 chars — too wide for a per-row
// line. Keep the distinguishing suffix so lines remain clickable-unique.
fn short_tx(s: &str) -> String {
    if s.len() <= 24 {
        s.to_string()
    } else {
        format!("{}…{}", &s[..9], &s[s.len() - 8..])
    }
}

fn fmt_int(n: i64) -> String {
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

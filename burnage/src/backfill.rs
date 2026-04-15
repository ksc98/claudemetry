// `burnage vectorize-backfill` — re-embed every historical turn and upsert
// it into the caller's Vectorize namespace. One-off for vectors that predate
// the namespace migration; safe to re-run.

use anyhow::{anyhow, Result};
use crossterm::style::Stylize;
use serde_json::Value;
use std::io::Write;
use std::time::Instant;

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
    let mut page = 0i64;
    let mut total_rows: i64 = 0;
    let mut total_batches: i64 = 0;
    let started = Instant::now();

    loop {
        page += 1;
        let mut body = serde_json::json!({ "batch_size": opts.batch_size });
        if let Some(ts) = before_ts {
            body["before_ts"] = serde_json::json!(ts);
        }

        // Pre-flight line (no newline, flushed) so the user sees that we're
        // blocked on the server, not hung.
        let header = if total_batches > 0 {
            format!("batch {}/{}", page, total_batches)
        } else {
            format!("batch {}", page)
        };
        let pre = format!("  {} requesting…", header.clone().dark_grey());
        print!("\r{pre}");
        let _ = std::io::stdout().flush();

        let req_start = Instant::now();
        let resp = ureq::post(&url).set("Authorization", &auth).send_json(body);
        let text = match resp {
            Ok(r) => r.into_string()?,
            Err(ureq::Error::Status(code, r)) => {
                let raw = r.into_string().unwrap_or_default();
                return Err(anyhow!("HTTP {code}: {raw}"));
            }
            Err(e) => return Err(anyhow!(e)),
        };
        let req_ms = req_start.elapsed().as_millis() as i64;

        let v: Value =
            serde_json::from_str(&text).map_err(|e| anyhow!("parse {text}: {e}"))?;
        let scanned = i64_at(&v, "scanned");
        let upserted = i64_at(&v, "upserted");
        let skipped = i64_at(&v, "skipped_empty");
        let embed_err = i64_at(&v, "embed_errors");
        let upsert_err = i64_at(&v, "upsert_errors");
        let batch_upsert_ms = i64_at(&v, "batch_upsert_ms");
        let done = v.get("done").and_then(|x| x.as_bool()).unwrap_or(true);
        let empty_rows: Vec<Value> = Vec::new();
        let rows = v
            .get("rows")
            .and_then(|x| x.as_array())
            .unwrap_or(&empty_rows);

        // Cache the total on the first response — server recomputes it every
        // batch but it only changes if new turns land mid-backfill, which
        // isn't a case we need to be precise for.
        if total_rows == 0 {
            total_rows = i64_at(&v, "total_rows");
            if total_rows > 0 && opts.batch_size > 0 {
                total_batches = (total_rows + opts.batch_size - 1) / opts.batch_size;
            }
        }

        totals.scanned += scanned;
        totals.upserted += upserted;
        totals.skipped += skipped;
        totals.embed_err += embed_err;
        totals.upsert_err += upsert_err;

        // Clear the pre-flight line, then print the batch header (with total
        // if known) and a progress bar tracking scanned-so-far / total.
        print!("\r\x1b[2K");
        let embed_sum: i64 = rows.iter().map(|r| i64_at(r, "embed_ms")).sum();
        let bar = progress_bar(totals.scanned, total_rows, 24);
        let pct = if total_rows > 0 {
            (totals.scanned as f64 / total_rows as f64) * 100.0
        } else {
            0.0
        };
        let header_line = if total_batches > 0 {
            format!("batch {}/{}", page, total_batches)
        } else {
            format!("batch {}", page)
        };
        println!(
            "  {} {}  {} {}/{} ({:.1}%)  scanned={} upserted={} skipped={} embed_err={} upsert_err={}  (embed {:.1}s · upsert {:.2}s · rtt {:.1}s)",
            header_line.dark_grey(),
            bar,
            "progress".dark_grey(),
            fmt_int(totals.scanned),
            fmt_int(total_rows.max(totals.scanned)),
            pct,
            scanned,
            upserted,
            skipped,
            embed_err,
            upsert_err,
            embed_sum as f64 / 1000.0,
            batch_upsert_ms as f64 / 1000.0,
            req_ms as f64 / 1000.0,
        );
        for r in rows {
            let tx_id = r.get("tx_id").and_then(|x| x.as_str()).unwrap_or("");
            let status = r.get("status").and_then(|x| x.as_str()).unwrap_or("");
            let embed_ms = i64_at(r, "embed_ms");
            let text_len = i64_at(r, "text_len");
            let marker = match status {
                "upserted" => "✓".green().to_string(),
                "skipped_empty" => "·".dark_grey().to_string(),
                _ => "⚠".red().to_string(),
            };
            let detail = match status {
                "upserted" => format!("embed {}ms", embed_ms),
                "embed_err" => format!("embed {}ms  [embed failed]", embed_ms)
                    .red()
                    .to_string(),
                "upsert_err" => {
                    let err = r.get("err").and_then(|x| x.as_str()).unwrap_or("?");
                    format!("embed {}ms  [batch upsert failed: {}]", embed_ms, err)
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
                detail,
            );
        }

        if done {
            break;
        }
        let next = v
            .get("next_before_ts")
            .and_then(|x| x.as_i64())
            .ok_or_else(|| anyhow!("missing next_before_ts in response: {v}"))?;
        before_ts = Some(next);
    }

    println!();
    let total_s = started.elapsed().as_secs_f64();
    println!(
        "{} scanned={} upserted={} skipped={} embed_err={} upsert_err={}  ({:.1}s wall)",
        "total".yellow().bold(),
        fmt_int(totals.scanned),
        fmt_int(totals.upserted),
        fmt_int(totals.skipped),
        fmt_int(totals.embed_err),
        fmt_int(totals.upsert_err),
        total_s,
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

fn progress_bar(done: i64, total: i64, width: usize) -> String {
    if total <= 0 {
        return format!("[{}]", "·".repeat(width).dark_grey());
    }
    let ratio = (done as f64 / total as f64).clamp(0.0, 1.0);
    let filled = (ratio * width as f64).round() as usize;
    let filled = filled.min(width);
    let bar = format!(
        "{}{}",
        "█".repeat(filled).green(),
        "░".repeat(width - filled).dark_grey(),
    );
    format!("[{}]", bar)
}

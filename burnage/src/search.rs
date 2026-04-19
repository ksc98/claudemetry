// `burnage search` — headless wrapper over /_cm/search.
//
// Modes: fts (exact tokens, bm25), vector (semantic cosine), hybrid (both,
// merged via reciprocal rank fusion). Default is hybrid.
//
// Output auto-detects: styled table on a tty, raw JSON when stdout is piped.
// Override with --format {table,json}. Table mode converts the server's
// <mark> snippet highlights into bold-yellow ANSI for terminal-friendly
// display; JSON mode preserves them verbatim so downstream consumers can
// render however they like.

use anyhow::{anyhow, Result};
use crossterm::style::Stylize;
use serde::Deserialize;
use std::io::{self, IsTerminal};

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum Mode {
    Fts,
    Vector,
    Hybrid,
}

impl Mode {
    fn as_str(&self) -> &'static str {
        match self {
            Mode::Fts => "fts",
            Mode::Vector => "vector",
            Mode::Hybrid => "hybrid",
        }
    }
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum Format {
    Table,
    Json,
}

pub struct SearchOpts {
    pub base: String,
    pub token: String,
    pub query: String,
    pub mode: Mode,
    pub limit: u32,
    pub format: Option<Format>,
    /// When true, include tx_id + session_id footer per hit.
    pub verbose: bool,
}

#[derive(Deserialize)]
struct SearchResp {
    #[serde(default)]
    mode: String,
    #[serde(default)]
    results: Vec<Hit>,
}

#[derive(Deserialize)]
struct Hit {
    tx_id: String,
    #[serde(default)]
    ts: i64,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    user_snip: Option<String>,
    #[serde(default)]
    asst_snip: Option<String>,
    #[serde(default)]
    score: f64,
    #[serde(default)]
    match_source: String,
}

pub fn run(opts: SearchOpts) -> Result<()> {
    let stdout_tty = io::stdout().is_terminal();
    let fmt = opts
        .format
        .unwrap_or(if stdout_tty { Format::Table } else { Format::Json });

    let url = format!("{}/_cm/search", opts.base.trim_end_matches('/'));
    let body = serde_json::json!({
        "q": opts.query,
        "mode": opts.mode.as_str(),
        "limit": opts.limit,
    });
    let auth = format!("Bearer {}", opts.token);
    let resp = ureq::post(&url).set("Authorization", &auth).send_json(body);
    let text = match resp {
        Ok(r) => r.into_string()?,
        Err(ureq::Error::Status(429, r)) => {
            // Bubble up the retry-after so scripted use can react. On a tty,
            // print a friendly summary; elsewhere, preserve the JSON.
            let raw = r.into_string().unwrap_or_default();
            if stdout_tty {
                let retry = serde_json::from_str::<serde_json::Value>(&raw)
                    .ok()
                    .and_then(|v| v.get("retry_after_seconds").and_then(|x| x.as_i64()))
                    .unwrap_or(0);
                return Err(anyhow!(
                    "rate-limited (120 req/min). Retry in ~{retry}s."
                ));
            }
            return Err(anyhow!("HTTP 429: {raw}"));
        }
        Err(ureq::Error::Status(code, r)) => {
            let raw = r.into_string().unwrap_or_default();
            return Err(anyhow!("HTTP {code}: {raw}"));
        }
        Err(e) => return Err(anyhow!(e)),
    };

    let parsed: SearchResp = serde_json::from_str(&text)
        .map_err(|e| anyhow!("parse {text}: {e}"))?;

    match fmt {
        Format::Json => println!("{}", text.trim()),
        Format::Table => render_table(&parsed, opts.verbose),
    }
    Ok(())
}

fn render_table(resp: &SearchResp, verbose: bool) {
    if resp.results.is_empty() {
        println!("{}", "no matches".dark_grey());
        return;
    }
    let count = resp.results.len();
    println!(
        "{} {} · {}",
        format!("{count}").bold(),
        if count == 1 { "result" } else { "results" }.dark_grey(),
        resp.mode.clone().yellow(),
    );
    for (i, h) in resp.results.iter().enumerate() {
        if i > 0 {
            println!();
        }
        render_hit(h, verbose);
    }
}

fn render_hit(h: &Hit, verbose: bool) {
    let badge: crossterm::style::StyledContent<String> = match h.match_source.as_str() {
        "fts" => "keyword".to_string().cyan(),
        "vector" => "semantic".to_string().magenta(),
        "both" => "both".to_string().green(),
        _ => h.match_source.clone().dark_grey(),
    };
    let when = fmt_ago(h.ts);
    let model = h.model.clone().unwrap_or_else(|| "?".to_string());
    // Line 1: badge · model · when (score only in verbose — RRF scale is
    // confusing at a glance and match_source already tells the story).
    if verbose {
        println!(
            "{} {}  {}  {}",
            badge.bold(),
            model.dark_grey(),
            when.dark_grey(),
            format!("{:.3}", h.score).dark_grey(),
        );
    } else {
        println!("{} {}  {}", badge.bold(), model.dark_grey(), when.dark_grey());
    }
    // Line 2: one snippet. Prefer asst_snip (usually the substantive
    // response); fall back to user_snip if the asst text was empty.
    let snip = h
        .asst_snip
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .or(h.user_snip.as_deref())
        .unwrap_or("");
    let role_tag = if h
        .asst_snip
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty())
    {
        "ast".dark_grey()
    } else {
        "you".cyan()
    };
    let cleaned = clean_snippet_for_display(snip);
    println!("  {} {}", role_tag, cleaned);
    // Verbose: restore the old tx/sess footer and the suppressed snippet.
    if verbose {
        if let Some(s) = &h.user_snip {
            if !s.trim().is_empty() && h.asst_snip.as_deref().is_some_and(|a| !a.trim().is_empty()) {
                let cleaned_u = clean_snippet_for_display(s);
                println!("  {} {}", "you".cyan(), cleaned_u);
            }
        }
        let session = h.session_id.clone().unwrap_or_else(|| "-".to_string());
        println!(
            "  {} {}  {} {}",
            "tx".dark_grey(),
            h.tx_id.clone().dark_grey(),
            "sess".dark_grey(),
            session.dark_grey(),
        );
    }
    // Resume hint — a copy-pasteable command that reopens the session this
    // hit came from. Skip when the row has no session_id (older rows, etc.).
    if let Some(sid) = h.session_id.as_deref().filter(|s| !s.is_empty()) {
        println!("  {}", format!("claude -r {sid}").dark_grey());
    }
}

/// For display only: convert <mark> to ANSI highlight, strip ASCII
/// box-drawing characters (replaced with a single space), collapse runs of
/// whitespace. The underlying FTS5 index keeps the original text — these
/// edits are pure presentation.
fn clean_snippet_for_display(text: &str) -> String {
    let ansi = mark_to_ansi(text);
    // Strip box-drawing + common unicode "draw" chars that only exist in
    // replayed tool output, never in actual prose. Replace with space so
    // word boundaries still separate.
    let filtered: String = ansi
        .chars()
        .map(|c| match c {
            '│' | '├' | '┤' | '┬' | '┴' | '┼' | '┘' | '└' | '┌' | '┐' | '─' | '━'
            | '┃' | '╎' | '╏' | '╌' | '╍' => ' ',
            _ => c,
        })
        .collect();
    // Collapse whitespace runs so the snippet fits on one row.
    filtered.split_whitespace().collect::<Vec<_>>().join(" ")
}

// Convert SQLite's `<mark>…</mark>` snippet highlights into ANSI bold yellow,
// and strip anything we don't emit ourselves. Cheap linear scan — no regex.
fn mark_to_ansi(s: &str) -> String {
    const OPEN: &str = "<mark>";
    const CLOSE: &str = "</mark>";
    const HL_ON: &str = "\x1b[1;33m";
    const HL_OFF: &str = "\x1b[0m";
    let mut out = String::with_capacity(s.len() + 16);
    let mut i = 0;
    let bytes = s.as_bytes();
    while i < bytes.len() {
        if s[i..].starts_with(OPEN) {
            out.push_str(HL_ON);
            i += OPEN.len();
        } else if s[i..].starts_with(CLOSE) {
            out.push_str(HL_OFF);
            i += CLOSE.len();
        } else {
            // Consume one UTF-8 scalar — byte offset scan, char boundary safe.
            let ch = s[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

fn fmt_ago(ts_ms: i64) -> String {
    if ts_ms <= 0 {
        return "-".to_string();
    }
    let now = chrono::Utc::now().timestamp_millis();
    let delta = (now - ts_ms).max(0) / 1000;
    if delta < 60 {
        format!("{delta}s ago")
    } else if delta < 3600 {
        format!("{}m ago", delta / 60)
    } else if delta < 86400 {
        format!("{}h ago", delta / 3600)
    } else {
        format!("{}d ago", delta / 86400)
    }
}

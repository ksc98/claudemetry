// `burnage shell` — generic SQL REPL + headless executor against any DO's
// SQLite via the proxy's /_cm/admin/sql endpoint.
//
// Modes (auto-detected unless overridden):
//   burnage shell                  → interactive REPL against your own DO
//   burnage shell --hash <h>       → REPL against another DO (admin/migration)
//   burnage shell -c "SQL"         → one-shot, json output if stdout is piped
//   burnage shell -f script.sql    → run each ;-terminated statement
//   echo "SELECT…" | burnage shell → read SQL from stdin
//
// Output formats:
//   table  — pretty rendering with crossterm styling, default for tty
//   json   — `rows` array, default when stdout is piped
//   tsv    — header + tab-separated rows
//
// Dot commands (REPL only): .tables, .schema [tbl], .hash <h|->, .whoami,
// .quit, .help. History persists at ~/.cache/burnage/shell_history.
//
// The line editor is hand-rolled on crossterm raw mode — supports
// arrow-key cursor + history navigation, Home/End, Backspace, Ctrl-C
// (cancel current input), Ctrl-D (EOF / exit when buffer empty).

use anyhow::{anyhow, Context, Result};
use crossterm::{
    cursor::MoveToColumn,
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    queue,
    style::Stylize,
    terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType},
};
use serde::Deserialize;
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::{self, IsTerminal, Read, Write};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum Format {
    Table,
    Json,
    Tsv,
}

pub struct ShellOpts {
    pub base: String,
    pub token: String,
    pub hash: Option<String>,
    pub command: Option<String>,
    pub file: Option<PathBuf>,
    pub format: Option<Format>,
}

#[derive(Deserialize)]
struct SqlResp {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<Value>,
    #[serde(default)]
    affected: i64,
    #[serde(default)]
    took_ms: i64,
}

#[derive(Deserialize)]
struct SqlError {
    error: String,
}

pub fn run(opts: ShellOpts) -> Result<()> {
    let stdout_tty = io::stdout().is_terminal();
    let stdin_tty = io::stdin().is_terminal();
    let fmt = opts.format.unwrap_or(if stdout_tty {
        Format::Table
    } else {
        Format::Json
    });

    if let Some(sql) = opts.command.as_deref() {
        return run_script(&opts.base, &opts.token, opts.hash.as_deref(), sql, fmt);
    }
    if let Some(path) = opts.file.as_deref() {
        let body = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        return run_script(&opts.base, &opts.token, opts.hash.as_deref(), &body, fmt);
    }
    if !stdin_tty {
        let mut body = String::new();
        io::stdin().read_to_string(&mut body)?;
        return run_script(&opts.base, &opts.token, opts.hash.as_deref(), &body, fmt);
    }
    repl(&opts.base, &opts.token, opts.hash, fmt)
}

// ---------- Network ----------

fn run_script(
    base: &str,
    token: &str,
    hash: Option<&str>,
    sql: &str,
    fmt: Format,
) -> Result<()> {
    for stmt in split_statements(sql) {
        let resp = exec(base, token, hash, &stmt)?;
        render(&resp, fmt);
    }
    Ok(())
}

fn exec(base: &str, token: &str, hash: Option<&str>, sql: &str) -> Result<SqlResp> {
    let url = format!("{}/_cm/admin/sql", base.trim_end_matches('/'));
    let mut body = serde_json::json!({ "sql": sql, "params": [] });
    if let Some(h) = hash {
        body["hash"] = Value::String(h.to_string());
    }
    let auth = format!("Bearer {token}");
    let res = ureq::post(&url)
        .set("Authorization", &auth)
        .send_json(body);
    match res {
        Ok(r) => {
            let body = r.into_string()?;
            serde_json::from_str(&body).context("parsing /_cm/admin/sql response")
        }
        Err(ureq::Error::Status(_, r)) => {
            let body = r.into_string().unwrap_or_default();
            if let Ok(SqlError { error }) = serde_json::from_str::<SqlError>(&body) {
                Err(anyhow!("{error}"))
            } else {
                Err(anyhow!("{body}"))
            }
        }
        Err(e) => Err(anyhow!(e)),
    }
}

// ---------- Output ----------

fn render(resp: &SqlResp, fmt: Format) {
    match fmt {
        Format::Json => {
            // Pretty so humans can skim a piped result; jq doesn't care.
            match serde_json::to_string_pretty(&resp.rows) {
                Ok(s) => println!("{s}"),
                Err(_) => println!("[]"),
            }
        }
        Format::Tsv => {
            if !resp.columns.is_empty() {
                println!("{}", resp.columns.join("\t"));
            }
            for row in &resp.rows {
                if let Some(obj) = row.as_object() {
                    let cells: Vec<String> = resp
                        .columns
                        .iter()
                        .map(|c| cell_string(obj.get(c).unwrap_or(&Value::Null)))
                        .collect();
                    println!("{}", cells.join("\t"));
                }
            }
        }
        Format::Table => render_table(resp),
    }
}

fn render_table(resp: &SqlResp) {
    if !resp.columns.is_empty() && !resp.rows.is_empty() {
        let cells: Vec<Vec<String>> = resp
            .rows
            .iter()
            .map(|row| {
                resp.columns
                    .iter()
                    .map(|c| {
                        row.as_object()
                            .and_then(|o| o.get(c))
                            .map(cell_string)
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .collect();

        // Column widths: max(header, longest row cell), capped so a single
        // wide value doesn't blow up the layout. Truncated cells get an
        // ellipsis so it's visually obvious.
        let widths: Vec<usize> = resp
            .columns
            .iter()
            .enumerate()
            .map(|(i, h)| {
                let mut w = display_width(h);
                for r in &cells {
                    w = w.max(display_width(&r[i]));
                }
                w.min(64)
            })
            .collect();

        let header_line = resp
            .columns
            .iter()
            .enumerate()
            .map(|(i, h)| pad(h, widths[i]))
            .collect::<Vec<_>>()
            .join("  ");
        println!("{}", header_line.bold());

        let sep = widths
            .iter()
            .map(|w| "─".repeat(*w))
            .collect::<Vec<_>>()
            .join("  ");
        println!("{}", sep.dark_grey());

        for r in &cells {
            let row = r
                .iter()
                .enumerate()
                .map(|(i, c)| pad(&truncate(c, widths[i]), widths[i]))
                .collect::<Vec<_>>()
                .join("  ");
            println!("{row}");
        }
    }
    // Footer always to stderr so `2>/dev/null` keeps stdout clean.
    let suffix = if resp.affected != resp.rows.len() as i64 {
        format!(" · {} affected", resp.affected)
    } else {
        String::new()
    };
    eprintln!(
        "{}",
        format!(
            "{} row{} · {} ms{}",
            resp.rows.len(),
            if resp.rows.len() == 1 { "" } else { "s" },
            resp.took_ms,
            suffix
        )
        .dark_grey()
    );
}

fn cell_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => v.to_string(),
    }
}

fn display_width(s: &str) -> usize {
    // Char count is a close-enough approximation for ASCII-dominant SQL
    // result sets; double-width CJK or zero-width modifiers will misalign
    // by one cell — acceptable for an admin tool.
    s.chars().count()
}

fn pad(s: &str, w: usize) -> String {
    let cur = display_width(s);
    if cur >= w {
        s.to_string()
    } else {
        let mut r = String::with_capacity(s.len() + (w - cur));
        r.push_str(s);
        for _ in 0..(w - cur) {
            r.push(' ');
        }
        r
    }
}

fn truncate(s: &str, w: usize) -> String {
    if display_width(s) <= w {
        return s.to_string();
    }
    if w == 0 {
        return String::new();
    }
    let take = w.saturating_sub(1);
    let mut out: String = s.chars().take(take).collect();
    out.push('…');
    out
}

// ---------- Statement splitting ----------

// Tracks single-quoted strings (with SQL-style '' escape) so semicolons
// inside literals don't terminate. Ignores double-quoted identifiers and
// comments — fine for the admin SQL we exercise here.
fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_str = false;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                buf.push(ch);
                if in_str {
                    if chars.peek() == Some(&'\'') {
                        buf.push(chars.next().unwrap());
                    } else {
                        in_str = false;
                    }
                } else {
                    in_str = true;
                }
            }
            ';' if !in_str => {
                let t = buf.trim();
                if !t.is_empty() {
                    out.push(t.to_string());
                }
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }
    let t = buf.trim();
    if !t.is_empty() {
        out.push(t.to_string());
    }
    out
}

// ---------- REPL ----------

fn repl(base: &str, token: &str, mut hash: Option<String>, fmt: Format) -> Result<()> {
    eprintln!(
        "{} · /_cm/admin/sql · {} for commands, {} to exit",
        "burnage shell".cyan().bold(),
        ".help".green(),
        ".quit".green()
    );
    if let Some(h) = &hash {
        eprintln!("{} {}", "targeting".dark_grey(), h.as_str().yellow());
    }

    let history_path = history_file()?;
    let mut editor = LineEditor::load(history_path);

    loop {
        let prompt = match &hash {
            Some(h) => format!("sql({})> ", &h[..h.len().min(6)]),
            None => "sql> ".to_string(),
        };
        match editor.read_line(&prompt)? {
            Some(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if trimmed.starts_with('.') {
                    if handle_dot(trimmed, &mut hash, base, token, fmt) {
                        break;
                    }
                    continue;
                }
                // Auto-append `;` if missing — convenient for one-line use.
                let stmts = split_statements(trimmed);
                for stmt in stmts {
                    match exec(base, token, hash.as_deref(), &stmt) {
                        Ok(resp) => render(&resp, fmt),
                        Err(e) => eprintln!("{} {e}", "error:".red().bold()),
                    }
                }
            }
            None => break, // EOF
        }
    }
    Ok(())
}

// Returns true iff the loop should exit.
fn handle_dot(
    cmd: &str,
    hash: &mut Option<String>,
    base: &str,
    token: &str,
    fmt: Format,
) -> bool {
    let mut parts = cmd.splitn(2, char::is_whitespace);
    let head = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("").trim();
    match head {
        ".quit" | ".exit" | ".q" => return true,
        ".help" | ".h" => {
            eprintln!("{}", "  .tables           list user tables".dark_grey());
            eprintln!("{}", "  .schema [name]    show CREATE statements".dark_grey());
            eprintln!("{}", "  .hash <h>|-       switch DO target (- clears)".dark_grey());
            eprintln!("{}", "  .whoami           print current target hash".dark_grey());
            eprintln!("{}", "  .quit             exit".dark_grey());
        }
        ".tables" => {
            run_one(
                base,
                token,
                hash.as_deref(),
                "SELECT name FROM sqlite_master \
                 WHERE type='table' AND name NOT LIKE 'sqlite_%' \
                 ORDER BY name",
                fmt,
            );
        }
        ".schema" => {
            let sql = if rest.is_empty() {
                "SELECT name, sql FROM sqlite_master \
                 WHERE type IN ('table','index') AND sql IS NOT NULL \
                 ORDER BY name"
                    .to_string()
            } else {
                format!(
                    "SELECT name, sql FROM sqlite_master \
                     WHERE name='{}' AND sql IS NOT NULL",
                    rest.replace('\'', "''")
                )
            };
            run_one(base, token, hash.as_deref(), &sql, fmt);
        }
        ".hash" => {
            if rest == "-" {
                *hash = None;
                eprintln!("{}", "cleared override; targeting your own DO".dark_grey());
            } else if rest.len() == 16 && rest.bytes().all(|b| b.is_ascii_hexdigit()) {
                *hash = Some(rest.to_string());
                eprintln!("{} {}", "targeting".dark_grey(), rest.yellow());
            } else {
                eprintln!("usage: .hash <16-hex>|-");
            }
        }
        ".whoami" => {
            eprintln!("{}", hash.as_deref().unwrap_or("<self>"));
        }
        _ => eprintln!("unknown command: {head} (try .help)"),
    }
    false
}

fn run_one(base: &str, token: &str, hash: Option<&str>, sql: &str, fmt: Format) {
    match exec(base, token, hash, sql) {
        Ok(r) => render(&r, fmt),
        Err(e) => eprintln!("{} {e}", "error:".red().bold()),
    }
}

// ---------- Line editor (crossterm raw mode) ----------

struct LineEditor {
    history: Vec<String>,
    history_path: PathBuf,
}

// Restores cooked-mode terminal even if the caller bails on error or panics
// mid-readline. Without this, an early return from inside raw mode leaves
// the user's shell unable to echo input.
struct RawGuard;
impl RawGuard {
    fn new() -> io::Result<Self> {
        enable_raw_mode()?;
        Ok(Self)
    }
}
impl Drop for RawGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}

impl LineEditor {
    fn load(history_path: PathBuf) -> Self {
        let history = std::fs::read_to_string(&history_path)
            .ok()
            .map(|s| s.lines().map(String::from).collect::<Vec<_>>())
            .unwrap_or_default();
        Self {
            history,
            history_path,
        }
    }

    fn append_history(&mut self, line: &str) {
        if line.trim().is_empty() {
            return;
        }
        // Skip immediate duplicates so spamming Enter doesn't pollute history.
        if self.history.last().map(|s| s.as_str()) == Some(line) {
            return;
        }
        self.history.push(line.to_string());
        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.history_path)
        {
            let _ = writeln!(f, "{line}");
        }
    }

    // Returns Ok(Some(line)) on Enter, Ok(None) on Ctrl-D with empty buffer.
    // Ctrl-C surfaces as Ok(Some(String::new())) so the caller can ignore.
    fn read_line(&mut self, prompt: &str) -> Result<Option<String>> {
        let _guard = RawGuard::new()?;
        let mut stdout = io::stdout();
        let mut buf = String::new();
        let mut cursor: usize = 0;
        let mut hist_pos: Option<usize> = None;
        let mut saved_buf = String::new();

        write!(stdout, "{prompt}")?;
        stdout.flush()?;

        loop {
            let ev = event::read()?;
            let Event::Key(KeyEvent {
                code,
                modifiers,
                kind,
                ..
            }) = ev
            else {
                continue;
            };
            // Many terminals emit both Press and Release; only act on Press
            // so each keystroke isn't applied twice.
            if kind != KeyEventKind::Press {
                continue;
            }

            match (code, modifiers) {
                (KeyCode::Enter, _) => {
                    write!(stdout, "\r\n")?;
                    stdout.flush()?;
                    self.append_history(&buf);
                    return Ok(Some(buf));
                }
                (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                    write!(stdout, "^C\r\n")?;
                    stdout.flush()?;
                    return Ok(Some(String::new()));
                }
                (KeyCode::Char('d'), KeyModifiers::CONTROL) => {
                    if buf.is_empty() {
                        write!(stdout, "\r\n")?;
                        stdout.flush()?;
                        return Ok(None);
                    }
                    if cursor < buf.len() {
                        let next = next_boundary(&buf, cursor);
                        buf.drain(cursor..next);
                    }
                }
                (KeyCode::Char('a'), KeyModifiers::CONTROL) => cursor = 0,
                (KeyCode::Char('e'), KeyModifiers::CONTROL) => cursor = buf.len(),
                (KeyCode::Char('u'), KeyModifiers::CONTROL) => {
                    buf.drain(0..cursor);
                    cursor = 0;
                }
                (KeyCode::Char('k'), KeyModifiers::CONTROL) => {
                    buf.truncate(cursor);
                }
                (KeyCode::Char('l'), KeyModifiers::CONTROL) => {
                    queue!(
                        stdout,
                        crossterm::terminal::Clear(ClearType::All),
                        crossterm::cursor::MoveTo(0, 0)
                    )?;
                }
                (KeyCode::Char(c), m)
                    if m == KeyModifiers::NONE || m == KeyModifiers::SHIFT =>
                {
                    buf.insert(cursor, c);
                    cursor += c.len_utf8();
                }
                (KeyCode::Backspace, _) => {
                    if cursor > 0 {
                        let prev = prev_boundary(&buf, cursor);
                        buf.drain(prev..cursor);
                        cursor = prev;
                    }
                }
                (KeyCode::Delete, _) => {
                    if cursor < buf.len() {
                        let next = next_boundary(&buf, cursor);
                        buf.drain(cursor..next);
                    }
                }
                (KeyCode::Left, _) => {
                    if cursor > 0 {
                        cursor = prev_boundary(&buf, cursor);
                    }
                }
                (KeyCode::Right, _) => {
                    if cursor < buf.len() {
                        cursor = next_boundary(&buf, cursor);
                    }
                }
                (KeyCode::Home, _) => cursor = 0,
                (KeyCode::End, _) => cursor = buf.len(),
                (KeyCode::Up, _) => {
                    if self.history.is_empty() {
                        continue;
                    }
                    let new_pos = match hist_pos {
                        None => {
                            saved_buf = buf.clone();
                            self.history.len() - 1
                        }
                        Some(p) if p > 0 => p - 1,
                        Some(p) => p,
                    };
                    hist_pos = Some(new_pos);
                    buf.clone_from(&self.history[new_pos]);
                    cursor = buf.len();
                }
                (KeyCode::Down, _) => match hist_pos {
                    Some(p) if p + 1 < self.history.len() => {
                        hist_pos = Some(p + 1);
                        buf.clone_from(&self.history[p + 1]);
                        cursor = buf.len();
                    }
                    Some(_) => {
                        hist_pos = None;
                        buf = std::mem::take(&mut saved_buf);
                        cursor = buf.len();
                    }
                    None => continue,
                },
                _ => continue,
            }
            redraw(&mut stdout, prompt, &buf, cursor)?;
        }
    }
}

fn redraw(stdout: &mut io::Stdout, prompt: &str, buf: &str, cursor: usize) -> io::Result<()> {
    queue!(stdout, MoveToColumn(0), Clear(ClearType::CurrentLine))?;
    write!(stdout, "{prompt}{buf}")?;
    let target = (display_width(prompt) + display_width(&buf[..cursor])) as u16;
    queue!(stdout, MoveToColumn(target))?;
    stdout.flush()
}

fn prev_boundary(s: &str, idx: usize) -> usize {
    let mut i = idx.saturating_sub(1);
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn next_boundary(s: &str, idx: usize) -> usize {
    let mut i = idx + 1;
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
}

fn history_file() -> Result<PathBuf> {
    let cache_dir = std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_default();
            PathBuf::from(home).join(".cache")
        })
        .join("burnage");
    std::fs::create_dir_all(&cache_dir).context("creating cache dir")?;
    Ok(cache_dir.join("shell_history"))
}

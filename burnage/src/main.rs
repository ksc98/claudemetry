use anyhow::{anyhow, Context, Result};
use clap::builder::styling::{AnsiColor, Effects, Styles};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod quota;
mod shell;
mod usage;

use usage as do_usage;

const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Yellow.on_default().effects(Effects::BOLD))
    .usage(AnsiColor::Yellow.on_default().effects(Effects::BOLD))
    .literal(AnsiColor::Cyan.on_default().effects(Effects::BOLD))
    .placeholder(AnsiColor::Green.on_default());

const DEFAULT_URL: Option<&str> = option_env!("BURNAGE_DEFAULT_URL");

#[derive(Parser)]
#[command(version, about = "Minimal CLI for cc-proxy admin endpoints", styles = STYLES)]
struct Cli {
    /// Proxy base URL (falls back to $ANTHROPIC_BASE_URL, then the baked-in default).
    #[arg(long, env = "ANTHROPIC_BASE_URL")]
    url: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print your stable user_hash.
    Whoami,
    /// Aggregate counts and token totals.
    Stats,
    /// Recent transactions, newest first.
    Recent,
    /// Session operations.
    #[command(subcommand)]
    Session(SessionCmd),
    /// Cloudflare usage + live per-user Durable Object state.
    #[command(subcommand)]
    Quota(QuotaCmd),
    /// SQL shell against any DO via /_cm/admin/sql.
    Shell(ShellArgs),
}

#[derive(clap::Args)]
struct ShellArgs {
    /// Run a single SQL statement (or `;`-separated script) and exit.
    #[arg(short = 'c', long = "command", conflicts_with = "file")]
    command: Option<String>,
    /// Run statements from a SQL file and exit.
    #[arg(short = 'f', long = "file")]
    file: Option<PathBuf>,
    /// Target a specific DO by 16-hex user_hash. Default: your own.
    #[arg(long)]
    hash: Option<String>,
    /// Output format. Defaults: table for tty, json otherwise.
    #[arg(long)]
    format: Option<shell::Format>,
}

#[derive(Subcommand)]
enum QuotaCmd {
    /// Cloudflare Workers + Durable Objects usage vs Paid plan allocation.
    Cf(QuotaArgs),
    /// Live DO state: turns, storage bytes, active window, token totals.
    Do,
}

#[derive(clap::Args)]
struct QuotaArgs {
    /// Time window: 1h, 24h, 7d, 30d, or month (calendar month-to-date UTC).
    #[arg(default_value = "30d")]
    window: String,
    /// Cloudflare API token with Account Analytics: Read.
    #[arg(long, env = "CF_API_TOKEN", hide_env_values = true)]
    api_token: String,
    /// Cloudflare account ID.
    #[arg(long, env = "CF_ACCOUNT_ID")]
    account_id: String,
}

#[derive(Subcommand)]
enum SessionCmd {
    /// Mark a session as ended.
    End {
        /// Claude Code session_id (the value of x-claude-code-session-id).
        session_id: String,
    },
    /// List all recorded session end timestamps.
    Ends,
}

enum Method {
    Get,
    Post(serde_json::Value),
}

fn resolve_base(cli_url: Option<&str>) -> Result<&str> {
    cli_url
        .or(DEFAULT_URL)
        .ok_or_else(|| anyhow!("no proxy URL: pass --url, set $ANTHROPIC_BASE_URL, or bake in $BURNAGE_DEFAULT_URL at build time"))
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let url_opt = cli.url.clone();
    let (method, path) = match cli.cmd {
        Cmd::Quota(QuotaCmd::Cf(args)) => {
            return quota::run(quota::QuotaArgs {
                window: args.window,
                api_token: args.api_token,
                account_id: args.account_id,
            });
        }
        Cmd::Quota(QuotaCmd::Do) => {
            let token = read_token()?;
            let base = resolve_base(url_opt.as_deref())?;
            return do_usage::do_run(base, &token);
        }
        Cmd::Shell(args) => {
            let token = read_token()?;
            let base = resolve_base(url_opt.as_deref())?.to_string();
            return shell::run(shell::ShellOpts {
                base,
                token,
                hash: args.hash,
                command: args.command,
                file: args.file,
                format: args.format,
            });
        }
        Cmd::Whoami => (Method::Get, "/_cm/whoami"),
        Cmd::Stats => (Method::Get, "/_cm/stats"),
        Cmd::Recent => (Method::Get, "/_cm/recent"),
        Cmd::Session(SessionCmd::End { session_id }) => (
            Method::Post(serde_json::json!({ "session_id": session_id })),
            "/_cm/session/end",
        ),
        Cmd::Session(SessionCmd::Ends) => (Method::Get, "/_cm/sessions/ends"),
    };
    let token = read_token()?;
    let base = resolve_base(url_opt.as_deref())?;
    let url = format!("{}{}", base.trim_end_matches('/'), path);
    let auth = format!("Bearer {token}");
    let res = match method {
        Method::Get => ureq::get(&url).set("Authorization", &auth).call(),
        Method::Post(body) => ureq::post(&url)
            .set("Authorization", &auth)
            .send_json(body),
    };
    let body = match res {
        Ok(r) => r.into_string()?,
        Err(ureq::Error::Status(code, r)) => {
            let b = r.into_string().unwrap_or_default();
            return Err(anyhow!("HTTP {code}: {b}"));
        }
        Err(e) => return Err(anyhow!(e)),
    };
    match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(v) => println!("{}", serde_json::to_string_pretty(&v)?),
        Err(_) => println!("{body}"),
    }
    Ok(())
}

fn config_dir() -> Result<PathBuf> {
    if let Ok(d) = std::env::var("CLAUDE_CONFIG_DIR") {
        return Ok(PathBuf::from(d));
    }
    let home = std::env::var("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home).join(".claude"))
}

fn read_token() -> Result<String> {
    let path = config_dir()?.join(".credentials.json");
    let bytes = std::fs::read(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    let v: serde_json::Value = serde_json::from_slice(&bytes)?;
    v["claudeAiOauth"]["accessToken"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| anyhow!("missing claudeAiOauth.accessToken in {}", path.display()))
}

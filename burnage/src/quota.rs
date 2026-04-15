use anyhow::{anyhow, Result};
use chrono::{Datelike, Duration, Utc};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::IsTerminal;

// Workers Paid plan monthly allocation (included before overage).
// https://developers.cloudflare.com/workers/platform/pricing/
const LIMIT_REQ: f64 = 10_000_000.0;
const LIMIT_CPU_MS: f64 = 30_000_000.0;
const LIMIT_DO_REQ: f64 = 1_000_000.0;
const LIMIT_DO_BYTES: f64 = 5.0 * 1024.0 * 1024.0 * 1024.0;
const LIMIT_BUILD_MIN: f64 = 3000.0;

pub(crate) const BAR_WIDTH: usize = 24;
pub(crate) const DO_STORAGE_LIMIT: f64 = LIMIT_DO_BYTES;

const WORKERS_Q: &str = r#"query W($acct:String!,$from:Time!,$to:Time!){
  viewer{accounts(filter:{accountTag:$acct}){
    workersInvocationsAdaptive(
      filter:{datetime_geq:$from,datetime_leq:$to},
      limit:1000
    ){
      sum{requests errors subrequests cpuTimeUs}
      quantiles{cpuTimeP50 cpuTimeP99}
      dimensions{scriptName}
    }
  }}
}"#;

const DO_INV_Q: &str = r#"query D($acct:String!,$from:Time!,$to:Time!){
  viewer{accounts(filter:{accountTag:$acct}){
    durableObjectsInvocationsAdaptiveGroups(
      filter:{datetime_geq:$from,datetime_leq:$to},
      limit:1000
    ){
      sum{requests errors}
      quantiles{wallTimeP50 wallTimeP99}
      dimensions{scriptName namespaceId}
    }
  }}
}"#;

const DO_STORAGE_Q: &str = r#"query S($acct:String!,$from:Time!,$to:Time!){
  viewer{accounts(filter:{accountTag:$acct}){
    durableObjectsSqlStorageGroups(
      filter:{datetime_geq:$from,datetime_leq:$to},
      limit:1000,
      orderBy:[datetime_DESC]
    ){
      max{storedBytes}
      dimensions{namespaceId datetime}
    }
  }}
}"#;

const BUILD_Q: &str = r#"query B($acct:String!,$from:Time!,$to:Time!){
  viewer{accounts(filter:{accountTag:$acct}){
    workersBuildsBuildMinutesAdaptiveGroups(
      filter:{datetime_geq:$from,datetime_leq:$to},
      limit:1000
    ){
      sum{buildMinutes}
    }
  }}
}"#;

pub struct QuotaArgs {
    pub window: String,
    pub api_token: String,
    pub account_id: String,
}

pub fn run(args: QuotaArgs) -> Result<()> {
    let (from, to, label) = resolve_window(&args.window)?;
    let vars = json!({ "acct": args.account_id, "from": from, "to": to });

    let workers = gql(&args.api_token, WORKERS_Q, &vars)?;
    let do_inv = gql(&args.api_token, DO_INV_Q, &vars)?;
    let do_storage = gql(&args.api_token, DO_STORAGE_Q, &vars)?;
    let builds = gql(&args.api_token, BUILD_Q, &vars)?;

    let ws = aggregate_workers(&workers);
    let dos = aggregate_dos(&do_inv);
    let ns_map = namespace_script_map(&do_inv);
    let storage = aggregate_storage(&do_storage, &ns_map);
    let build_min = sum_build_minutes(&builds);

    let sty = Style::new(std::io::stdout().is_terminal());

    println!(
        "{} {} → {} {}",
        sty.dim("cf-usage"),
        &from[..from.find('T').unwrap_or(from.len())],
        &to[..to.find('T').unwrap_or(to.len())],
        sty.dim(&format!("({label})")),
    );
    println!();

    print_totals(&sty, &ws, &dos, &storage, build_min);
    println!();
    print_workers(&sty, &ws);
    println!();
    print_dos(&sty, &dos);
    println!();
    print_storage(&sty, &storage);

    Ok(())
}

fn resolve_window(w: &str) -> Result<(String, String, String)> {
    let now = Utc::now();
    let from = match w {
        "1h" => now - Duration::hours(1),
        "24h" => now - Duration::hours(24),
        "7d" => now - Duration::days(7),
        "30d" => now - Duration::days(30),
        "month" => {
            return Ok((
                format!("{:04}-{:02}-01T00:00:00Z", now.year(), now.month()),
                now.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                "month".into(),
            ));
        }
        other => return Err(anyhow!("invalid window '{other}' (use 1h|24h|7d|30d|month)")),
    };
    Ok((
        from.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        now.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        w.to_string(),
    ))
}

fn gql(token: &str, query: &str, vars: &Value) -> Result<Value> {
    let body = json!({ "query": query, "variables": vars });
    let res = ureq::post("https://api.cloudflare.com/client/v4/graphql")
        .set("Authorization", &format!("Bearer {token}"))
        .set("Content-Type", "application/json")
        .send_json(body);
    let text = match res {
        Ok(r) => r.into_string()?,
        Err(ureq::Error::Status(code, r)) => {
            let b = r.into_string().unwrap_or_default();
            return Err(anyhow!("graphql HTTP {code}: {b}"));
        }
        Err(e) => return Err(anyhow!(e)),
    };
    let v: Value = serde_json::from_str(&text)?;
    if let Some(errs) = v.get("errors").and_then(|e| e.as_array()) {
        if !errs.is_empty() {
            let msg = errs[0]
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown");
            return Err(anyhow!("graphql error: {msg}"));
        }
    }
    Ok(v)
}

struct WorkerRow {
    script: String,
    requests: u64,
    errors: u64,
    subreq: u64,
    cpu_us: u64,
    p50: u64,
    p99: u64,
}

fn aggregate_workers(v: &Value) -> Vec<WorkerRow> {
    let rows = account(v)
        .and_then(|a| a.get("workersInvocationsAdaptive"))
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut by_script: BTreeMap<String, WorkerRow> = BTreeMap::new();
    for r in rows {
        let script = r
            .pointer("/dimensions/scriptName")
            .and_then(|x| x.as_str())
            .unwrap_or("__unknown__")
            .to_string();
        let entry = by_script.entry(script.clone()).or_insert(WorkerRow {
            script,
            requests: 0,
            errors: 0,
            subreq: 0,
            cpu_us: 0,
            p50: 0,
            p99: 0,
        });
        entry.requests += u64_at(&r, "/sum/requests");
        entry.errors += u64_at(&r, "/sum/errors");
        entry.subreq += u64_at(&r, "/sum/subrequests");
        entry.cpu_us += u64_at(&r, "/sum/cpuTimeUs");
        entry.p50 = entry.p50.max(u64_at(&r, "/quantiles/cpuTimeP50"));
        entry.p99 = entry.p99.max(u64_at(&r, "/quantiles/cpuTimeP99"));
    }
    let mut out: Vec<_> = by_script.into_values().collect();
    out.sort_by(|a, b| b.requests.cmp(&a.requests));
    out
}

struct DoRow {
    script: String,
    requests: u64,
    errors: u64,
    p50: u64,
    p99: u64,
}

fn aggregate_dos(v: &Value) -> Vec<DoRow> {
    let rows = account(v)
        .and_then(|a| a.get("durableObjectsInvocationsAdaptiveGroups"))
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut by_script: BTreeMap<String, DoRow> = BTreeMap::new();
    for r in rows {
        let script = r
            .pointer("/dimensions/scriptName")
            .and_then(|x| x.as_str())
            .unwrap_or("__unknown__")
            .to_string();
        let entry = by_script.entry(script.clone()).or_insert(DoRow {
            script,
            requests: 0,
            errors: 0,
            p50: 0,
            p99: 0,
        });
        entry.requests += u64_at(&r, "/sum/requests");
        entry.errors += u64_at(&r, "/sum/errors");
        entry.p50 = entry.p50.max(u64_at(&r, "/quantiles/wallTimeP50"));
        entry.p99 = entry.p99.max(u64_at(&r, "/quantiles/wallTimeP99"));
    }
    let mut out: Vec<_> = by_script.into_values().collect();
    out.sort_by(|a, b| b.requests.cmp(&a.requests));
    out
}

fn namespace_script_map(v: &Value) -> BTreeMap<String, String> {
    let rows = account(v)
        .and_then(|a| a.get("durableObjectsInvocationsAdaptiveGroups"))
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut m = BTreeMap::new();
    for r in rows {
        let ns = r
            .pointer("/dimensions/namespaceId")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let sn = r
            .pointer("/dimensions/scriptName")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        if !ns.is_empty() && !sn.is_empty() {
            m.entry(ns).or_insert(sn);
        }
    }
    m
}

struct StorageRow {
    label: String,
    bytes: u64,
}

fn aggregate_storage(v: &Value, ns_map: &BTreeMap<String, String>) -> Vec<StorageRow> {
    let rows = account(v)
        .and_then(|a| a.get("durableObjectsSqlStorageGroups"))
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut latest: BTreeMap<String, (String, u64)> = BTreeMap::new();
    for r in rows {
        let ns = r
            .pointer("/dimensions/namespaceId")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let dt = r
            .pointer("/dimensions/datetime")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let bytes = u64_at(&r, "/max/storedBytes");
        latest
            .entry(ns)
            .and_modify(|(cur_dt, cur_b)| {
                if dt > *cur_dt {
                    *cur_dt = dt.clone();
                    *cur_b = bytes;
                }
            })
            .or_insert((dt, bytes));
    }
    latest
        .into_iter()
        .map(|(ns, (_, bytes))| StorageRow {
            label: ns_map.get(&ns).cloned().unwrap_or(ns),
            bytes,
        })
        .collect()
}

fn sum_build_minutes(v: &Value) -> f64 {
    account(v)
        .and_then(|a| a.get("workersBuildsBuildMinutesAdaptiveGroups"))
        .and_then(|x| x.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|r| r.pointer("/sum/buildMinutes").and_then(|x| x.as_f64()))
                .sum()
        })
        .unwrap_or(0.0)
}

fn account(v: &Value) -> Option<&Value> {
    v.pointer("/data/viewer/accounts/0")
}

fn u64_at(v: &Value, ptr: &str) -> u64 {
    v.pointer(ptr)
        .and_then(|x| x.as_u64().or_else(|| x.as_f64().map(|f| f as u64)))
        .unwrap_or(0)
}

fn print_totals(
    sty: &Style,
    ws: &[WorkerRow],
    dos: &[DoRow],
    storage: &[StorageRow],
    build_min: f64,
) {
    let total_req: u64 = ws.iter().map(|w| w.requests).sum();
    let total_err: u64 = ws.iter().map(|w| w.errors).sum();
    let total_sub: u64 = ws.iter().map(|w| w.subreq).sum();
    let total_cpu_ms: u64 = ws.iter().map(|w| w.cpu_us).sum::<u64>() / 1000;
    let total_do_req: u64 = dos.iter().map(|d| d.requests).sum();
    let total_do_bytes: u64 = storage.iter().map(|s| s.bytes).sum();

    println!("{}", sty.header("ACCOUNT TOTALS"));
    println!(
        "{}",
        sty.dim("  (monthly allocation included in Workers Paid plan)")
    );
    let items: Vec<(&str, String, String, f64)> = vec![
        (
            "requests",
            human_count(total_req),
            human_count(LIMIT_REQ as u64),
            total_req as f64 / LIMIT_REQ,
        ),
        (
            "cpu time",
            human_ms(total_cpu_ms),
            human_ms(LIMIT_CPU_MS as u64),
            total_cpu_ms as f64 / LIMIT_CPU_MS,
        ),
        (
            "do requests",
            human_count(total_do_req),
            human_count(LIMIT_DO_REQ as u64),
            total_do_req as f64 / LIMIT_DO_REQ,
        ),
        (
            "do storage",
            human_bytes(total_do_bytes),
            human_bytes(LIMIT_DO_BYTES as u64),
            total_do_bytes as f64 / LIMIT_DO_BYTES,
        ),
        (
            "build mins",
            format!("{build_min:.1}"),
            format!("{}", LIMIT_BUILD_MIN as u64),
            build_min / LIMIT_BUILD_MIN,
        ),
    ];

    let label_w = items.iter().map(|i| i.0.len()).max().unwrap_or(0);
    let val_w = items
        .iter()
        .map(|i| i.1.chars().count() + 3 + i.2.chars().count())
        .max()
        .unwrap_or(0);

    for (label, used, limit, frac) in &items {
        let used_limit = format!("{used} / {limit}");
        let pad_label = " ".repeat(label_w - label.len());
        let pad_val = " ".repeat(val_w.saturating_sub(used_limit.chars().count()));
        let pct_str = format_pct(*frac);
        println!(
            "  {}{}  {}{}  {}  {}",
            label,
            pad_label,
            used_limit,
            pad_val,
            bar(sty, *frac, BAR_WIDTH),
            sty.bold_color(&pct_str, frac_color(*frac)),
        );
    }

    println!(
        "  {}{}  {}",
        "errors",
        " ".repeat(label_w - "errors".len()),
        sty.dim(&human_count(total_err)),
    );
    println!(
        "  {}{}  {}",
        "subrequests",
        " ".repeat(label_w - "subrequests".len()),
        sty.dim(&human_count(total_sub)),
    );
}

#[derive(Copy, Clone)]
enum Align {
    Left,
    Right,
}

struct Col<'a> {
    header: &'a str,
    align: Align,
}

fn print_table(sty: &Style, cols: &[Col], rows: &[Vec<String>]) {
    if rows.is_empty() {
        return;
    }
    let widths: Vec<usize> = (0..cols.len())
        .map(|i| {
            let h = cols[i].header.chars().count();
            let r = rows
                .iter()
                .map(|row| row.get(i).map_or(0, |c| visible_len(c)))
                .max()
                .unwrap_or(0);
            h.max(r)
        })
        .collect();

    let mut header_line = String::from("  ");
    for (i, col) in cols.iter().enumerate() {
        if i > 0 {
            header_line.push_str("   ");
        }
        let w = widths[i];
        match col.align {
            Align::Left => {
                header_line.push_str(col.header);
                for _ in 0..w.saturating_sub(col.header.chars().count()) {
                    header_line.push(' ');
                }
            }
            Align::Right => {
                for _ in 0..w.saturating_sub(col.header.chars().count()) {
                    header_line.push(' ');
                }
                header_line.push_str(col.header);
            }
        }
    }
    println!("{}", sty.dim(header_line.trim_end()));

    for row in rows {
        let mut line = String::from("  ");
        for (i, col) in cols.iter().enumerate() {
            if i > 0 {
                line.push_str("   ");
            }
            let w = widths[i];
            let cell = row.get(i).cloned().unwrap_or_default();
            let pad = w.saturating_sub(visible_len(&cell));
            match col.align {
                Align::Left => {
                    line.push_str(&cell);
                    for _ in 0..pad {
                        line.push(' ');
                    }
                }
                Align::Right => {
                    for _ in 0..pad {
                        line.push(' ');
                    }
                    line.push_str(&cell);
                }
            }
        }
        println!("{}", line.trim_end());
    }
}

fn visible_len(s: &str) -> usize {
    let mut len = 0;
    let mut in_esc = false;
    for c in s.chars() {
        if in_esc {
            if c == 'm' {
                in_esc = false;
            }
            continue;
        }
        if c == '\x1b' {
            in_esc = true;
            continue;
        }
        len += 1;
    }
    len
}

fn print_workers(sty: &Style, ws: &[WorkerRow]) {
    println!("{}", sty.header("WORKERS"));
    if ws.is_empty() {
        println!("  {}", sty.dim("(no invocations in window)"));
        return;
    }
    let total_req: u64 = ws.iter().map(|w| w.requests).sum();
    let total_cpu_us: u64 = ws.iter().map(|w| w.cpu_us).sum();

    let cols = [
        Col { header: "script",   align: Align::Left },
        Col { header: "requests", align: Align::Right },
        Col { header: "share",    align: Align::Right },
        Col { header: "cpu",      align: Align::Right },
        Col { header: "share",    align: Align::Right },
        Col { header: "p50 (µs)", align: Align::Right },
        Col { header: "p99 (µs)", align: Align::Right },
        Col { header: "subreq",   align: Align::Right },
        Col { header: "errors",   align: Align::Right },
    ];

    let rows: Vec<Vec<String>> = ws
        .iter()
        .map(|w| {
            let req_share = frac(w.requests, total_req);
            let cpu_share = if total_cpu_us > 0 {
                w.cpu_us as f64 / total_cpu_us as f64
            } else {
                0.0
            };
            let err_cell = if w.errors > 0 {
                sty.red(&human_count(w.errors))
            } else {
                sty.dim("0")
            };
            vec![
                w.script.clone(),
                human_count(w.requests),
                sty.dim(&format!("{:.1}%", req_share * 100.0)),
                human_ms(w.cpu_us / 1000),
                sty.dim(&format!("{:.1}%", cpu_share * 100.0)),
                w.p50.to_string(),
                w.p99.to_string(),
                human_count(w.subreq),
                err_cell,
            ]
        })
        .collect();

    print_table(sty, &cols, &rows);
}

fn print_dos(sty: &Style, dos: &[DoRow]) {
    println!("{}", sty.header("DURABLE OBJECTS (invocations)"));
    if dos.is_empty() {
        println!("  {}", sty.dim("(no invocations in window)"));
        return;
    }
    let total_req: u64 = dos.iter().map(|d| d.requests).sum();

    let cols = [
        Col { header: "script",   align: Align::Left },
        Col { header: "requests", align: Align::Right },
        Col { header: "share",    align: Align::Right },
        Col { header: "p50 (µs)", align: Align::Right },
        Col { header: "p99 (µs)", align: Align::Right },
        Col { header: "errors",   align: Align::Right },
    ];

    let rows: Vec<Vec<String>> = dos
        .iter()
        .map(|d| {
            let share = frac(d.requests, total_req);
            let err_cell = if d.errors > 0 {
                sty.red(&human_count(d.errors))
            } else {
                sty.dim("0")
            };
            vec![
                d.script.clone(),
                human_count(d.requests),
                sty.dim(&format!("{:.1}%", share * 100.0)),
                d.p50.to_string(),
                d.p99.to_string(),
                err_cell,
            ]
        })
        .collect();

    print_table(sty, &cols, &rows);
}

fn print_storage(sty: &Style, storage: &[StorageRow]) {
    println!("{}", sty.header("DURABLE OBJECTS (sqlite storage, current)"));
    if storage.is_empty() {
        println!("  {}", sty.dim("(no storage reported)"));
        return;
    }
    let total: u64 = storage.iter().map(|s| s.bytes).sum();

    let cols = [
        Col { header: "script", align: Align::Left },
        Col { header: "bytes",  align: Align::Right },
        Col { header: "share",  align: Align::Right },
    ];

    let mut rows: Vec<Vec<String>> = storage
        .iter()
        .map(|s| {
            let share = frac(s.bytes, total);
            vec![
                s.label.clone(),
                human_bytes(s.bytes),
                sty.dim(&format!("{:.1}%", share * 100.0)),
            ]
        })
        .collect();
    rows.push(vec![
        sty.dim("total"),
        sty.bold(&human_bytes(total)),
        String::new(),
    ]);

    print_table(sty, &cols, &rows);
}

fn frac(a: u64, b: u64) -> f64 {
    if b == 0 {
        0.0
    } else {
        a as f64 / b as f64
    }
}

pub(crate) fn human_count(n: u64) -> String {
    let f = n as f64;
    if f >= 1e9 {
        format!("{:.2}B", f / 1e9)
    } else if f >= 1e6 {
        format!("{:.2}M", f / 1e6)
    } else if f >= 1e3 {
        format!("{:.2}k", f / 1e3)
    } else {
        n.to_string()
    }
}

fn human_ms(ms: u64) -> String {
    let f = ms as f64;
    if f >= 60_000.0 {
        format!("{:.1} min", f / 60_000.0)
    } else if f >= 1_000.0 {
        format!("{:.1} s", f / 1_000.0)
    } else {
        format!("{ms} ms")
    }
}

pub(crate) fn human_bytes(n: u64) -> String {
    let mut f = n as f64;
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut i = 0;
    while f >= 1024.0 && i + 1 < units.len() {
        f /= 1024.0;
        i += 1;
    }
    format!("{:.1} {}", f, units[i])
}

pub(crate) fn format_pct(frac: f64) -> String {
    let p = frac * 100.0;
    if p >= 100.0 {
        format!("{p:.0}%")
    } else if p >= 10.0 {
        format!("{p:.1}%")
    } else if p >= 1.0 {
        format!("{p:.2}%")
    } else {
        format!("{p:.3}%")
    }
}

fn frac_color(frac: f64) -> &'static str {
    if frac >= 0.80 {
        "31" // red
    } else if frac >= 0.50 {
        "33" // yellow
    } else {
        "32" // green
    }
}

pub(crate) fn bar(sty: &Style, frac: f64, width: usize) -> String {
    let filled = ((frac.min(1.0).max(0.0)) * width as f64).round() as usize;
    let empty = width.saturating_sub(filled);
    let fill: String = "█".repeat(filled);
    let rest: String = "░".repeat(empty);
    let color = frac_color(frac);
    format!("{}{}", sty.color(&fill, color), sty.dim(&rest))
}

pub(crate) struct Style {
    enabled: bool,
}

impl Style {
    pub(crate) fn new(tty: bool) -> Self {
        Self { enabled: tty }
    }
    fn wrap(&self, s: &str, open: &str) -> String {
        if self.enabled {
            format!("\x1b[{open}m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }
    pub(crate) fn bold(&self, s: &str) -> String {
        self.wrap(s, "1")
    }
    pub(crate) fn dim(&self, s: &str) -> String {
        self.wrap(s, "2")
    }
    #[allow(dead_code)]
    pub(crate) fn red(&self, s: &str) -> String {
        self.wrap(s, "31")
    }
    pub(crate) fn header(&self, s: &str) -> String {
        self.wrap(s, "1;36")
    }
    pub(crate) fn color(&self, s: &str, code: &str) -> String {
        self.wrap(s, code)
    }
    #[allow(dead_code)]
    pub(crate) fn bold_color(&self, s: &str, code: &str) -> String {
        self.wrap(s, &format!("1;{code}"))
    }
}

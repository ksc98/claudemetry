use anyhow::{anyhow, Result};
use chrono::{Datelike, Duration, Utc};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::IsTerminal;

// Workers Paid plan monthly allocation (included before overage).
// Cloudflare measures storage in decimal GB (1 GB = 1_000_000_000 bytes).
// https://developers.cloudflare.com/workers/platform/pricing/
// https://developers.cloudflare.com/durable-objects/platform/pricing/
// https://developers.cloudflare.com/durable-objects/platform/limits/
// https://developers.cloudflare.com/vectorize/platform/pricing/
const LIMIT_REQ: f64 = 10_000_000.0;
const LIMIT_CPU_MS: f64 = 30_000_000.0;
const LIMIT_DO_REQ: f64 = 1_000_000.0;
const LIMIT_DO_DURATION_GB_S: f64 = 400_000.0;
const LIMIT_DO_ROWS_READ: f64 = 25_000_000_000.0;
const LIMIT_DO_ROWS_WRITTEN: f64 = 50_000_000.0;
const LIMIT_DO_BYTES: f64 = 5.0e9;
const LIMIT_BUILD_MIN: f64 = 3_000.0;
const LIMIT_VEC_QUERIED_DIMS: f64 = 50_000_000.0;
const LIMIT_VEC_STORED_DIMS: f64 = 10_000_000.0;
// Workers AI on the Paid plan includes 10,000 neurons/day. There's no
// monthly pool — the daily allocation is multiplied by the number of days
// in the window to produce an approximate "included" total. Overage is
// billed at $0.011 per 1,000 neurons.
// https://developers.cloudflare.com/workers-ai/platform/pricing/
const AI_NEURONS_PER_DAY: f64 = 10_000.0;

// Overage pricing (USD per overage unit). These are used to estimate what
// you'd pay if the current usage persisted for the full billing month —
// they are "what if" projections against the displayed bars, not a
// statement about what Cloudflare will actually charge (billing is
// per-calendar-month, not per-query-window).
const RATE_REQ_PER_M: f64 = 0.30;
const RATE_CPU_PER_M_MS: f64 = 0.02;
const RATE_DO_REQ_PER_M: f64 = 0.15;
const RATE_DO_DURATION_PER_M_GBS: f64 = 12.50;
const RATE_DO_ROWS_READ_PER_M: f64 = 0.001;
const RATE_DO_ROWS_WRITTEN_PER_M: f64 = 1.0;
const RATE_DO_STORAGE_PER_GB: f64 = 0.20;
const RATE_VEC_QUERIED_PER_M: f64 = 0.01;
const RATE_VEC_STORED_PER_100M: f64 = 0.05;
const RATE_AI_PER_1K: f64 = 0.011;

pub(crate) const BAR_WIDTH: usize = 24;
// Per-Durable-Object SQLite storage hard cap (not the account-wide monthly
// allocation). Used for the single-DO storage bar in `usage.rs`.
pub(crate) const DO_STORAGE_LIMIT: f64 = 10.0e9;

const WORKERS_Q: &str = r#"query W($acct:String!,$from:Time!,$to:Time!){
  viewer{accounts(filter:{accountTag:$acct}){
    workersInvocationsAdaptive(
      filter:{datetime_geq:$from,datetime_leq:$to},
      limit:1000
    ){
      sum{requests errors subrequests cpuTimeUs wallTime duration responseBodySize clientDisconnects}
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

const DO_PERIODIC_Q: &str = r#"query P($acct:String!,$from:Time!,$to:Time!){
  viewer{accounts(filter:{accountTag:$acct}){
    durableObjectsPeriodicGroups(
      filter:{datetime_geq:$from,datetime_leq:$to},
      limit:10000
    ){
      sum{duration cpuTime activeTime rowsRead rowsWritten exceededCpuErrors exceededMemoryErrors subrequests}
      dimensions{namespaceId}
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

const VEC_QUERIES_Q: &str = r#"query VQ($acct:String!,$from:Time!,$to:Time!){
  viewer{accounts(filter:{accountTag:$acct}){
    vectorizeV2QueriesAdaptiveGroups(
      filter:{datetime_geq:$from,datetime_leq:$to},
      limit:1000
    ){
      sum{queriedVectorDimensions servedVectorCount requestDurationMs}
      dimensions{indexName}
    }
  }}
}"#;

const VEC_STORAGE_Q: &str = r#"query VS($acct:String!,$from:Time!,$to:Time!){
  viewer{accounts(filter:{accountTag:$acct}){
    vectorizeV2StorageAdaptiveGroups(
      filter:{datetime_geq:$from,datetime_leq:$to},
      limit:1000,
      orderBy:[datetime_DESC]
    ){
      max{storedVectorDimensions vectorCount}
      dimensions{indexName datetime}
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

const AI_Q: &str = r#"query AI($acct:String!,$from:Time!,$to:Time!){
  viewer{accounts(filter:{accountTag:$acct}){
    aiInferenceAdaptiveGroups(
      filter:{datetime_geq:$from,datetime_leq:$to},
      limit:1000
    ){
      sum{totalNeurons}
      count
      dimensions{modelId}
    }
  }}
}"#;

pub struct QuotaArgs {
    pub window: String,
    pub api_token: Option<String>,
    pub account_id: Option<String>,
}

pub fn run(args: QuotaArgs) -> Result<()> {
    let sty = Style::new(std::io::stdout().is_terminal());

    let (api_token, account_id) = match (args.api_token, args.account_id) {
        (Some(t), Some(a)) => (t, a),
        _ => {
            println!("{}", sty.header("CLOUDFLARE ACCOUNT"));
            println!(
                "  {}",
                sty.dim("set CF_API_TOKEN + CF_ACCOUNT_ID for Workers/DO account totals")
            );
            return Ok(());
        }
    };

    let (from, to, label) = resolve_window(&args.window)?;
    let vars = json!({ "acct": account_id, "from": from, "to": to });

    let workers = gql(&api_token, WORKERS_Q, &vars)?;
    let do_inv = gql(&api_token, DO_INV_Q, &vars)?;
    let do_periodic = gql(&api_token, DO_PERIODIC_Q, &vars)?;
    let do_storage = gql(&api_token, DO_STORAGE_Q, &vars)?;
    let vec_queries = gql(&api_token, VEC_QUERIES_Q, &vars)?;
    let vec_storage = gql(&api_token, VEC_STORAGE_Q, &vars)?;
    let builds = gql(&api_token, BUILD_Q, &vars)?;
    let ai = gql(&api_token, AI_Q, &vars)?;
    // Workers AI resets at 00:00 UTC, so a separate "today" query tracks
    // the only window that actually matters for the daily allocation.
    let today_from = format!(
        "{:04}-{:02}-{:02}T00:00:00Z",
        Utc::now().year(),
        Utc::now().month(),
        Utc::now().day()
    );
    let ai_today_vars = json!({
        "acct": account_id,
        "from": today_from,
        "to": to,
    });
    let ai_today = gql(&api_token, AI_Q, &ai_today_vars)?;

    let ws = aggregate_workers(&workers);
    let dos = aggregate_dos(&do_inv);
    let ns_map = namespace_script_map(&do_inv);
    let do_usage = aggregate_do_periodic(&do_periodic, &ns_map);
    let storage = aggregate_storage(&do_storage, &ns_map);
    let vec_q = aggregate_vec_queries(&vec_queries);
    let vec_s = aggregate_vec_storage(&vec_storage);
    let build_min = sum_build_minutes(&builds);
    let ai_rows = aggregate_ai(&ai);
    let ai_today_rows = aggregate_ai(&ai_today);

    println!(
        "{} {} → {} {}",
        sty.dim("cf-usage"),
        &from[..from.find('T').unwrap_or(from.len())],
        &to[..to.find('T').unwrap_or(to.len())],
        sty.dim(&format!("({label})")),
    );
    println!();

    print_totals(
        &sty,
        &ws,
        &dos,
        &do_usage,
        &storage,
        &vec_q,
        &vec_s,
        build_min,
        &ai_rows,
        &ai_today_rows,
    );
    println!();
    print_workers(&sty, &ws);
    println!();
    print_dos(&sty, &dos, &do_usage);
    println!();
    print_storage(&sty, &storage);
    println!();
    print_vectorize(&sty, &vec_q, &vec_s);
    println!();
    print_workers_ai(&sty, &ai_rows);

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
    wall_us: u64,
    duration_gb_s: f64,
    resp_bytes: u64,
    client_disconnects: u64,
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
            wall_us: 0,
            duration_gb_s: 0.0,
            resp_bytes: 0,
            client_disconnects: 0,
            p50: 0,
            p99: 0,
        });
        entry.requests += u64_at(&r, "/sum/requests");
        entry.errors += u64_at(&r, "/sum/errors");
        entry.subreq += u64_at(&r, "/sum/subrequests");
        entry.cpu_us += u64_at(&r, "/sum/cpuTimeUs");
        entry.wall_us += u64_at(&r, "/sum/wallTime");
        entry.duration_gb_s += f64_at(&r, "/sum/duration");
        entry.resp_bytes += u64_at(&r, "/sum/responseBodySize");
        entry.client_disconnects += u64_at(&r, "/sum/clientDisconnects");
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

#[derive(Default)]
struct DoUsageRow {
    script: String,
    duration_gb_s: f64,
    cpu_us: u64,
    active_us: u64,
    rows_read: u64,
    rows_written: u64,
    subrequests: u64,
    exceeded_cpu: u64,
    exceeded_mem: u64,
}

fn aggregate_do_periodic(
    v: &Value,
    ns_map: &BTreeMap<String, String>,
) -> Vec<DoUsageRow> {
    let rows = account(v)
        .and_then(|a| a.get("durableObjectsPeriodicGroups"))
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut by_script: BTreeMap<String, DoUsageRow> = BTreeMap::new();
    for r in rows {
        let ns = r
            .pointer("/dimensions/namespaceId")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let script = ns_map.get(&ns).cloned().unwrap_or_else(|| {
            if ns.is_empty() {
                "__unknown__".into()
            } else {
                format!("ns:{}", &ns[..ns.len().min(8)])
            }
        });
        let entry = by_script.entry(script.clone()).or_insert(DoUsageRow {
            script,
            ..Default::default()
        });
        entry.duration_gb_s += f64_at(&r, "/sum/duration");
        entry.cpu_us += u64_at(&r, "/sum/cpuTime");
        entry.active_us += u64_at(&r, "/sum/activeTime");
        entry.rows_read += u64_at(&r, "/sum/rowsRead");
        entry.rows_written += u64_at(&r, "/sum/rowsWritten");
        entry.subrequests += u64_at(&r, "/sum/subrequests");
        entry.exceeded_cpu += u64_at(&r, "/sum/exceededCpuErrors");
        entry.exceeded_mem += u64_at(&r, "/sum/exceededMemoryErrors");
    }
    let mut out: Vec<_> = by_script.into_values().collect();
    out.sort_by(|a, b| {
        b.duration_gb_s
            .partial_cmp(&a.duration_gb_s)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out
}

struct VecQueryRow {
    index: String,
    queried_dims: u64,
    served: u64,
    duration_ms: u64,
}

fn aggregate_vec_queries(v: &Value) -> Vec<VecQueryRow> {
    let rows = account(v)
        .and_then(|a| a.get("vectorizeV2QueriesAdaptiveGroups"))
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut by_index: BTreeMap<String, VecQueryRow> = BTreeMap::new();
    for r in rows {
        let index = r
            .pointer("/dimensions/indexName")
            .and_then(|x| x.as_str())
            .unwrap_or("__unknown__")
            .to_string();
        let entry = by_index.entry(index.clone()).or_insert(VecQueryRow {
            index,
            queried_dims: 0,
            served: 0,
            duration_ms: 0,
        });
        entry.queried_dims += u64_at(&r, "/sum/queriedVectorDimensions");
        entry.served += u64_at(&r, "/sum/servedVectorCount");
        entry.duration_ms += u64_at(&r, "/sum/requestDurationMs");
    }
    let mut out: Vec<_> = by_index.into_values().collect();
    out.sort_by(|a, b| b.queried_dims.cmp(&a.queried_dims));
    out
}

struct VecStorageRow {
    index: String,
    stored_dims: u64,
    vector_count: u64,
}

fn aggregate_vec_storage(v: &Value) -> Vec<VecStorageRow> {
    let rows = account(v)
        .and_then(|a| a.get("vectorizeV2StorageAdaptiveGroups"))
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut latest: BTreeMap<String, (String, u64, u64)> = BTreeMap::new();
    for r in rows {
        let index = r
            .pointer("/dimensions/indexName")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let dt = r
            .pointer("/dimensions/datetime")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let dims = u64_at(&r, "/max/storedVectorDimensions");
        let vc = u64_at(&r, "/max/vectorCount");
        latest
            .entry(index)
            .and_modify(|(cur_dt, cur_d, cur_v)| {
                if dt > *cur_dt {
                    *cur_dt = dt.clone();
                    *cur_d = dims;
                    *cur_v = vc;
                }
            })
            .or_insert((dt, dims, vc));
    }
    latest
        .into_iter()
        .map(|(index, (_, stored_dims, vector_count))| VecStorageRow {
            index,
            stored_dims,
            vector_count,
        })
        .collect()
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

struct AiRow {
    model: String,
    requests: u64,
    neurons: f64,
}

fn aggregate_ai(v: &Value) -> Vec<AiRow> {
    let rows = account(v)
        .and_then(|a| a.get("aiInferenceAdaptiveGroups"))
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut by_model: BTreeMap<String, AiRow> = BTreeMap::new();
    for r in rows {
        let model = r
            .pointer("/dimensions/modelId")
            .and_then(|x| x.as_str())
            .unwrap_or("__unknown__")
            .to_string();
        let entry = by_model.entry(model.clone()).or_insert(AiRow {
            model,
            requests: 0,
            neurons: 0.0,
        });
        entry.requests += u64_at(&r, "/count");
        entry.neurons += f64_at(&r, "/sum/totalNeurons");
    }
    let mut out: Vec<_> = by_model.into_values().collect();
    out.sort_by(|a, b| {
        b.neurons
            .partial_cmp(&a.neurons)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out
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

fn f64_at(v: &Value, ptr: &str) -> f64 {
    v.pointer(ptr)
        .and_then(|x| x.as_f64().or_else(|| x.as_u64().map(|u| u as f64)))
        .unwrap_or(0.0)
}

fn print_totals(
    sty: &Style,
    ws: &[WorkerRow],
    dos: &[DoRow],
    do_usage: &[DoUsageRow],
    storage: &[StorageRow],
    vec_q: &[VecQueryRow],
    vec_s: &[VecStorageRow],
    build_min: f64,
    ai: &[AiRow],
    ai_today: &[AiRow],
) {
    let total_req: u64 = ws.iter().map(|w| w.requests).sum();
    let total_err: u64 = ws.iter().map(|w| w.errors).sum();
    let total_sub: u64 = ws.iter().map(|w| w.subreq).sum();
    let total_cpu_ms: u64 = ws.iter().map(|w| w.cpu_us).sum::<u64>() / 1000;
    let total_wall_ms: u64 = ws.iter().map(|w| w.wall_us).sum::<u64>() / 1000;
    let total_resp_bytes: u64 = ws.iter().map(|w| w.resp_bytes).sum();
    let total_disconnects: u64 = ws.iter().map(|w| w.client_disconnects).sum();
    let total_do_req: u64 = dos.iter().map(|d| d.requests).sum();
    let total_do_err: u64 = dos.iter().map(|d| d.errors).sum();
    let total_do_duration: f64 = do_usage.iter().map(|d| d.duration_gb_s).sum();
    let total_do_rows_read: u64 = do_usage.iter().map(|d| d.rows_read).sum();
    let total_do_rows_written: u64 = do_usage.iter().map(|d| d.rows_written).sum();
    let total_do_bytes: u64 = storage.iter().map(|s| s.bytes).sum();
    let total_do_exceeded_cpu: u64 = do_usage.iter().map(|d| d.exceeded_cpu).sum();
    let total_do_exceeded_mem: u64 = do_usage.iter().map(|d| d.exceeded_mem).sum();
    let total_vec_queried: u64 = vec_q.iter().map(|v| v.queried_dims).sum();
    let total_vec_stored: u64 = vec_s.iter().map(|v| v.stored_dims).sum();
    let total_ai_requests: u64 = ai.iter().map(|r| r.requests).sum();
    let total_ai_neurons: f64 = ai.iter().map(|r| r.neurons).sum();
    // Workers AI resets at 00:00 UTC and does not pool across days, so the
    // "billable" comparison is today's usage vs the 10k/day allocation.
    let total_ai_neurons_today: f64 = ai_today.iter().map(|r| r.neurons).sum();
    let total_ai_requests_today: u64 = ai_today.iter().map(|r| r.requests).sum();

    println!("{}", sty.header("ACCOUNT TOTALS"));
    println!(
        "{}",
        sty.dim(
            "  (allocation included in Workers Paid plan; $ = projected overage if usage persists)"
        )
    );
    // Overage $ is a "what if usage persisted through end-of-month"
    // projection for pooled metrics (requests, CPU, DO, Vectorize) and
    // windowed daily-avg projection for AI neurons. Not an authoritative
    // bill; the rates are from CF's public pricing pages.
    fn overage(used: f64, limit: f64, rate_per_unit: f64) -> f64 {
        ((used - limit).max(0.0)) * rate_per_unit
    }
    // AI overage: today's usage beyond the 10k daily allocation. Bills
    // happen per calendar day; this is the only honest number to show.
    let ai_overage_usd = (total_ai_neurons_today - AI_NEURONS_PER_DAY).max(0.0)
        * RATE_AI_PER_1K
        / 1_000.0;

    let items: Vec<(&str, String, String, f64, f64)> = vec![
        (
            "requests",
            human_count(total_req),
            human_count(LIMIT_REQ as u64),
            total_req as f64 / LIMIT_REQ,
            overage(total_req as f64, LIMIT_REQ, RATE_REQ_PER_M / 1e6),
        ),
        (
            "cpu time",
            human_ms(total_cpu_ms),
            human_ms(LIMIT_CPU_MS as u64),
            total_cpu_ms as f64 / LIMIT_CPU_MS,
            overage(total_cpu_ms as f64, LIMIT_CPU_MS, RATE_CPU_PER_M_MS / 1e6),
        ),
        (
            "do requests",
            human_count(total_do_req),
            human_count(LIMIT_DO_REQ as u64),
            total_do_req as f64 / LIMIT_DO_REQ,
            overage(total_do_req as f64, LIMIT_DO_REQ, RATE_DO_REQ_PER_M / 1e6),
        ),
        (
            "do duration",
            format!("{} GB-s", human_num(total_do_duration)),
            format!("{} GB-s", human_num(LIMIT_DO_DURATION_GB_S)),
            total_do_duration / LIMIT_DO_DURATION_GB_S,
            overage(
                total_do_duration,
                LIMIT_DO_DURATION_GB_S,
                RATE_DO_DURATION_PER_M_GBS / 1e6,
            ),
        ),
        (
            "do rows read",
            human_count(total_do_rows_read),
            human_count(LIMIT_DO_ROWS_READ as u64),
            total_do_rows_read as f64 / LIMIT_DO_ROWS_READ,
            overage(
                total_do_rows_read as f64,
                LIMIT_DO_ROWS_READ,
                RATE_DO_ROWS_READ_PER_M / 1e6,
            ),
        ),
        (
            "do rows written",
            human_count(total_do_rows_written),
            human_count(LIMIT_DO_ROWS_WRITTEN as u64),
            total_do_rows_written as f64 / LIMIT_DO_ROWS_WRITTEN,
            overage(
                total_do_rows_written as f64,
                LIMIT_DO_ROWS_WRITTEN,
                RATE_DO_ROWS_WRITTEN_PER_M / 1e6,
            ),
        ),
        (
            "do storage",
            human_bytes(total_do_bytes),
            human_bytes(LIMIT_DO_BYTES as u64),
            total_do_bytes as f64 / LIMIT_DO_BYTES,
            // DO storage is priced per GB-month; convert bytes over limit
            // into decimal GB (Cloudflare's convention) and multiply by rate.
            overage(
                total_do_bytes as f64 / 1e9,
                LIMIT_DO_BYTES / 1e9,
                RATE_DO_STORAGE_PER_GB,
            ),
        ),
        (
            "vec queried",
            human_count(total_vec_queried),
            human_count(LIMIT_VEC_QUERIED_DIMS as u64),
            total_vec_queried as f64 / LIMIT_VEC_QUERIED_DIMS,
            overage(
                total_vec_queried as f64,
                LIMIT_VEC_QUERIED_DIMS,
                RATE_VEC_QUERIED_PER_M / 1e6,
            ),
        ),
        (
            "vec stored",
            human_count(total_vec_stored),
            human_count(LIMIT_VEC_STORED_DIMS as u64),
            total_vec_stored as f64 / LIMIT_VEC_STORED_DIMS,
            overage(
                total_vec_stored as f64,
                LIMIT_VEC_STORED_DIMS,
                RATE_VEC_STORED_PER_100M / 1e8,
            ),
        ),
        (
            "ai neurons today",
            human_num(total_ai_neurons_today),
            human_num(AI_NEURONS_PER_DAY),
            total_ai_neurons_today / AI_NEURONS_PER_DAY,
            ai_overage_usd,
        ),
        (
            "build mins",
            format!("{build_min:.1}"),
            format!("{}", LIMIT_BUILD_MIN as u64),
            build_min / LIMIT_BUILD_MIN,
            0.0, // build minutes overage not included here
        ),
    ];

    let label_w = items.iter().map(|i| i.0.len()).max().unwrap_or(0).max(18);
    let val_w = items
        .iter()
        .map(|i| i.1.chars().count() + 3 + i.2.chars().count())
        .max()
        .unwrap_or(0);

    for (label, used, limit, frac, overage_usd) in &items {
        let used_limit = format!("{used} / {limit}");
        let pad_label = " ".repeat(label_w.saturating_sub(label.len()));
        let pad_val = " ".repeat(val_w.saturating_sub(used_limit.chars().count()));
        let pct_str = format_pct(*frac);
        let overage_cell = if *overage_usd > 0.0 {
            sty.bold_color(&fmt_usd(*overage_usd), "31")
        } else {
            sty.dim("—")
        };
        println!(
            "  {}{}  {}{}  {}  {}  {}",
            label,
            pad_label,
            used_limit,
            pad_val,
            bar(sty, *frac, BAR_WIDTH),
            sty.bold_color(&pct_str, frac_color(*frac)),
            overage_cell,
        );
    }

    // Informational counters — not billable, no limit bar.
    let info_rows: Vec<(&str, String)> = vec![
        ("wall time", sty.dim(&human_ms(total_wall_ms))),
        ("resp bytes", sty.dim(&human_bytes(total_resp_bytes))),
        ("subrequests", sty.dim(&human_count(total_sub))),
        (
            "errors",
            if total_err > 0 {
                sty.red(&human_count(total_err))
            } else {
                sty.dim("0")
            },
        ),
        (
            "do errors",
            if total_do_err > 0 {
                sty.red(&human_count(total_do_err))
            } else {
                sty.dim("0")
            },
        ),
        (
            "do exceeded cpu",
            if total_do_exceeded_cpu > 0 {
                sty.red(&human_count(total_do_exceeded_cpu))
            } else {
                sty.dim("0")
            },
        ),
        (
            "do exceeded mem",
            if total_do_exceeded_mem > 0 {
                sty.red(&human_count(total_do_exceeded_mem))
            } else {
                sty.dim("0")
            },
        ),
        (
            "client disconnects",
            if total_disconnects > 0 {
                sty.dim(&human_count(total_disconnects))
            } else {
                sty.dim("0")
            },
        ),
        (
            "ai requests",
            sty.dim(&format!(
                "{} ({} today)",
                human_count(total_ai_requests),
                human_count(total_ai_requests_today),
            )),
        ),
        (
            "ai neurons",
            sty.dim(&format!(
                "{} ({} today)",
                human_num(total_ai_neurons),
                human_num(total_ai_neurons_today),
            )),
        ),
    ];
    for (label, val) in &info_rows {
        let pad = " ".repeat(label_w.saturating_sub(label.len()));
        println!("  {}{}  {}", label, pad, val);
    }
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

fn print_dos(sty: &Style, dos: &[DoRow], do_usage: &[DoUsageRow]) {
    println!("{}", sty.header("DURABLE OBJECTS (invocations)"));
    if dos.is_empty() && do_usage.is_empty() {
        println!("  {}", sty.dim("(no invocations in window)"));
        return;
    }
    let total_req: u64 = dos.iter().map(|d| d.requests).sum();
    let usage_by_script: BTreeMap<&str, &DoUsageRow> =
        do_usage.iter().map(|u| (u.script.as_str(), u)).collect();

    let cols = [
        Col { header: "script",    align: Align::Left },
        Col { header: "requests",  align: Align::Right },
        Col { header: "share",     align: Align::Right },
        Col { header: "duration",  align: Align::Right },
        Col { header: "rows read", align: Align::Right },
        Col { header: "rows wrt",  align: Align::Right },
        Col { header: "p50 (µs)",  align: Align::Right },
        Col { header: "p99 (µs)",  align: Align::Right },
        Col { header: "errors",    align: Align::Right },
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
            let usage = usage_by_script.get(d.script.as_str());
            let duration_cell = usage
                .map(|u| format!("{} GB-s", human_num(u.duration_gb_s)))
                .unwrap_or_else(|| sty.dim("—"));
            let rows_read_cell = usage
                .map(|u| human_count(u.rows_read))
                .unwrap_or_else(|| sty.dim("—"));
            let rows_written_cell = usage
                .map(|u| human_count(u.rows_written))
                .unwrap_or_else(|| sty.dim("—"));
            vec![
                d.script.clone(),
                human_count(d.requests),
                sty.dim(&format!("{:.1}%", share * 100.0)),
                duration_cell,
                rows_read_cell,
                rows_written_cell,
                d.p50.to_string(),
                d.p99.to_string(),
                err_cell,
            ]
        })
        .collect();

    print_table(sty, &cols, &rows);
}

fn print_vectorize(sty: &Style, vec_q: &[VecQueryRow], vec_s: &[VecStorageRow]) {
    println!("{}", sty.header("VECTORIZE (per-index)"));
    if vec_q.is_empty() && vec_s.is_empty() {
        println!("  {}", sty.dim("(no vectorize activity in window)"));
        return;
    }

    let storage_by_idx: BTreeMap<&str, &VecStorageRow> =
        vec_s.iter().map(|s| (s.index.as_str(), s)).collect();
    let query_by_idx: BTreeMap<&str, &VecQueryRow> =
        vec_q.iter().map(|q| (q.index.as_str(), q)).collect();

    let mut all_indexes: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    all_indexes.extend(vec_q.iter().map(|q| q.index.as_str()));
    all_indexes.extend(vec_s.iter().map(|s| s.index.as_str()));

    let cols = [
        Col { header: "index",        align: Align::Left },
        Col { header: "vectors",      align: Align::Right },
        Col { header: "stored dims",  align: Align::Right },
        Col { header: "queried dims", align: Align::Right },
        Col { header: "served",       align: Align::Right },
        Col { header: "query ms",     align: Align::Right },
    ];

    let rows: Vec<Vec<String>> = all_indexes
        .into_iter()
        .map(|idx| {
            let s = storage_by_idx.get(idx);
            let q = query_by_idx.get(idx);
            vec![
                idx.to_string(),
                s.map(|s| human_count(s.vector_count))
                    .unwrap_or_else(|| sty.dim("—")),
                s.map(|s| human_count(s.stored_dims))
                    .unwrap_or_else(|| sty.dim("—")),
                q.map(|q| human_count(q.queried_dims))
                    .unwrap_or_else(|| sty.dim("0")),
                q.map(|q| human_count(q.served))
                    .unwrap_or_else(|| sty.dim("0")),
                q.map(|q| human_count(q.duration_ms))
                    .unwrap_or_else(|| sty.dim("0")),
            ]
        })
        .collect();

    print_table(sty, &cols, &rows);
}

fn print_workers_ai(sty: &Style, ai: &[AiRow]) {
    println!("{}", sty.header("WORKERS AI (per-model)"));
    if ai.is_empty() {
        println!("  {}", sty.dim("(no Workers AI inference in window)"));
        return;
    }

    let total_req: u64 = ai.iter().map(|r| r.requests).sum();
    let total_neurons: f64 = ai.iter().map(|r| r.neurons).sum();

    let cols = [
        Col { header: "model",    align: Align::Left },
        Col { header: "requests", align: Align::Right },
        Col { header: "share",    align: Align::Right },
        Col { header: "neurons",  align: Align::Right },
        Col { header: "share",    align: Align::Right },
    ];

    let rows: Vec<Vec<String>> = ai
        .iter()
        .map(|r| {
            let req_share = frac(r.requests, total_req);
            let neu_share = if total_neurons > 0.0 {
                r.neurons / total_neurons
            } else {
                0.0
            };
            vec![
                r.model.clone(),
                human_count(r.requests),
                sty.dim(&format!("{:.1}%", req_share * 100.0)),
                human_num(r.neurons),
                sty.dim(&format!("{:.1}%", neu_share * 100.0)),
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

fn human_num(f: f64) -> String {
    if f.abs() >= 1e9 {
        format!("{:.2}B", f / 1e9)
    } else if f.abs() >= 1e6 {
        format!("{:.2}M", f / 1e6)
    } else if f.abs() >= 1e3 {
        format!("{:.2}k", f / 1e3)
    } else if f.abs() >= 10.0 {
        format!("{f:.1}")
    } else {
        format!("{f:.2}")
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
    // Decimal units to match Cloudflare's billing convention
    // (1 GB = 1,000,000,000 bytes).
    let mut f = n as f64;
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut i = 0;
    while f >= 1000.0 && i + 1 < units.len() {
        f /= 1000.0;
        i += 1;
    }
    format!("{:.1} {}", f, units[i])
}

pub(crate) fn fmt_usd(amount: f64) -> String {
    if amount >= 1000.0 {
        format!("${amount:.0}")
    } else if amount >= 10.0 {
        format!("${amount:.2}")
    } else if amount >= 0.01 {
        format!("${amount:.3}")
    } else {
        format!("${amount:.4}")
    }
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

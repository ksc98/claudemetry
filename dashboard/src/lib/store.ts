// Tiny client over the cc-proxy UserStore Durable Object.
// Each user_hash = one DO instance = one private SQLite.

export type TransactionRow = {
  tx_id: string;
  ts: number;
  session_id: string | null;
  model: string | null;
  method?: string | null;
  url?: string | null;
  status: number;
  elapsed_ms: number;
  input_tokens: number;
  output_tokens: number;
  cache_read: number;
  cache_creation: number;
  stop_reason: string | null;
  tools_json: string | null;
  req_body_bytes: number;
  resp_body_bytes: number;
  /** Short-TTL (5 min) cache writes — newer API only, may be null. */
  cache_creation_5m?: number | null;
  /** Long-TTL (1 hour) cache writes — newer API only, may be null. */
  cache_creation_1h?: number | null;
  /** Request-side thinking budget; non-null means extended thinking was enabled. */
  thinking_budget?: number | null;
  /** Count of `thinking` content blocks in the response. */
  thinking_blocks?: number | null;
  /** Request-side max_tokens ceiling. */
  max_tokens?: number | null;
  rl_req_remaining?: number | null;
  rl_req_limit?: number | null;
  rl_tok_remaining?: number | null;
  rl_tok_limit?: number | null;
};

export type Stats = {
  turns: number;
  input_tokens: number;
  output_tokens: number;
  cache_read: number;
  cache_creation: number;
  first_ts: number | null;
  last_ts: number | null;
};

function stubFor(ns: DurableObjectNamespace, userHash: string) {
  const id = ns.idFromName(userHash);
  return ns.get(id);
}

async function call<T>(
  ns: DurableObjectNamespace,
  userHash: string,
  path: string,
): Promise<T> {
  const stub = stubFor(ns, userHash);
  const res = await stub.fetch(`https://store${path}`);
  if (!res.ok) throw new Error(`DO ${path} ${res.status}`);
  return (await res.json()) as T;
}

export function getStats(ns: DurableObjectNamespace, userHash: string) {
  return call<Stats>(ns, userHash, "/stats");
}

export function getRecent(ns: DurableObjectNamespace, userHash: string) {
  return call<TransactionRow[]>(ns, userHash, "/recent");
}

export type SessionEnds = Record<string, number>;

export function getSessionEnds(ns: DurableObjectNamespace, userHash: string) {
  return call<SessionEnds>(ns, userHash, "/session/ends");
}

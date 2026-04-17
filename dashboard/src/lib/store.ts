// Tiny client over the burnage-api UserStore Durable Object.
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
  /** Only present on /turn (detail fetch); list endpoints omit it. */
  tools_json?: string | null;
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
  /** Compact tool_choice from the request: "auto", "any", or "tool:<name>". In-flight only. */
  tool_choice?: string | null;
  /** 1 while the proxy is still waiting on upstream (spinner state); 0/null once finalized. */
  in_flight?: number | null;
  /** Anthropic's `message.id` (set on finalize). Row PK `tx_id` is a stable synthetic id. */
  anthropic_message_id?: string | null;
  /** Only present on /turn (detail fetch); list endpoints omit it. */
  user_text?: string | null;
  /** Only present on /turn (detail fetch); list endpoints omit it. */
  assistant_text?: string | null;
  /** 1 if the response produced a non-empty `text` content block (vs pure
   * tool_use). Set on list endpoints and WS turn_complete so the table can
   * flag turns that actually replied without shipping the body. */
  has_text?: number | null;
};

export type Stats = {
  turns: number;
  input_tokens: number;
  output_tokens: number;
  cache_read: number;
  cache_creation: number;
  first_ts: number | null;
  last_ts: number | null;
  sessions: number;
  total_elapsed_ms: number;
  cache_creation_5m: number;
  cache_creation_1h: number;
  estimated_cost_usd: number;
};

/** One row per (session_id, model) returned by /sessions/summary. */
export type SessionModelBucket = {
  session_id: string;
  model: string | null;
  turns: number;
  first_ts: number;
  last_ts: number;
  input_tokens: number;
  output_tokens: number;
  cache_read: number;
  cache_creation: number;
};

function stubFor(ns: DurableObjectNamespace, userHash: string) {
  const id = ns.idFromName(userHash);
  return ns.get(id);
}

async function callGet<T>(
  ns: DurableObjectNamespace,
  userHash: string,
  path: string,
  query?: Record<string, string | number | undefined>,
): Promise<T> {
  const stub = stubFor(ns, userHash);
  let url = `https://store${path}`;
  if (query) {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v != null) qs.set(k, String(v));
    }
    const s = qs.toString();
    if (s) url += `?${s}`;
  }
  const res = await stub.fetch(url);
  if (!res.ok) throw new Error(`DO ${path} ${res.status}`);
  return (await res.json()) as T;
}

async function callPost<T>(
  ns: DurableObjectNamespace,
  userHash: string,
  path: string,
  body: unknown,
): Promise<T> {
  const stub = stubFor(ns, userHash);
  const res = await stub.fetch(`https://store${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`DO ${path} ${res.status}`);
  return (await res.json()) as T;
}

export function getStats(
  ns: DurableObjectNamespace,
  userHash: string,
  since?: number,
) {
  return callGet<Stats>(ns, userHash, "/stats", { since });
}

export function getRecent(
  ns: DurableObjectNamespace,
  userHash: string,
  since?: number,
) {
  return callGet<TransactionRow[]>(ns, userHash, "/recent", { since });
}

export type SessionEnds = Record<string, number>;

export function getSessionEnds(ns: DurableObjectNamespace, userHash: string) {
  return callGet<SessionEnds>(ns, userHash, "/session/ends");
}

export function getSessionsSummary(
  ns: DurableObjectNamespace,
  userHash: string,
) {
  return callGet<SessionModelBucket[]>(ns, userHash, "/sessions/summary");
}

export function getSessionTurns(
  ns: DurableObjectNamespace,
  userHash: string,
  sessionId: string,
  limit?: number,
) {
  return callGet<TransactionRow[]>(ns, userHash, "/session/turns", {
    id: sessionId,
    limit,
  });
}

export function getTurn(
  ns: DurableObjectNamespace,
  userHash: string,
  txId: string,
) {
  return callPost<TransactionRow>(ns, userHash, "/turn", { tx_id: txId });
}

export interface InFlightTurn {
  tx_id: string;
  session_id: string | null;
  ts: number;
  model: string | null;
  tool_choice: string | null;
  thinking_budget: number | null;
  max_tokens: number | null;
}

export function getInFlight(
  ns: DurableObjectNamespace,
  userHash: string,
  sessionId?: string,
) {
  return callGet<InFlightTurn[]>(ns, userHash, "/in_flight", {
    session_id: sessionId,
  });
}

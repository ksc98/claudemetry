import type { SessionEnds, SessionModelBucket } from "@/lib/store";
import { estimateCostUsd } from "@/lib/format";

// A session is "active" if its most recent turn lands within this window
// AND no SessionEnd marker has been recorded at-or-after that last turn.
export const ACTIVE_WINDOW_MS = 3 * 60_000;

export type SessionSummary = {
  id: string;
  turns: number;
  costUsd: number;
  firstTs: number;
  lastTs: number;
  active: boolean;
  topModel: string | null;
  modelCount: number;
  /** Per-model turn counts (short names), for the table's mix-bar swatches. */
  models: Array<{ model: string; turns: number }>;
};

const shortModel = (m: string): string =>
  m.replace(/-\d{8}$/, "").replace(/^claude-/, "");

// Build the session list directly from the DO's /sessions/summary aggregate.
// Cost is summed per (session, model) bucket via the client-side pricing table.
// `inFlightSessionIds` (optional) marks sessions with a request currently in
// flight as active even if their last completed turn fell outside the
// activity window — fixes the "no spinner on refresh" gap when a slow turn
// runs longer than ACTIVE_WINDOW_MS.
export function buildSessionListFromSummary(
  buckets: SessionModelBucket[],
  sessionEnds: SessionEnds,
  inFlightSessionIds?: ReadonlySet<string>,
): SessionSummary[] {
  type Agg = {
    id: string;
    turns: number;
    firstTs: number;
    lastTs: number;
    cost: number;
    topModel: string | null;
    topModelTurns: number;
    models: Map<string, number>;
  };
  const map = new Map<string, Agg>();
  for (const b of buckets) {
    const cur =
      map.get(b.session_id) ??
      ({
        id: b.session_id,
        turns: 0,
        firstTs: b.first_ts,
        lastTs: b.last_ts,
        cost: 0,
        topModel: null,
        topModelTurns: 0,
        models: new Map<string, number>(),
      } satisfies Agg);
    cur.turns += b.turns;
    cur.firstTs = Math.min(cur.firstTs, b.first_ts);
    cur.lastTs = Math.max(cur.lastTs, b.last_ts);
    cur.cost += estimateCostUsd({
      model: b.model,
      input_tokens: b.input_tokens,
      output_tokens: b.output_tokens,
      cache_read: b.cache_read,
      cache_creation: b.cache_creation,
    });
    if (b.model) {
      const short = shortModel(b.model);
      cur.models.set(short, (cur.models.get(short) ?? 0) + b.turns);
      if (b.turns > cur.topModelTurns) {
        cur.topModel = short;
        cur.topModelTurns = b.turns;
      }
    }
    map.set(b.session_id, cur);
  }

  const now = Date.now();
  const isActive = (s: Agg): boolean => {
    if (inFlightSessionIds?.has(s.id)) return true;
    if (now - s.lastTs >= ACTIVE_WINDOW_MS) return false;
    const endedAt = sessionEnds[s.id];
    return endedAt == null || endedAt < s.lastTs;
  };

  return [...map.values()]
    .map((agg) => ({
      id: agg.id,
      turns: agg.turns,
      costUsd: agg.cost,
      firstTs: agg.firstTs,
      lastTs: agg.lastTs,
      active: isActive(agg),
      topModel: agg.topModel,
      modelCount: agg.models.size,
      models: [...agg.models.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([model, turns]) => ({ model, turns })),
    }))
    .sort((a, b) => {
      const aa = a.active ? 1 : 0;
      const ba = b.active ? 1 : 0;
      if (aa !== ba) return ba - aa;
      return b.lastTs - a.lastTs;
    });
}

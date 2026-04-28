import { useEffect, useMemo, useRef, useState } from "react";
import type { TransactionRow } from "@/lib/store";
import { estimateCostUsd } from "@/lib/format";
import { subscribeRows } from "@/lib/rowsBus";
import {
  DEFAULT_WINDOW,
  windowIsBucketed,
  windowIsShort,
  windowMs,
  type Window,
} from "@/lib/pillWindow";
import { useHydrated } from "@/hooks/use-hydrated";
import TokenAreaChart, { type TokenAreaPoint } from "./charts/TokenAreaChart";

type Point = TokenAreaPoint & { ts: number };

// Insert an explicit null point whenever consecutive turns are far enough
// apart in time that the gap likely represents "the user wasn't using
// Claude". Without this, the cache_read series — which is non-zero on
// every turn — draws a continuous line across the gap, masking the time
// of actual usage. Null breaks all series consistently.
function withTimeGaps(points: Point[], bucketMs: number): Point[] {
  if (points.length < 2) return points;

  let threshold: number;
  if (bucketMs > 0) {
    // Bucketed: any missing bucket is a gap.
    threshold = bucketMs * 1.5;
  } else {
    // Per-turn: gap = much-bigger-than-typical inter-turn delta.
    const deltas: number[] = [];
    for (let i = 1; i < points.length; i++) {
      deltas.push(points[i].ts - points[i - 1].ts);
    }
    deltas.sort((a, b) => a - b);
    const median = deltas[Math.floor(deltas.length / 2)];
    threshold = Math.max(60_000, median * 8);
  }

  const out: Point[] = [];
  for (let i = 0; i < points.length; i++) {
    out.push(points[i]);
    const next = points[i + 1];
    if (next && next.ts - points[i].ts > threshold) {
      out.push({
        ts: points[i].ts + (next.ts - points[i].ts) / 2,
        input: null,
        output: null,
        cache_read: null,
        cache_creation: null,
        cost: null,
      });
    }
  }
  return out;
}

// Bucket sizes per window. Tokens are averaged per turn; cost is summed.
// Only long windows (> 24h) bucket — shorter windows render individual turns.
function bucketMsFor(win: Window): number {
  return windowIsBucketed(win) ? 5 * 60_000 : 0;
}

function rowsToPoints(rows: TransactionRow[], win: Window): Point[] {
  const filtered = [...rows]
    .filter((r) => r.output_tokens > 0 || r.input_tokens > 50)
    .sort((a, b) => a.ts - b.ts);

  if (filtered.length === 0) return [];

  const bucketMs = bucketMsFor(win);

  // Zero → null so the chart draws a gap at that point instead of a line
  // hugging the baseline. Honest representation of "no data for this
  // series this turn," and required for log scale (log(0) is undefined).
  const gap = (n: number) => (n > 0 ? n : null);

  // Small windows: individual turns.
  if (bucketMs === 0) {
    return filtered.map((r) => ({
      ts: r.ts,
      input: gap(r.input_tokens),
      output: gap(r.output_tokens),
      cache_read: gap(r.cache_read),
      cache_creation: gap(r.cache_creation),
      cost: estimateCostUsd(r),
    }));
  }

  // Bucket: average tokens per turn, sum cost.
  const firstTs = filtered[0].ts;
  type Acc = {
    ts: number;
    input: number;
    output: number;
    cache_read: number;
    cache_creation: number;
    cost: number;
    n: number;
  };
  const buckets = new Map<number, Acc>();

  for (const r of filtered) {
    const key = firstTs + Math.floor((r.ts - firstTs) / bucketMs) * bucketMs;
    const existing = buckets.get(key);
    if (existing) {
      existing.input += r.input_tokens;
      existing.output += r.output_tokens;
      existing.cache_read += r.cache_read;
      existing.cache_creation += r.cache_creation;
      existing.cost += estimateCostUsd(r);
      existing.n += 1;
    } else {
      buckets.set(key, {
        ts: key + bucketMs / 2,
        input: r.input_tokens,
        output: r.output_tokens,
        cache_read: r.cache_read,
        cache_creation: r.cache_creation,
        cost: estimateCostUsd(r),
        n: 1,
      });
    }
  }

  return [...buckets.values()]
    .sort((a, b) => a.ts - b.ts)
    .map((p) => ({
      ts: p.ts,
      input: gap(Math.round(p.input / p.n)),
      output: gap(Math.round(p.output / p.n)),
      cache_read: gap(Math.round(p.cache_read / p.n)),
      cache_creation: gap(Math.round(p.cache_creation / p.n)),
      cost: p.cost,
    }));
}

function fmtTs(ms: number, win: Window): string {
  const d = new Date(ms);
  const h = String(d.getHours()).padStart(2, "0");
  const m = String(d.getMinutes()).padStart(2, "0");
  if (windowIsShort(win)) return `${h}:${m}`;
  // Longer windows: show date
  const mon = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${mon}/${day} ${h}:${m}`;
}

export default function TokenChart({
  initialRows,
  window: win = DEFAULT_WINDOW,
}: {
  initialRows: TransactionRow[];
  window?: Window;
}) {
  const [rows, setRows] = useState<TransactionRow[]>(initialRows);
  const baselineRef = useRef(initialRows);

  // Merge WS-pushed rows into the SSR baseline so the chart keeps showing
  // the full pill-windowed dataset instead of only live rows.
  useEffect(
    () =>
      subscribeRows((busRows) => {
        const byId = new Map<string, TransactionRow>();
        for (const r of baselineRef.current) byId.set(r.tx_id, r);
        for (const r of busRows) byId.set(r.tx_id, r);
        setRows([...byId.values()]);
      }),
    [],
  );

  const data = useMemo(
    () => withTimeGaps(rowsToPoints(rows, win), bucketMsFor(win)),
    [rows, win],
  );

  // Enable the brush on wide windows where point counts are high and
  // scrubbing becomes useful. Bucketed windows still produce hundreds of
  // points even after bucketing, so the brush is worthwhile there.
  const showBrush = windowIsBucketed(win);

  // fmtTs uses the user's local timezone (Date.prototype.getHours), but
  // Cloudflare Workers SSR runs in UTC. Rendering the chart on SSR would
  // emit UTC tick labels that don't match the client's first paint, which
  // trips React #418. Defer rendering until after hydration so SSR + first
  // hydration paint both emit nothing, then the real chart pops in.
  const hydrated = useHydrated();
  if (!hydrated || data.length === 0) return null;

  // Anchor the X-axis to the active window so the right edge is "now",
  // not the last data point. Without this, a long stretch of inactivity
  // collapses out of the chart and the user can't tell whether the most
  // recent activity was 5 minutes ago or 5 hours ago.
  const now = Date.now();
  const xDomain: [number, number] = [now - windowMs(win), now];

  return (
    <TokenAreaChart
      data={data}
      xKey="ts"
      xDomain={xDomain}
      xTickFormatter={(v) => fmtTs(v, win)}
      xLabelFormatter={(v) => fmtTs(v as number, win)}
      yScale="log"
      linearThreshold={100_000}
      linearTickStep={100_000}
      instanceId="tokenChart"
      showBrush={showBrush}
    />
  );
}

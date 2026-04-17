import { useEffect, useMemo, useRef, useState } from "react";
import type { TransactionRow } from "@/lib/store";
import { estimateCostUsd } from "@/lib/format";
import { subscribeRows } from "@/lib/rowsBus";
import type { Window } from "@/lib/pillWindow";
import TokenAreaChart, { type TokenAreaPoint } from "./charts/TokenAreaChart";

type Point = TokenAreaPoint & { ts: number };

// Bucket sizes per window. Tokens are averaged per turn; cost is summed.
const BUCKET_MS: Record<Window, number> = {
  "15m": 0, // individual turns
  "1h": 0,
  "24h": 0, // individual turns
  "3d": 5 * 60_000, // 5 min → ~864 buckets
  "7d": 5 * 60_000, // 5 min → ~2016 buckets
};

function rowsToPoints(rows: TransactionRow[], win: Window): Point[] {
  const filtered = [...rows]
    .filter((r) => r.output_tokens > 0 || r.input_tokens > 50)
    .sort((a, b) => a.ts - b.ts);

  if (filtered.length === 0) return [];

  const bucketMs = BUCKET_MS[win];

  // Small windows: individual turns.
  if (bucketMs === 0) {
    return filtered.map((r) => ({
      ts: r.ts,
      input: Math.max(r.input_tokens, 1),
      output: Math.max(r.output_tokens, 1),
      cache_read: Math.max(r.cache_read, 1),
      cache_creation: Math.max(r.cache_creation, 1),
      cost: estimateCostUsd(r),
    }));
  }

  // Bucket: average tokens per turn, sum cost.
  const firstTs = filtered[0].ts;
  type Acc = Point & { n: number };
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
      input: Math.max(Math.round(p.input / p.n), 1),
      output: Math.max(Math.round(p.output / p.n), 1),
      cache_read: Math.max(Math.round(p.cache_read / p.n), 1),
      cache_creation: Math.max(Math.round(p.cache_creation / p.n), 1),
      cost: p.cost,
    }));
}

function fmtTs(ms: number, win: Window): string {
  const d = new Date(ms);
  const h = String(d.getHours()).padStart(2, "0");
  const m = String(d.getMinutes()).padStart(2, "0");
  if (win === "15m" || win === "1h") return `${h}:${m}`;
  // Longer windows: show date
  const mon = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${mon}/${day} ${h}:${m}`;
}

export default function TokenChart({
  initialRows,
  window: win = "1h",
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

  const data = useMemo(() => rowsToPoints(rows, win), [rows, win]);

  // Enable the brush on wide windows where point counts are high and
  // scrubbing becomes useful. 3d/7d use 5-min buckets but still produce
  // hundreds of points.
  const showBrush = win === "3d" || win === "7d";

  if (data.length === 0) return null;
  return (
    <TokenAreaChart
      data={data}
      xKey="ts"
      xTickFormatter={(v) => fmtTs(v, win)}
      xLabelFormatter={(v) => fmtTs(v as number, win)}
      yScale="log"
      instanceId="tokenChart"
      showBrush={showBrush}
    />
  );
}

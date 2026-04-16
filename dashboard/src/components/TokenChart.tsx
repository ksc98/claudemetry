import { useEffect, useMemo, useRef, useState } from "react";
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { TransactionRow } from "@/lib/store";
import { estimateCostUsd } from "@/lib/format";
import { subscribeRows } from "@/lib/rowsBus";
import type { Window } from "@/lib/pillWindow";

type Point = {
  ts: number;
  input: number;
  output: number;
  cache_read: number;
  cache_creation: number;
  cost: number;
};

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

function fmtTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return `${n}`;
}

function fmtUsd(n: number): string {
  if (n === 0) return "$0";
  if (n < 0.01) return `$${n.toFixed(4)}`;
  return `$${n.toFixed(2)}`;
}

export default function TokenChart({
  initialRows,
  window: win = "1h",
}: {
  initialRows: TransactionRow[];
  window?: Window;
}) {
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState<number>(0);
  const [rows, setRows] = useState<TransactionRow[]>(initialRows);
  const baselineRef = useRef(initialRows);
  const height = 280;

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width ?? 0;
      if (w > 0) setWidth(Math.floor(w));
    });
    ro.observe(el);
    const initial = el.getBoundingClientRect().width;
    if (initial > 0) setWidth(Math.floor(initial));
    return () => ro.disconnect();
  }, []);

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

  if (data.length === 0) return null;
  return (
    <div ref={wrapRef} style={{ width: "100%", height }}>
      {width > 0 && (
        <ComposedChart
          width={width}
          height={height}
          data={data}
          margin={{ top: 12, right: 48, bottom: 4, left: 4 }}
        >
          <defs>
            <linearGradient id="fillCacheRead" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="var(--color-chart-1)" stopOpacity={0.35} />
              <stop offset="95%" stopColor="var(--color-chart-1)" stopOpacity={0.05} />
            </linearGradient>
            <linearGradient id="fillCacheCreation" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="var(--color-chart-2)" stopOpacity={0.35} />
              <stop offset="95%" stopColor="var(--color-chart-2)" stopOpacity={0.05} />
            </linearGradient>
            <linearGradient id="fillOutput" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="var(--color-chart-4)" stopOpacity={0.45} />
              <stop offset="95%" stopColor="var(--color-chart-4)" stopOpacity={0.08} />
            </linearGradient>
            <linearGradient id="fillInput" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="var(--color-chart-5)" stopOpacity={0.5} />
              <stop offset="95%" stopColor="var(--color-chart-5)" stopOpacity={0.1} />
            </linearGradient>
          </defs>
          <CartesianGrid
            stroke="var(--color-border)"
            strokeDasharray="2 4"
            vertical={false}
          />
          <XAxis
            dataKey="ts"
            tickFormatter={(v) => fmtTs(v as number, win)}
            axisLine={false}
            tickLine={false}
            tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
            minTickGap={60}
            interval="preserveStartEnd"
          />
          <YAxis
            yAxisId="tokens"
            scale="log"
            domain={[1, "dataMax"]}
            tickFormatter={fmtTokens}
            axisLine={false}
            tickLine={false}
            tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
            width={40}
            allowDataOverflow={false}
          />
          <YAxis
            yAxisId="cost"
            orientation="right"
            tickFormatter={fmtUsd}
            axisLine={false}
            tickLine={false}
            tick={{ fill: "var(--color-money)", fontSize: 11 }}
            width={44}
          />
          <Tooltip
            contentStyle={{
              background: "var(--color-card-elevated)",
              border: "1px solid var(--color-border-strong)",
              borderRadius: 8,
              fontSize: 12,
            }}
            labelFormatter={(v) => fmtTs(v as number, win)}
            formatter={(value, name) => {
              if (name === "cost") return [fmtUsd(value as number), "cost"];
              return [fmtTokens(value as number), name];
            }}
          />
          {/* Independent (not stacked) overlapping areas — stacking on log
              scale is mathematically invalid, so each series gets its own
              alpha-fill layer. Order matters for visual stacking: largest
              series first so smaller ones remain visible on top. */}
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="cache_read"
            stroke="var(--color-chart-1)"
            strokeWidth={1.25}
            fill="url(#fillCacheRead)"
            name="cache_read"
            isAnimationActive={false}
          />
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="cache_creation"
            stroke="var(--color-chart-2)"
            strokeWidth={1.25}
            fill="url(#fillCacheCreation)"
            name="cache_creation"
            isAnimationActive={false}
          />
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="output"
            stroke="var(--color-chart-4)"
            strokeWidth={1.25}
            fill="url(#fillOutput)"
            name="output"
            isAnimationActive={false}
          />
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="input"
            stroke="var(--color-chart-5)"
            strokeWidth={1.25}
            fill="url(#fillInput)"
            name="input"
            isAnimationActive={false}
          />
          <Line
            yAxisId="cost"
            type="monotone"
            dataKey="cost"
            stroke="var(--color-money)"
            strokeWidth={1.75}
            dot={false}
            name="cost"
            isAnimationActive={false}
          />
        </ComposedChart>
      )}
    </div>
  );
}

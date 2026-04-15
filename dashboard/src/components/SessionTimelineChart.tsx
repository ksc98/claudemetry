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

type Point = {
  turn: number;
  ts: number;
  input: number;
  output: number;
  cache_read: number;
  cache_creation: number;
  cost: number;
};

function rowsToPoints(rows: TransactionRow[], sessionId: string): Point[] {
  return rows
    .filter((r) => r.session_id === sessionId)
    .sort((a, b) => a.ts - b.ts)
    .map((r, i) => ({
      turn: i + 1,
      ts: r.ts,
      input: r.input_tokens,
      output: r.output_tokens,
      cache_read: r.cache_read,
      cache_creation: r.cache_creation,
      cost: estimateCostUsd(r),
    }));
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

export default function SessionTimelineChart({
  initialRows,
  sessionId,
}: {
  initialRows: TransactionRow[];
  sessionId: string;
}) {
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState<number>(0);
  const [rows, setRows] = useState<TransactionRow[]>(initialRows);
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

  useEffect(() => subscribeRows(setRows), []);

  const data = useMemo(() => rowsToPoints(rows, sessionId), [rows, sessionId]);

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
            <linearGradient id="sfillCacheRead" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="var(--color-chart-1)" stopOpacity={0.35} />
              <stop offset="95%" stopColor="var(--color-chart-1)" stopOpacity={0.05} />
            </linearGradient>
            <linearGradient id="sfillCacheCreation" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="var(--color-chart-2)" stopOpacity={0.35} />
              <stop offset="95%" stopColor="var(--color-chart-2)" stopOpacity={0.05} />
            </linearGradient>
            <linearGradient id="sfillOutput" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="var(--color-chart-4)" stopOpacity={0.45} />
              <stop offset="95%" stopColor="var(--color-chart-4)" stopOpacity={0.08} />
            </linearGradient>
            <linearGradient id="sfillInput" x1="0" y1="0" x2="0" y2="1">
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
            dataKey="turn"
            axisLine={false}
            tickLine={false}
            tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
            tickFormatter={(v) => `#${v}`}
            minTickGap={20}
            interval="preserveStartEnd"
          />
          <YAxis
            yAxisId="tokens"
            tickFormatter={fmtTokens}
            axisLine={false}
            tickLine={false}
            tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
            width={44}
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
            labelFormatter={(v) => `Turn #${v}`}
            formatter={(value, name) => {
              if (name === "cost") return [fmtUsd(value as number), "cost"];
              return [fmtTokens(value as number), name];
            }}
          />
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="cache_read"
            stroke="var(--color-chart-1)"
            strokeWidth={1.25}
            fill="url(#sfillCacheRead)"
            name="cache_read"
            isAnimationActive={false}
          />
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="cache_creation"
            stroke="var(--color-chart-2)"
            strokeWidth={1.25}
            fill="url(#sfillCacheCreation)"
            name="cache_creation"
            isAnimationActive={false}
          />
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="output"
            stroke="var(--color-chart-4)"
            strokeWidth={1.25}
            fill="url(#sfillOutput)"
            name="output"
            isAnimationActive={false}
          />
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="input"
            stroke="var(--color-chart-5)"
            strokeWidth={1.25}
            fill="url(#sfillInput)"
            name="input"
            isAnimationActive={false}
          />
          <Line
            yAxisId="cost"
            type="monotone"
            dataKey="cost"
            stroke="var(--color-money)"
            strokeWidth={1.75}
            dot={{ r: 2, fill: "var(--color-money)" }}
            name="cost"
            isAnimationActive={false}
          />
        </ComposedChart>
      )}
    </div>
  );
}

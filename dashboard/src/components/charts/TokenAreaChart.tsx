import { useEffect, useMemo, useRef, useState } from "react";
import {
  Area,
  Brush,
  CartesianGrid,
  ComposedChart,
  Line,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import ChartTooltipCard from "./ChartTooltipCard";

export type TokenAreaPoint = {
  input: number;
  output: number;
  cache_read: number;
  cache_creation: number;
  cost: number;
  // xKey is dynamic (either "ts" or "turn")
  [k: string]: number;
};

interface Props {
  data: TokenAreaPoint[];
  xKey: "ts" | "turn";
  xTickFormatter: (v: number) => string;
  xLabelFormatter?: (v: unknown) => string;
  yScale: "log" | "linear";
  /** Unique prefix for SVG <defs> ids (avoids collisions when >1 chart on page). */
  instanceId: string;
  /** Enable recharts Brush slider beneath the chart. */
  showBrush?: boolean;
  /** Draw points on the cost line (useful for per-turn charts). */
  showCostDots?: boolean;
  height?: number;
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

export default function TokenAreaChart({
  data,
  xKey,
  xTickFormatter,
  xLabelFormatter,
  yScale,
  instanceId,
  showBrush = false,
  showCostDots = false,
  height = 280,
}: Props) {
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [width, setWidth] = useState<number>(0);
  const [mounted, setMounted] = useState(false);

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
    // Trigger mount fade-in after first measurable paint.
    requestAnimationFrame(() => setMounted(true));
    return () => ro.disconnect();
  }, []);

  const gradients = useMemo(
    () => [
      {
        id: `${instanceId}-cacheRead`,
        color: "var(--color-chart-1)",
        top: 0.35,
        bottom: 0.05,
      },
      {
        id: `${instanceId}-cacheCreation`,
        color: "var(--color-chart-2)",
        top: 0.35,
        bottom: 0.05,
      },
      {
        id: `${instanceId}-output`,
        color: "var(--color-chart-4)",
        top: 0.45,
        bottom: 0.08,
      },
      {
        id: `${instanceId}-input`,
        color: "var(--color-chart-5)",
        top: 0.5,
        bottom: 0.1,
      },
    ],
    [instanceId],
  );

  return (
    <div
      ref={wrapRef}
      style={{
        width: "100%",
        height: height + (showBrush ? 36 : 0),
        opacity: mounted ? 1 : 0,
        transform: mounted ? "translateY(0)" : "translateY(4px)",
        transition: "opacity 280ms ease, transform 280ms ease",
      }}
    >
      {width > 0 && (
        <ComposedChart
          width={width}
          height={height + (showBrush ? 36 : 0)}
          data={data}
          margin={{ top: 12, right: 48, bottom: showBrush ? 8 : 4, left: 4 }}
        >
          <defs>
            {gradients.map((g) => (
              <linearGradient
                key={g.id}
                id={g.id}
                x1="0"
                y1="0"
                x2="0"
                y2="1"
              >
                <stop offset="5%" stopColor={g.color} stopOpacity={g.top} />
                <stop offset="95%" stopColor={g.color} stopOpacity={g.bottom} />
              </linearGradient>
            ))}
          </defs>
          <CartesianGrid
            stroke="var(--color-border)"
            strokeDasharray="2 4"
            vertical={false}
          />
          <XAxis
            dataKey={xKey}
            tickFormatter={(v) => xTickFormatter(v as number)}
            axisLine={false}
            tickLine={false}
            tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
            minTickGap={xKey === "turn" ? 20 : 60}
            interval="preserveStartEnd"
          />
          <YAxis
            yAxisId="tokens"
            scale={yScale}
            domain={yScale === "log" ? [1, "dataMax"] : undefined}
            tickFormatter={fmtTokens}
            axisLine={false}
            tickLine={false}
            tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
            width={yScale === "log" ? 40 : 44}
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
            cursor={{
              stroke: "var(--color-border-strong)",
              strokeDasharray: "2 4",
              strokeWidth: 1,
            }}
            content={(props) => (
              <ChartTooltipCard
                {...(props as unknown as Parameters<typeof ChartTooltipCard>[0])}
                labelFormatter={xLabelFormatter}
              />
            )}
          />
          {/* Render order: largest series first so smaller ones stay visible on top.
              Areas are independent (not stacked) — log scale makes stacking invalid
              and linear scale looks busy. Each series gets its own gradient fill. */}
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="cache_read"
            stroke="var(--color-chart-1)"
            strokeWidth={1.25}
            fill={`url(#${instanceId}-cacheRead)`}
            name="cache_read"
            isAnimationActive={false}
          />
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="cache_creation"
            stroke="var(--color-chart-2)"
            strokeWidth={1.25}
            fill={`url(#${instanceId}-cacheCreation)`}
            name="cache_creation"
            isAnimationActive={false}
          />
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="output"
            stroke="var(--color-chart-4)"
            strokeWidth={1.25}
            fill={`url(#${instanceId}-output)`}
            name="output"
            isAnimationActive={false}
          />
          <Area
            yAxisId="tokens"
            type="monotone"
            dataKey="input"
            stroke="var(--color-chart-5)"
            strokeWidth={1.25}
            fill={`url(#${instanceId}-input)`}
            name="input"
            isAnimationActive={false}
          />
          <Line
            yAxisId="cost"
            type="monotone"
            dataKey="cost"
            stroke="var(--color-money)"
            strokeWidth={1.75}
            dot={showCostDots ? { r: 2, fill: "var(--color-money)" } : false}
            name="cost"
            isAnimationActive={false}
          />
          {showBrush && (
            <Brush
              dataKey={xKey}
              height={22}
              travellerWidth={8}
              stroke="var(--color-border-strong)"
              fill="var(--color-card)"
              tickFormatter={(v) => xTickFormatter(v as number)}
            />
          )}
        </ComposedChart>
      )}
    </div>
  );
}

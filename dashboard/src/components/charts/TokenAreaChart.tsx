import { useEffect, useMemo, useState } from "react";
import {
  Area,
  Brush,
  CartesianGrid,
  ComposedChart,
  Line,
  XAxis,
  YAxis,
} from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";

export type TokenAreaPoint = {
  // Token series use `number | null`; null renders as a gap in the area
  // (the only honest representation of "this turn had no input/output of
  // this kind" on a log scale, where 0 isn't a real coordinate).
  input: number | null;
  output: number | null;
  cache_read: number | null;
  cache_creation: number | null;
  cost: number;
  // xKey is dynamic (either "ts" or "turn")
  [k: string]: number | null;
};

interface Props {
  data: TokenAreaPoint[];
  xKey: "ts" | "turn";
  xTickFormatter: (v: number) => string;
  xLabelFormatter?: (v: unknown) => string;
  yScale: "log" | "linear";
  /** For linear scale: force ticks at this step (e.g. 10_000). Ignored on log. */
  linearTickStep?: number;
  /** Log scale only: switch to linear ticks of `linearTickStep` at/above this
   * value (e.g. 200_000). Below the threshold, standard log decades apply. */
  linearThreshold?: number;
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

// Snap the log-axis top to a 1/2/5 × 10^n "nice" value just above the data
// max. Without this, rounding to the next decade (e.g. 428k → 1M) wastes
// ~70% of the top as empty space. The 1/2/5 progression is the standard
// "nice number" sequence for axis scales.
function logCeiling(max: number): number {
  if (!Number.isFinite(max) || max <= 1) return 10;
  const magnitude = Math.pow(10, Math.floor(Math.log10(max)));
  const ratio = max / magnitude;
  if (ratio < 2) return magnitude * 2;
  if (ratio < 5) return magnitude * 5;
  return magnitude * 10;
}

function logTicks(ceiling: number): number[] {
  const ticks: number[] = [];
  for (let v = 1; v < ceiling; v *= 10) ticks.push(v);
  if (ticks[ticks.length - 1] !== ceiling) ticks.push(ceiling);
  return ticks;
}

// Log scale with linear top: decade ticks up to `linearThreshold`, then the
// given linear step above. The visual is still log (low values stay
// readable), but the upper region gains 100k/200k/… reference gridlines
// instead of only the decade jump. Used when tokens routinely span 5+
// orders of magnitude but the "interesting" zone sits above 100k.
function hybridLogLinearTicks(
  max: number,
  linearThreshold: number,
  linearStep: number,
): { ticks: number[]; ceiling: number } {
  if (!Number.isFinite(max) || max <= 1) {
    return { ticks: [1, 10], ceiling: 10 };
  }
  const ticks: number[] = [];
  for (let v = 1; v < linearThreshold; v *= 10) ticks.push(v);
  ticks.push(linearThreshold);
  if (max <= linearThreshold) {
    return { ticks, ceiling: linearThreshold };
  }
  const ceiling = Math.ceil(max / linearStep) * linearStep;
  for (let v = linearThreshold + linearStep; v <= ceiling; v += linearStep) {
    ticks.push(v);
  }
  return { ticks, ceiling };
}

// Linear ticks at a fixed step up to the smallest multiple ≥ max.
function linearTicksAtStep(max: number, step: number): number[] {
  if (!Number.isFinite(max) || max <= 0) return [0, step];
  const ceiling = Math.ceil(max / step) * step;
  const ticks: number[] = [];
  for (let v = 0; v <= ceiling; v += step) ticks.push(v);
  return ticks;
}

const TOKEN_KEYS: readonly (keyof TokenAreaPoint)[] = [
  "cache_read",
  "cache_creation",
  "output",
  "input",
];

// Each series's label + color. CSS vars are resolved inside ChartContainer so
// the tooltip swatches and axis strokes all share one source of truth.
const chartConfig = {
  cache_read: {
    label: "cache read",
    color: "var(--color-chart-1)",
  },
  cache_creation: {
    label: "cache create",
    color: "var(--color-chart-2)",
  },
  output: {
    label: "output",
    color: "var(--color-chart-4)",
  },
  input: {
    label: "input",
    color: "var(--color-chart-5)",
  },
  cost: {
    label: "cost",
    color: "var(--color-money)",
  },
} satisfies ChartConfig;

export default function TokenAreaChart({
  data,
  xKey,
  xTickFormatter,
  xLabelFormatter,
  yScale,
  linearTickStep,
  linearThreshold,
  instanceId,
  showBrush = false,
  showCostDots = false,
  height = 280,
}: Props) {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    requestAnimationFrame(() => setMounted(true));
  }, []);

  const gradients = useMemo(
    () => [
      { id: `${instanceId}-cacheRead`, color: "var(--color-chart-1)", top: 0.35, bottom: 0.05 },
      { id: `${instanceId}-cacheCreation`, color: "var(--color-chart-2)", top: 0.35, bottom: 0.05 },
      { id: `${instanceId}-output`, color: "var(--color-chart-4)", top: 0.45, bottom: 0.08 },
      { id: `${instanceId}-input`, color: "var(--color-chart-5)", top: 0.5, bottom: 0.1 },
    ],
    [instanceId],
  );

  // Compute domain + ticks for the token axis. Three modes:
  //   log: decade ticks, ceiling snapped to 1/2/5 × 10ⁿ
  //   log + linearThreshold: decade ticks below threshold, linear step above
  //   linear + linearTickStep: flat 10k/100k/… ticks
  const { yDomain, yTicks } = useMemo(() => {
    let m = 1;
    for (const d of data) {
      for (const k of TOKEN_KEYS) {
        const v = d[k];
        if (typeof v === "number" && v > m) m = v;
      }
    }
    if (yScale === "log") {
      if (linearThreshold && linearTickStep) {
        const { ticks, ceiling } = hybridLogLinearTicks(
          m,
          linearThreshold,
          linearTickStep,
        );
        return { yDomain: [1, ceiling] as [number, number], yTicks: ticks };
      }
      const ceiling = logCeiling(m);
      return {
        yDomain: [1, ceiling] as [number, number],
        yTicks: logTicks(ceiling),
      };
    }
    if (linearTickStep && linearTickStep > 0) {
      const ticks = linearTicksAtStep(m, linearTickStep);
      return {
        yDomain: [0, ticks[ticks.length - 1]] as [number, number],
        yTicks: ticks,
      };
    }
    return { yDomain: undefined, yTicks: undefined };
  }, [data, yScale, linearTickStep, linearThreshold]);

  const wrapperHeight = height + (showBrush ? 36 : 0);

  return (
    <div
      style={{
        width: "100%",
        height: wrapperHeight,
        opacity: mounted ? 1 : 0,
        transform: mounted ? "translateY(0)" : "translateY(4px)",
        transition: "opacity 280ms ease, transform 280ms ease",
      }}
    >
      <ChartContainer
        id={instanceId}
        config={chartConfig}
        className="!aspect-auto h-full w-full"
      >
        <ComposedChart
          data={data}
          margin={{ top: 12, right: 48, bottom: showBrush ? 8 : 4, left: 4 }}
        >
          <defs>
            {gradients.map((g) => (
              <linearGradient key={g.id} id={g.id} x1="0" y1="0" x2="0" y2="1">
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
            domain={yDomain}
            ticks={yTicks}
            tickFormatter={fmtTokens}
            axisLine={false}
            tickLine={false}
            tick={{ fill: "var(--color-muted-foreground)", fontSize: 11 }}
            width={44}
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
          <ChartTooltip
            cursor={{
              stroke: "var(--color-border-strong)",
              strokeDasharray: "2 4",
              strokeWidth: 1,
            }}
            content={
              <ChartTooltipContent
                indicator="dot"
                // Shadcn's ChartTooltipContent resolves `label` through
                // ChartConfig, which works for categorical x-axes but not
                // our numeric ts/turn. Read the raw x value off the first
                // payload entry's source row so the header stays correct.
                labelFormatter={(_label, payload) => {
                  const raw = payload?.[0]?.payload as
                    | Record<string, unknown>
                    | undefined;
                  const x = raw?.[xKey];
                  if (typeof x !== "number") return "";
                  return xLabelFormatter
                    ? xLabelFormatter(x)
                    : xTickFormatter(x);
                }}
                // Token series = fmtTokens; cost = fmtUsd + money color.
                formatter={(value, name) => {
                  const n = Number(value);
                  const isCost = name === "cost";
                  return (
                    <>
                      <span
                        aria-hidden
                        className="inline-block h-2 w-2 shrink-0 rounded-[2px]"
                        style={{
                          background: `var(--color-${String(name)})`,
                        }}
                      />
                      <div className="flex flex-1 items-center justify-between gap-3 leading-none">
                        <span className="text-muted-foreground">
                          {chartConfig[name as keyof typeof chartConfig]?.label ?? name}
                        </span>
                        <span
                          className={
                            isCost
                              ? "font-mono text-[var(--color-money)] font-medium tabular-nums"
                              : "font-mono text-foreground tabular-nums"
                          }
                        >
                          {isCost ? fmtUsd(n) : fmtTokens(n)}
                        </span>
                      </div>
                    </>
                  );
                }}
              />
            }
          />
          {/* Render order: largest series first so smaller ones stay visible on top.
              Areas are independent (not stacked): stacking is meaningless on log
              and busy on linear. Each series gets its own gradient fill. */}
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
      </ChartContainer>
    </div>
  );
}

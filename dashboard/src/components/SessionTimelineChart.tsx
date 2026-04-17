import { useEffect, useMemo, useState } from "react";
import type { TransactionRow } from "@/lib/store";
import { estimateCostUsd } from "@/lib/format";
import { subscribeRows } from "@/lib/rowsBus";
import TokenAreaChart, { type TokenAreaPoint } from "./charts/TokenAreaChart";

type Point = TokenAreaPoint & { turn: number; ts: number };

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

export default function SessionTimelineChart({
  initialRows,
  sessionId,
}: {
  initialRows: TransactionRow[];
  sessionId: string;
}) {
  const [rows, setRows] = useState<TransactionRow[]>(initialRows);

  useEffect(() => subscribeRows(setRows), []);

  const data = useMemo(() => rowsToPoints(rows, sessionId), [rows, sessionId]);

  if (data.length === 0) return null;
  return (
    <TokenAreaChart
      data={data}
      xKey="turn"
      xTickFormatter={(v) => `#${v}`}
      xLabelFormatter={(v) => `Turn #${v}`}
      yScale="linear"
      instanceId="sessionTimeline"
      showCostDots
    />
  );
}

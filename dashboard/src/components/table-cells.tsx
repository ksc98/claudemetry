import * as React from "react";
import { Loader2, MessageSquareText, Sparkles } from "lucide-react";
import type { TransactionRow } from "@/lib/store";
import { useHydrated } from "@/hooks/use-hydrated";
import {
  estimateCostUsd,
  fmtAgo,
  fmtDuration,
  fmtInt,
  fmtUsd,
} from "@/lib/format";
import { shortToolName } from "@/lib/tools";
import { stopDotClass } from "@/lib/stop";
import { cn } from "@/lib/cn";

export function shortModel(m: string | null | undefined): string {
  if (!m) return "—";
  return m.replace(/-\d{8}$/, "").replace(/^claude-/, "");
}

/** Columns whose numeric/cost content is right-aligned. */
export const RIGHT_ALIGNED_COLS: ReadonlySet<string> = new Set([
  "in",
  "out",
  "cache_read",
  "cache_5m",
  "cache_1h",
  "duration",
  "cost",
]);

export const COLUMN_LABELS: Readonly<Record<string, string>> = {
  turn: "Turn #",
  when: "When",
  model: "Model",
  in: "Input tokens",
  out: "Output tokens",
  cache_read: "Cache read",
  cache_5m: "Cache write 5m",
  cache_1h: "Cache write 1h",
  duration: "Duration",
  tools: "Tools",
  cost: "Cost",
};

export function StopDot({ tx }: { tx: TransactionRow }) {
  if (tx.in_flight === 1) {
    return (
      <Loader2
        size={10}
        className="animate-spin text-[var(--color-subtle-foreground)]"
        aria-label="in flight"
      />
    );
  }
  const cls = stopDotClass(tx.stop_reason);
  if (!cls) return null;
  return (
    <span
      className={cn("dot", cls)}
      title={tx.stop_reason ?? "—"}
      style={{ marginRight: 0 }}
    />
  );
}

export function WhenCell({ tx }: { tx: TransactionRow }) {
  const finishedAt = tx.in_flight === 1 ? tx.ts : tx.ts + tx.elapsed_ms;
  // Render the empty span on SSR + first hydration paint, then fill in
  // once the client has fully mounted. Base.astro's global 1s ticker
  // populates the text immediately after via the `data-ts` attribute,
  // and React's own re-renders take over from there. Prevents React #418.
  const hydrated = useHydrated();
  return (
    <span
      data-ts={finishedAt}
      className="text-[var(--color-muted-foreground)] font-mono text-xs tabular-nums whitespace-nowrap"
      suppressHydrationWarning
    >
      {hydrated ? fmtAgo(finishedAt) : ""}
    </span>
  );
}

export function DurationCell({ tx }: { tx: TransactionRow }) {
  return (
    <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-subtle-foreground)]">
      {tx.in_flight === 1 ? "—" : fmtDuration(tx.elapsed_ms)}
    </span>
  );
}

export function ModelCell({ tx }: { tx: TransactionRow }) {
  const inflight = tx.in_flight === 1;
  const m = tx.model;
  const thought = (tx.thinking_blocks ?? 0) > 0;
  const budget = tx.thinking_budget ?? null;
  const hasText = tx.has_text === 1;
  return (
    <span
      className={cn(
        "font-mono text-xs inline-flex items-center gap-1.5 whitespace-nowrap",
        inflight && "text-[var(--color-subtle-foreground)]",
      )}
    >
      <span title={m ?? "—"}>{shortModel(m)}</span>
      {(thought || budget != null) && (
        <span
          className="inline-flex items-center gap-0.5 text-[var(--color-chart-4)]"
          title={`extended thinking${thought ? ` · ${tx.thinking_blocks} block${(tx.thinking_blocks ?? 0) > 1 ? "s" : ""}` : inflight ? " budget set" : " budget set, not used this turn"}${budget ? ` · budget ${fmtInt(budget)}` : ""}`}
        >
          <Sparkles size={10} className="shrink-0" aria-label="extended thinking" />
          {budget != null && (
            <span className="tabular-nums text-[0.625rem]">
              {budget >= 1000 ? `${Math.round(budget / 1000)}k` : budget}
            </span>
          )}
        </span>
      )}
      {hasText && (
        <span
          className="inline-flex items-center text-[var(--color-muted-foreground)]"
          title="assistant replied with text"
        >
          <MessageSquareText
            size={10}
            className="shrink-0"
            aria-label="assistant replied with text"
          />
        </span>
      )}
    </span>
  );
}

export function InTokensCell({ tx }: { tx: TransactionRow }) {
  return (
    <span
      className={cn(
        "block text-right font-mono text-xs tabular-nums",
        tx.in_flight === 1 && "text-[var(--color-subtle-foreground)]",
      )}
    >
      {tx.in_flight === 1 ? "—" : fmtInt(tx.input_tokens)}
    </span>
  );
}

export function OutTokensCell({ tx }: { tx: TransactionRow }) {
  if (tx.in_flight === 1) {
    return (
      <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-subtle-foreground)]">
        —
      </span>
    );
  }
  const mx = tx.max_tokens ?? 0;
  const util = mx > 0 ? tx.output_tokens / mx : 0;
  const atCeiling = util >= 0.95;
  return (
    <span
      className={cn(
        "block text-right font-mono text-xs tabular-nums",
        atCeiling && "text-[var(--color-warn)] font-medium",
      )}
      title={
        mx > 0
          ? `${fmtInt(tx.output_tokens)} / ${fmtInt(mx)} max (${(util * 100).toFixed(0)}%)`
          : undefined
      }
    >
      {fmtInt(tx.output_tokens)}
    </span>
  );
}

export function CacheReadCell({ tx }: { tx: TransactionRow }) {
  return (
    <span
      className={cn(
        "block text-right font-mono text-xs tabular-nums",
        tx.in_flight === 1
          ? "text-[var(--color-subtle-foreground)]"
          : "text-[var(--color-volume)]/80",
      )}
    >
      {tx.in_flight === 1 ? "—" : fmtInt(tx.cache_read)}
    </span>
  );
}

export function CacheWrite5mCell({ tx }: { tx: TransactionRow }) {
  if (tx.in_flight === 1) {
    return (
      <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-subtle-foreground)]">
        —
      </span>
    );
  }
  const v = tx.cache_creation_5m ?? 0;
  return (
    <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-volume)]/55">
      {fmtInt(v)}
    </span>
  );
}

export function CacheWrite1hCell({ tx }: { tx: TransactionRow }) {
  if (tx.in_flight === 1) {
    return (
      <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-subtle-foreground)]">
        —
      </span>
    );
  }
  const v = tx.cache_creation_1h ?? 0;
  return (
    <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-volume)]/55">
      {fmtInt(v)}
    </span>
  );
}

export function ToolsCell({ tx }: { tx: TransactionRow }) {
  if (tx.in_flight === 1 && tx.tool_choice) {
    const tc = tx.tool_choice;
    const label = tc.startsWith("tool:") ? shortToolName(tc.slice(5)) : tc;
    return (
      <div className="flex flex-wrap gap-1 max-w-[22rem]">
        <span className="chip opacity-60" title={`tool_choice: ${tc}`}>
          {label}
        </span>
      </div>
    );
  }

  const raw: string[] = tx.tools_json ? JSON.parse(tx.tools_json) : [];
  const tools = raw.map(shortToolName);
  if (tools.length === 0)
    return (
      <span className="text-[var(--color-subtle-foreground)] text-xs">—</span>
    );
  return (
    <div className="flex flex-wrap gap-1 max-w-[22rem]">
      {tools.slice(0, 3).map((t) => (
        <span key={t} className="chip" title={t}>
          {t.length > 22 ? t.slice(0, 22) + "…" : t}
        </span>
      ))}
      {tools.length > 3 && (
        <span className="chip" title={tools.slice(3).join(", ")}>
          +{tools.length - 3}
        </span>
      )}
    </div>
  );
}

export function CostCell({ tx }: { tx: TransactionRow }) {
  return (
    <span
      className={cn(
        "block text-right font-mono text-xs tabular-nums",
        tx.in_flight === 1
          ? "text-[var(--color-subtle-foreground)]"
          : "text-[var(--color-money)]",
      )}
    >
      {tx.in_flight === 1 ? "—" : fmtUsd(estimateCostUsd(tx))}
    </span>
  );
}

/** Accessor helpers — keep filter/sort keys consistent between tables. */
export const txAccessors = {
  when: (tx: TransactionRow) =>
    tx.in_flight === 1 ? tx.ts : tx.ts + tx.elapsed_ms,
  duration: (tx: TransactionRow) => tx.elapsed_ms,
  model: (tx: TransactionRow) => tx.model ?? "",
  in: (tx: TransactionRow) => tx.input_tokens,
  out: (tx: TransactionRow) => tx.output_tokens,
  cache_read: (tx: TransactionRow) => tx.cache_read,
  cache_5m: (tx: TransactionRow) => tx.cache_creation_5m ?? 0,
  cache_1h: (tx: TransactionRow) => tx.cache_creation_1h ?? 0,
  cost: (tx: TransactionRow) => estimateCostUsd(tx),
  tools: (tx: TransactionRow) => {
    const arr: string[] = tx.tools_json ? JSON.parse(tx.tools_json) : [];
    return arr.map(shortToolName).join(" ");
  },
  stop: (tx: TransactionRow) =>
    tx.stop_reason ?? (tx.in_flight === 1 ? "(in flight)" : "(none)"),
} as const;

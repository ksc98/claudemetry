import { useEffect, useMemo, useState } from "react";
import { DollarSign, Flame, MessageSquare, Layers, AlertTriangle, Gauge } from "lucide-react";
import type { Stats, TransactionRow } from "@/lib/store";
import type { SessionSummary } from "@/lib/sessions";
import { estimateCostUsd } from "@/lib/format";
import { subscribeStats } from "@/lib/statsBus";
import { subscribeRows } from "@/lib/rowsBus";
import { subscribeSessions } from "@/lib/sessionsBus";

interface InitialHeaderStats {
  spendUsd: number;
  turns: number;
  sessions: number;
  activeSession: string | null;
  burnUsdPerMin: number | null;
  errorRate: number | null;
  rateLimit: { tokRemaining: number; tokLimit: number } | null;
}

function fmtUsd(n: number): string {
  if (n === 0) return "$0.00";
  if (n < 0.01) return `$${n.toFixed(4)}`;
  return `$${n.toFixed(2)}`;
}

function fmtBurn(n: number): string {
  if (n < 0.01) return `$${n.toFixed(4)}/min`;
  return `$${n.toFixed(2)}/min`;
}

function shortSession(id: string): string {
  return id.slice(0, 8);
}

function deriveFromRows(rows: TransactionRow[]) {
  const now = Date.now();
  const fiveMinAgo = now - 5 * 60_000;
  const recentSpend = rows
    .filter((r) => r.ts >= fiveMinAgo)
    .reduce((a, r) => a + estimateCostUsd(r), 0);
  const burnUsdPerMin = recentSpend / 5;

  // Rows from /recent are desc — take the first 50 as "last 50 turns".
  const last50 = rows.slice(0, 50);
  const errorRate =
    last50.length > 0
      ? last50.filter((r) => r.status >= 400).length / last50.length
      : 0;

  const rlRow = rows.find(
    (r) =>
      r.rl_tok_remaining != null &&
      r.rl_tok_limit != null &&
      r.rl_tok_limit > 0,
  );
  const rateLimit =
    rlRow && rlRow.rl_tok_remaining != null && rlRow.rl_tok_limit != null
      ? { tokRemaining: rlRow.rl_tok_remaining, tokLimit: rlRow.rl_tok_limit }
      : null;

  return { burnUsdPerMin, errorRate, rateLimit };
}

export default function LiveHeaderStats({
  initialStats,
  live = false,
}: {
  initialStats: InitialHeaderStats;
  /** Subscribe to global statsBus/rowsBus/sessionsBus. Pass `true` on pages
   * whose header reflects global (all-time, all-session) metrics — e.g. the
   * overview page. Session-detail pages leave this `false` so the header
   * keeps its scoped SSR values instead of flipping to global totals after
   * hydration. */
  live?: boolean;
}) {
  const [stats, setStats] = useState<Partial<Stats>>({
    turns: initialStats.turns,
    sessions: initialStats.sessions,
    estimated_cost_usd: initialStats.spendUsd,
  });
  const [rows, setRows] = useState<TransactionRow[] | null>(null);
  const [sessions, setSessions] = useState<SessionSummary[] | null>(null);

  useEffect(() => {
    if (!live) return;
    return subscribeStats(setStats);
  }, [live]);

  useEffect(() => {
    if (!live) return;
    return subscribeRows(setRows);
  }, [live]);

  useEffect(() => {
    if (!live) return;
    return subscribeSessions(setSessions);
  }, [live]);

  const spendUsd = stats.estimated_cost_usd ?? initialStats.spendUsd;
  const turns = stats.turns ?? initialStats.turns;
  const totalSessions = stats.sessions ?? initialStats.sessions;

  const activeSession = useMemo(() => {
    if (!sessions) return initialStats.activeSession;
    const top = sessions.find((s) => s.active);
    return top ? shortSession(top.id) : null;
  }, [sessions, initialStats.activeSession]);

  const { burnUsdPerMin, errorRate, rateLimit } = useMemo(() => {
    if (rows === null) {
      return {
        burnUsdPerMin: initialStats.burnUsdPerMin ?? 0,
        errorRate: initialStats.errorRate ?? 0,
        rateLimit: initialStats.rateLimit,
      };
    }
    return deriveFromRows(rows);
  }, [rows, initialStats.burnUsdPerMin, initialStats.errorRate, initialStats.rateLimit]);

  const rl = rateLimit;
  const rlPct =
    rl && rl.tokLimit > 0
      ? Math.max(0, Math.min(100, (rl.tokRemaining / rl.tokLimit) * 100))
      : null;
  const rlColor =
    rlPct == null
      ? null
      : rlPct < 15
        ? "var(--color-danger)"
        : rlPct < 40
          ? "var(--color-warn)"
          : "var(--color-good)";

  return (
    <>
      <div
        data-live-region="header-stats"
        className="hidden md:flex items-center gap-3 pl-4 border-l border-[var(--color-border)] text-xs text-[var(--color-muted-foreground)]"
      >
        {activeSession ? (
          <span
            className="flex items-center gap-1.5"
            title={`Active session: ${activeSession}`}
          >
            <span className="live-dot" />
            <span className="font-medium text-[var(--color-good)]">active</span>
            <code className="font-mono text-[var(--color-foreground)] text-[0.6875rem]">
              {activeSession}
            </code>
          </span>
        ) : (
          <span className="flex items-center gap-1.5" title="No active session">
            <span className="inline-block w-2 h-2 rounded-full bg-[var(--color-subtle-foreground)]" />
            <span className="font-medium text-[var(--color-muted-foreground)]">idle</span>
          </span>
        )}
        <span className="text-[var(--color-subtle-foreground)]">·</span>
        <span
          className="inline-flex items-center gap-1 tabular-nums font-mono"
          title="Total spend in the current window"
        >
          <DollarSign size={11} className="text-[var(--color-money)] opacity-70" />
          <span className="text-[var(--color-money)] font-medium">{fmtUsd(spendUsd)}</span>
        </span>
        {burnUsdPerMin != null && burnUsdPerMin > 0 && (
          <>
            <span className="text-[var(--color-subtle-foreground)]">·</span>
            <span
              className="inline-flex items-center gap-1 tabular-nums font-mono text-[var(--color-money)]/80"
              title="Trailing 5-minute burn rate"
            >
              <Flame size={11} className="opacity-70" />
              {fmtBurn(burnUsdPerMin)}
            </span>
          </>
        )}
        <span className="text-[var(--color-subtle-foreground)]">·</span>
        <span
          className="inline-flex items-center gap-1 tabular-nums font-mono"
          title={`${turns} turns in the current window`}
        >
          <MessageSquare size={11} className="opacity-60" />
          {turns} turns
        </span>
        <span className="text-[var(--color-subtle-foreground)]">·</span>
        <span
          className="inline-flex items-center gap-1 tabular-nums font-mono"
          title={`${totalSessions} sessions in the current window`}
        >
          <Layers size={11} className="opacity-60" />
          {totalSessions} sessions
        </span>
        {errorRate != null && errorRate > 0 && (
          <>
            <span className="text-[var(--color-subtle-foreground)]">·</span>
            <span
              className="inline-flex items-center gap-1 tabular-nums font-mono text-[var(--color-danger)] font-medium"
              title="Fraction of recent turns with status ≥ 400"
            >
              <AlertTriangle size={11} />
              {(errorRate * 100).toFixed(errorRate < 0.1 ? 1 : 0)}% err
            </span>
          </>
        )}
        {rl && rlPct != null && rlColor && (
          <>
            <span className="text-[var(--color-subtle-foreground)]">·</span>
            <span
              className="inline-flex items-center gap-1.5 tabular-nums"
              title={`Anthropic rate-limit — ${rl.tokRemaining.toLocaleString()} / ${rl.tokLimit.toLocaleString()} input tokens remaining this window`}
            >
              <Gauge size={11} className="opacity-60" />
              <span
                className="inline-flex overflow-hidden rounded-full bg-[var(--color-border)]"
                style={{ width: 32, height: 4 }}
              >
                <span style={{ width: `${rlPct}%`, height: "100%", background: rlColor }} />
              </span>
              <span className="font-mono text-[var(--color-muted-foreground)]">
                {Math.round(rlPct)}%
              </span>
            </span>
          </>
        )}
      </div>
      <div
        data-live-region="header-stats-mobile"
        className="flex md:hidden items-center gap-2 pl-3 border-l border-[var(--color-border)] text-xs text-[var(--color-muted-foreground)] min-w-0"
      >
        {activeSession ? (
          <span className="flex items-center gap-1 shrink-0">
            <span className="live-dot" />
            <span className="font-medium text-[var(--color-good)]">live</span>
          </span>
        ) : (
          <span className="inline-block w-1.5 h-1.5 rounded-full bg-[var(--color-subtle-foreground)] shrink-0" />
        )}
        <span className="text-[var(--color-money)] font-medium font-mono tabular-nums">
          {fmtUsd(spendUsd)}
        </span>
      </div>
    </>
  );
}

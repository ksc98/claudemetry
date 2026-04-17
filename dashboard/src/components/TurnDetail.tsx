import * as React from "react";
import { Check, Copy, Loader2, Sparkles } from "lucide-react";
import type { TransactionRow } from "@/lib/store";
import { fmtBytes, fmtInt, fmtTs } from "@/lib/format";
import { shortToolName } from "@/lib/tools";
import { stopDotClass } from "@/lib/stop";
import { cn } from "@/lib/cn";

// List endpoints drop user_text / assistant_text to keep the recent-turns
// + session-turns payloads small. The detail view fetches them on-demand
// from /api/turn the first time the user expands a row, and caches the
// hydrated row for the session so re-expanding is free.
const turnDetailCache = new Map<string, TransactionRow>();

function hasDetail(tx: TransactionRow): boolean {
  return tx.user_text != null || tx.assistant_text != null;
}

export function TurnDetail({ tx: initial }: { tx: TransactionRow }) {
  const [tx, setTx] = React.useState<TransactionRow>(
    () => turnDetailCache.get(initial.tx_id) ?? initial,
  );
  const [loading, setLoading] = React.useState(
    () =>
      !hasDetail(tx) &&
      !turnDetailCache.has(initial.tx_id) &&
      initial.in_flight !== 1,
  );

  React.useEffect(() => {
    // Already have text columns (session page hands us full rows) → nothing to do.
    if (hasDetail(tx)) return;
    // In-flight placeholder has no text yet; the row will refresh itself
    // via the rowsBus once finalized. Don't bother fetching.
    if (initial.in_flight === 1) return;
    // Cached from a previous expand in this session.
    const cached = turnDetailCache.get(initial.tx_id);
    if (cached) {
      setTx(cached);
      setLoading(false);
      return;
    }
    const ctrl = new AbortController();
    setLoading(true);
    fetch(`/api/turn?id=${encodeURIComponent(initial.tx_id)}`, {
      signal: ctrl.signal,
      cache: "no-store",
    })
      .then((r) => (r.ok ? r.json() : Promise.reject(r.status)))
      .then((full: TransactionRow) => {
        turnDetailCache.set(initial.tx_id, full);
        setTx(full);
      })
      .catch(() => {
        /* leave placeholder; user can re-expand to retry */
      })
      .finally(() => setLoading(false));
    return () => ctrl.abort();
  }, [initial.tx_id, initial.in_flight, tx]);

  const [copied, setCopied] = React.useState(false);

  const copyJson = React.useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      navigator.clipboard.writeText(JSON.stringify(tx, null, 2)).then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      });
    },
    [tx],
  );

  const tools: string[] = tx.tools_json ? JSON.parse(tx.tools_json) : [];
  const hasConversation = !!(tx.user_text || tx.assistant_text);
  return (
    <div className="flex flex-col gap-4">
      <div className="flex justify-end">
        <button
          type="button"
          onClick={copyJson}
          className={cn(
            "inline-flex items-center gap-1.5 text-[11px] font-mono px-2 py-1 rounded",
            "border border-[var(--color-border)] hover:bg-[var(--color-border)]/40 transition-colors",
            copied
              ? "text-[var(--color-good)]"
              : "text-[var(--color-muted-foreground)]",
          )}
        >
          {copied ? <Check size={11} /> : <Copy size={11} />}
          {copied ? "Copied" : "Copy JSON"}
        </button>
      </div>
      {loading && (
        <p className="flex items-center gap-2 text-xs text-[var(--color-subtle-foreground)]">
          <Loader2 size={12} className="animate-spin" />
          Loading conversation…
        </p>
      )}
      {hasConversation && (
        <div className="flex flex-col gap-3">
          {tx.user_text ? (
            <ConversationBlock role="you" text={tx.user_text} />
          ) : null}
          {tx.assistant_text ? (
            <ConversationBlock role="assistant" text={tx.assistant_text} />
          ) : null}
        </div>
      )}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2 text-xs">
        <DetailRow
          label="tx_id"
          value={<code className="font-mono">{tx.tx_id}</code>}
        />
        <DetailRow
          label="ts"
          value={<span className="font-mono">{fmtTs(tx.ts)}</span>}
        />
        <DetailRow
          label="url"
          value={
            <code
              className="font-mono text-[var(--color-muted-foreground)] break-all"
              title={tx.url ?? undefined}
            >
              {tx.method ? `${tx.method} ` : ""}
              {tx.url ?? "—"}
            </code>
          }
        />
        <DetailRow
          label="status"
          value={
            <span
              className={cn(
                "font-mono tabular-nums",
                tx.status >= 400
                  ? "text-[var(--color-danger)]"
                  : "text-[var(--color-good)]",
              )}
            >
              {tx.status}
            </span>
          }
        />
        <DetailRow
          label="stop_reason"
          value={
            <span className="font-mono">
              {(() => {
                const cls = stopDotClass(tx.stop_reason);
                return cls ? (
                  <span className={cn("dot", cls)} style={{ marginRight: 6 }} />
                ) : null;
              })()}
              {tx.stop_reason ?? "—"}
            </span>
          }
        />
        <DetailRow
          label="req / resp bytes"
          value={
            <span className="font-mono tabular-nums">
              {fmtBytes(tx.req_body_bytes)} · {fmtBytes(tx.resp_body_bytes)}
            </span>
          }
        />
        {tx.max_tokens != null && (
          <DetailRow
            label="max_tokens"
            value={
              tx.in_flight === 1 ? (
                <span className="font-mono tabular-nums text-[var(--color-subtle-foreground)]">
                  {fmtInt(tx.max_tokens)}
                </span>
              ) : (
                <span className="font-mono tabular-nums">
                  {fmtInt(tx.output_tokens)} / {fmtInt(tx.max_tokens)}
                  <span className="text-[var(--color-subtle-foreground)] ml-2">
                    ({((tx.output_tokens / tx.max_tokens) * 100).toFixed(0)}%)
                  </span>
                </span>
              )
            }
          />
        )}
        {(tx.thinking_budget != null || (tx.thinking_blocks ?? 0) > 0) && (
          <DetailRow
            label="thinking"
            value={
              <span className="font-mono tabular-nums inline-flex items-center gap-2">
                <Sparkles size={11} className="text-[var(--color-chart-4)]" />
                {tx.thinking_budget != null && (
                  <span>budget {fmtInt(tx.thinking_budget)}</span>
                )}
                {(tx.thinking_blocks ?? 0) > 0 && (
                  <span className="text-[var(--color-muted-foreground)]">
                    · {tx.thinking_blocks} block
                    {(tx.thinking_blocks ?? 0) > 1 ? "s" : ""}
                  </span>
                )}
              </span>
            }
          />
        )}
        {(tx.cache_creation_5m != null || tx.cache_creation_1h != null) && (
          <DetailRow
            label="cache writes"
            value={
              <span className="font-mono tabular-nums text-[var(--color-muted-foreground)]">
                {fmtInt(tx.cache_creation_5m ?? 0)} × 5m ·{" "}
                {fmtInt(tx.cache_creation_1h ?? 0)} × 1h
              </span>
            }
          />
        )}
        {tx.rl_tok_remaining != null && tx.rl_tok_limit != null && (
          <DetailRow
            label="rate-limit"
            value={
              <span className="font-mono tabular-nums text-[var(--color-muted-foreground)]">
                {fmtInt(tx.rl_tok_remaining)} / {fmtInt(tx.rl_tok_limit)} input
                tokens remaining
              </span>
            }
          />
        )}
        {tools.length > 0 && (
          <div className="md:col-span-2">
            <p className="text-[0.6875rem] uppercase tracking-[0.08em] text-[var(--color-muted-foreground)] mb-1">
              Tools ({tools.length})
            </p>
            <div className="flex flex-wrap gap-1">
              {tools.map((t, i) => (
                <span key={`${t}-${i}`} className="chip" title={t}>
                  {shortToolName(t)}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function ConversationBlock({
  role,
  text,
}: {
  role: "you" | "assistant";
  text: string;
}) {
  const [expanded, setExpanded] = React.useState(false);
  const LONG_THRESHOLD = 600;
  const isLong = text.length > LONG_THRESHOLD;
  return (
    <div className="flex flex-col gap-1">
      <div className="flex items-center justify-between">
        <span
          className={cn(
            "text-[0.6875rem] uppercase tracking-[0.08em] font-mono",
            role === "you"
              ? "text-sky-400/80"
              : "text-amber-400/80",
          )}
        >
          {role}{" "}
          <span className="text-[var(--color-subtle-foreground)] normal-case tracking-normal">
            · {fmtInt(text.length)} chars
          </span>
        </span>
        {isLong && (
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              setExpanded((v) => !v);
            }}
            className="text-[10px] text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)]"
          >
            {expanded ? "shrink" : "expand"}
          </button>
        )}
      </div>
      <div
        className={cn(
          "rounded-md border border-[var(--color-border)] bg-[var(--color-background)]/60 px-3 py-2.5",
          "text-[12px] leading-relaxed text-[var(--color-foreground)]/90",
          "whitespace-pre-wrap break-words overflow-auto",
          expanded ? "max-h-[36rem]" : "max-h-60",
        )}
        onClick={(e) => e.stopPropagation()}
      >
        {text}
      </div>
    </div>
  );
}

function DetailRow({
  label,
  value,
}: {
  label: string;
  value: React.ReactNode;
}) {
  return (
    <div className="flex items-baseline gap-3 min-w-0">
      <span className="text-[0.6875rem] uppercase tracking-[0.08em] text-[var(--color-muted-foreground)] w-24 shrink-0">
        {label}
      </span>
      <span className="min-w-0 flex-1 truncate">{value}</span>
    </div>
  );
}

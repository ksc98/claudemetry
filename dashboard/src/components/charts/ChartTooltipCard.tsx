import type { TooltipProps } from "recharts";

type Row = { name: string; value: number; color?: string };

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

const NAME_LABEL: Record<string, string> = {
  input: "input",
  output: "output",
  cache_read: "cache read",
  cache_creation: "cache create",
  cost: "cost",
};

export default function ChartTooltipCard({
  active,
  payload,
  label,
  labelFormatter,
}: TooltipProps<number, string> & {
  labelFormatter?: (v: unknown) => string;
}) {
  if (!active || !payload || payload.length === 0) return null;

  const rows: Row[] = payload
    .filter((p) => p.value != null)
    .map((p) => ({
      name: String(p.name ?? p.dataKey ?? ""),
      value: Number(p.value),
      color: (p.color ?? p.stroke ?? undefined) as string | undefined,
    }));

  // Put cost last so it visually groups with the dollar amount.
  rows.sort((a, b) => (a.name === "cost" ? 1 : b.name === "cost" ? -1 : 0));

  return (
    <div
      role="tooltip"
      className="rounded-lg border border-[var(--color-border-strong)] bg-[var(--color-card-elevated)] px-3 py-2 text-xs shadow-lg backdrop-blur-sm"
      style={{ minWidth: 160 }}
    >
      {label != null && (
        <div className="mb-1.5 text-[0.6875rem] uppercase tracking-[0.08em] text-[var(--color-muted-foreground)] font-medium">
          {labelFormatter ? labelFormatter(label) : String(label)}
        </div>
      )}
      <ul className="space-y-1">
        {rows.map((r) => (
          <li
            key={r.name}
            className="flex items-center justify-between gap-3 tabular-nums"
          >
            <span className="flex items-center gap-2 text-[var(--color-muted-foreground)]">
              <span
                aria-hidden
                className="inline-block h-2 w-2 rounded-sm"
                style={{ background: r.color }}
              />
              <span>{NAME_LABEL[r.name] ?? r.name}</span>
            </span>
            <span
              className={
                r.name === "cost"
                  ? "font-mono text-[var(--color-money)] font-medium"
                  : "font-mono text-[var(--color-foreground)]"
              }
            >
              {r.name === "cost" ? fmtUsd(r.value) : fmtTokens(r.value)}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

// Model pricing per 1M tokens (USD). Source: platform.claude.com/docs/en/about-claude/pricing
// cacheRead = 0.1x base input; cacheWrite5m = 1.25x base input.
const PRICING: Record<string, { in: number; out: number; cacheRead: number; cacheWrite5m: number }> = {
  "claude-opus-4-6":            { in: 5,    out: 25,   cacheRead: 0.50, cacheWrite5m: 6.25  },
  "claude-opus-4-5":            { in: 5,    out: 25,   cacheRead: 0.50, cacheWrite5m: 6.25  },
  "claude-opus-4-1":            { in: 15,   out: 75,   cacheRead: 1.50, cacheWrite5m: 18.75 },
  "claude-opus-4":              { in: 15,   out: 75,   cacheRead: 1.50, cacheWrite5m: 18.75 },
  "claude-sonnet-4-6":          { in: 3,    out: 15,   cacheRead: 0.30, cacheWrite5m: 3.75  },
  "claude-sonnet-4-5":          { in: 3,    out: 15,   cacheRead: 0.30, cacheWrite5m: 3.75  },
  "claude-sonnet-4":            { in: 3,    out: 15,   cacheRead: 0.30, cacheWrite5m: 3.75  },
  "claude-sonnet-4-5-20241022": { in: 3,    out: 15,   cacheRead: 0.30, cacheWrite5m: 3.75  },
  "claude-haiku-4-5-20251001":  { in: 1,    out: 5,    cacheRead: 0.10, cacheWrite5m: 1.25  },
  "claude-haiku-4-5":           { in: 1,    out: 5,    cacheRead: 0.10, cacheWrite5m: 1.25  },
  "claude-3-5-sonnet-20241022": { in: 3,    out: 15,   cacheRead: 0.30, cacheWrite5m: 3.75  },
  "claude-3-5-sonnet-20240620": { in: 3,    out: 15,   cacheRead: 0.30, cacheWrite5m: 3.75  },
  "claude-3-5-haiku-20241022":  { in: 1,    out: 5,    cacheRead: 0.10, cacheWrite5m: 1.25  },
  "claude-3-opus-20240229":     { in: 15,   out: 75,   cacheRead: 1.50, cacheWrite5m: 18.75 },
  "claude-3-haiku-20240307":    { in: 0.25, out: 1.25, cacheRead: 0.03, cacheWrite5m: 0.30  },
};

export function estimateCostUsd(row: {
  model: string | null;
  input_tokens: number;
  output_tokens: number;
  cache_read: number;
  cache_creation: number;
}): number {
  const p = PRICING[row.model ?? ""];
  if (!p) return 0;
  return (
    (row.input_tokens * p.in +
      row.output_tokens * p.out +
      row.cache_read * p.cacheRead +
      row.cache_creation * p.cacheWrite5m) /
    1_000_000
  );
}

export function fmtInt(n: number): string {
  return n.toLocaleString("en-US");
}

export function fmtUsd(n: number): string {
  if (n === 0) return "$0.00";
  if (n < 0.01) return `$${n.toFixed(4)}`;
  return `$${n.toFixed(2)}`;
}

export function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

export function fmtDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = Math.floor(s / 60);
  const rem = Math.round(s - m * 60);
  return `${m}m${String(rem).padStart(2, "0")}s`;
}

export function fmtTs(ms: number): string {
  const d = new Date(ms);
  return d.toISOString().replace("T", " ").slice(0, 19);
}

export function fmtAgo(ms: number): string {
  const diff = Date.now() - ms;
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rs = s % 60;
  if (m < 60) return rs > 0 ? `${m}m${rs}s` : `${m}m`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  if (h < 24) return rm > 0 ? `${h}h${rm}m` : `${h}h`;
  const days = Math.floor(h / 24);
  const rh = h % 24;
  return rh > 0 ? `${days}d${rh}h` : `${days}d`;
}

/** Format a timestamp as a clock time, e.g. "00:52" or "Apr 15 00:52" if not today. */
export function fmtTime(ms: number): string {
  const d = new Date(ms);
  const now = new Date();
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const time = `${hh}:${mm}`;
  if (
    d.getFullYear() === now.getFullYear() &&
    d.getMonth() === now.getMonth() &&
    d.getDate() === now.getDate()
  ) {
    return time;
  }
  const mon = d.toLocaleString("en", { month: "short" });
  return `${mon} ${d.getDate()} ${time}`;
}

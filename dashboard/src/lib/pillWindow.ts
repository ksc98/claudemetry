// Time-window pill on the overview. One canonical `since` value drives
// /stats, /recent, and every derived KPI / sparkline / chart on the page.
//
// Windows are free-form `Nm` / `Nh` / `Nd` strings (e.g. "3h", "15m", "7d").
// The pill lets the user pick a unit and an integer amount per unit.

export type Unit = "m" | "h" | "d";
export type Window = string;

export const UNITS: Unit[] = ["m", "h", "d"];

export const UNIT_MS: Record<Unit, number> = {
  m: 60_000,
  h: 60 * 60_000,
  d: 24 * 60 * 60_000,
};

// Max amount per unit. Crossing the boundary is what the neighbouring unit
// is for — 60m is just 1h, 24h is 1d, 31d is out of scope for this dashboard.
export const UNIT_MAX: Record<Unit, number> = {
  m: 59,
  h: 23,
  d: 30,
};

const WINDOW_RE = /^(\d+)([mhd])$/;

export const DEFAULT_WINDOW: Window = "3h";

export function isValidWindow(w: string): boolean {
  const m = WINDOW_RE.exec(w);
  if (!m) return false;
  const n = Number(m[1]);
  const u = m[2] as Unit;
  return Number.isFinite(n) && n >= 1 && n <= UNIT_MAX[u];
}

export function parseWindowParts(w: string): { n: number; u: Unit } {
  const m = WINDOW_RE.exec(w);
  if (!m) return parseWindowParts(DEFAULT_WINDOW);
  const u = m[2] as Unit;
  const n = Math.min(Math.max(Number(m[1]), 1), UNIT_MAX[u]);
  return { n, u };
}

export function formatWindow(n: number, u: Unit): Window {
  const clamped = Math.min(Math.max(Math.round(n), 1), UNIT_MAX[u]);
  return `${clamped}${u}`;
}

export function parseWindow(search: URLSearchParams): Window {
  const raw = search.get("w");
  if (raw && isValidWindow(raw)) return raw;
  return DEFAULT_WINDOW;
}

export function windowMs(w: Window): number {
  const { n, u } = parseWindowParts(w);
  return n * UNIT_MS[u];
}

export function windowSince(w: Window, now: number = Date.now()): number {
  return now - windowMs(w);
}

// Short windows render individual turns on the chart; longer ones benefit
// from date labels and bucketing.
export function windowIsShort(w: Window): boolean {
  return windowMs(w) <= UNIT_MS.h;
}

export function windowIsBucketed(w: Window): boolean {
  return windowMs(w) > UNIT_MS.d;
}

// Navigate to the current page with a new window selected. Full reload so
// the SSR-rendered KPIs + sparklines rebuild off the new `since`.
export function navigateToWindow(w: Window): void {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  if (w === DEFAULT_WINDOW) {
    url.searchParams.delete("w");
  } else {
    url.searchParams.set("w", w);
  }
  window.location.assign(url.toString());
}

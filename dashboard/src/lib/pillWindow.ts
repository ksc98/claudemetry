// Time-window pill on the overview. One canonical `since` value drives
// /stats, /recent, and every derived KPI / sparkline / chart on the page.

export type Window = "15m" | "1h" | "24h" | "3d" | "7d";

export const WINDOWS: Window[] = ["15m", "1h", "24h", "3d", "7d"];

export const WINDOW_MS: Record<Window, number> = {
  "15m": 15 * 60_000,
  "1h": 60 * 60_000,
  "24h": 24 * 60 * 60_000,
  "3d": 3 * 24 * 60 * 60_000,
  "7d": 7 * 24 * 60 * 60_000,
};

export const DEFAULT_WINDOW: Window = "1h";

export function parseWindow(search: URLSearchParams): Window {
  const raw = search.get("w");
  if (raw && (WINDOWS as string[]).includes(raw)) return raw as Window;
  return DEFAULT_WINDOW;
}

export function windowSince(w: Window, now: number = Date.now()): number {
  return now - WINDOW_MS[w];
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

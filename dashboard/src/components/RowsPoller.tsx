import { useEffect } from "react";
import type { TransactionRow } from "@/lib/store";
import { publishRows } from "@/lib/rowsBus";
import { parseWindow, windowSince } from "@/lib/pillWindow";

// Single page-level poller. Fetches a rows endpoint on an interval and
// publishes the result into rowsBus for every subscribing island.
// Pauses while the tab is hidden.
//
// When `windowed=true`, the poller reads the time-window pill out of the
// URL on mount and appends a rolling `?since=<ms>` to every request.
// Session pages leave it off since their endpoint isn't time-filtered.
export default function RowsPoller({
  intervalMs = 1000,
  endpoint = "/api/recent",
  windowed = false,
}: {
  intervalMs?: number;
  endpoint?: string;
  windowed?: boolean;
}) {
  useEffect(() => {
    let disposed = false;
    const ctrl = new AbortController();

    const win =
      windowed && typeof window !== "undefined"
        ? parseWindow(new URL(window.location.href).searchParams)
        : null;

    const buildUrl = () => {
      if (!win) return endpoint;
      const since = windowSince(win);
      const sep = endpoint.includes("?") ? "&" : "?";
      return `${endpoint}${sep}since=${since}`;
    };

    const tick = async () => {
      if (typeof document !== "undefined" && document.hidden) return;
      try {
        const res = await fetch(buildUrl(), {
          signal: ctrl.signal,
          headers: { accept: "application/json" },
          cache: "no-store",
        });
        if (!res.ok || disposed) return;
        const json = (await res.json()) as TransactionRow[];
        if (disposed) return;
        publishRows(json);
      } catch {
        /* next tick will retry */
      }
    };

    const iv = window.setInterval(tick, intervalMs);
    const onVis = () => {
      if (!document.hidden) void tick();
    };
    document.addEventListener("visibilitychange", onVis);

    void tick();

    return () => {
      disposed = true;
      ctrl.abort();
      window.clearInterval(iv);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, [intervalMs, endpoint, windowed]);

  return null;
}

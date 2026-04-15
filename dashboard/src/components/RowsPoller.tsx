import { useEffect } from "react";
import type { TransactionRow } from "@/lib/store";
import { publishRows } from "@/lib/rowsBus";

// Single page-level poller. Fetches /api/recent on an interval and publishes
// the result into rowsBus for every subscribing island (chart, table, …).
// Pauses while the tab is hidden.
export default function RowsPoller({
  intervalMs = 1000,
}: {
  intervalMs?: number;
}) {
  useEffect(() => {
    let disposed = false;
    const ctrl = new AbortController();

    const tick = async () => {
      if (document.hidden) return;
      try {
        const res = await fetch("/api/recent", {
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
      if (!document.hidden) tick();
    };
    document.addEventListener("visibilitychange", onVis);
    tick();

    return () => {
      disposed = true;
      ctrl.abort();
      window.clearInterval(iv);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, [intervalMs]);

  return null;
}

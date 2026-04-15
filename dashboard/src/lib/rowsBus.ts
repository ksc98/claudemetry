// Page-wide pub/sub for the latest /api/recent snapshot. Astro mounts each
// `client:load` island as its own React tree, so a React Context can't span
// them. Vite bundles this module into a shared chunk, giving us a single
// instance of `listeners` and `latest` for the page.

import type { TransactionRow } from "@/lib/store";

type Listener = (rows: TransactionRow[]) => void;

const listeners = new Set<Listener>();
let latest: TransactionRow[] | null = null;

export function publishRows(rows: TransactionRow[]): void {
  latest = rows;
  listeners.forEach((fn) => {
    try {
      fn(rows);
    } catch {
      /* one bad subscriber shouldn't break the others */
    }
  });
}

export function subscribeRows(fn: Listener): () => void {
  listeners.add(fn);
  if (latest) fn(latest);
  return () => {
    listeners.delete(fn);
  };
}

export function latestRows(): TransactionRow[] | null {
  return latest;
}

import * as React from "react";
import {
  flexRender,
  getCoreRowModel,
  getExpandedRowModel,
  getFilteredRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type ColumnFiltersState,
  type ExpandedState,
  type SortingState,
  type VisibilityState,
} from "@tanstack/react-table";
import {
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Loader2,
} from "lucide-react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import {
  DataTableColumnHeader,
  DataTableFacetedFilter,
  DataTableToolbar,
  type FacetOption,
} from "@/components/ui/data-table";
import type { TransactionRow } from "@/lib/store";
import type { SessionSummary } from "@/lib/sessions";
import { fmtDuration, fmtUsd } from "@/lib/format";
import { shortToolName } from "@/lib/tools";
import { cn } from "@/lib/cn";
import { subscribeRows } from "@/lib/rowsBus";
import { TurnDetail } from "@/components/TurnDetail";
import { ModelMixInline } from "@/components/ModelMixInline";
import {
  CacheReadCell,
  CacheWrite1hCell,
  CacheWrite5mCell,
  COLUMN_LABELS,
  CostCell,
  DurationCell,
  InTokensCell,
  ModelCell,
  OutTokensCell,
  RIGHT_ALIGNED_COLS,
  shortModel,
  StopDot,
  ToolsCell,
  txAccessors,
  WhenCell,
} from "@/components/table-cells";

type LeafRow = {
  kind: "leaf";
  id: string;
  tx: TransactionRow;
  posInGroup: number;
  groupSize: number;
};
type GroupRow = {
  kind: "group";
  id: string;
  sessionId: string | null;
  turns: number;
  firstTs: number;
  lastTs: number;
  cost: number;
  subRows: LeafRow[];
  models: Map<string, number>;
};

const MODEL_PALETTE = [
  "var(--color-chart-3)",
  "var(--color-chart-1)",
  "var(--color-chart-5)",
  "var(--color-chart-2)",
  "var(--color-chart-4)",
];
type UIRow = GroupRow | LeafRow;

function shortSession(id: string | null): string {
  return id ? id.slice(0, 8) : "(no session)";
}

// Build groups from pre-aggregated summaries. Header metrics (turns/cost/
// models) come from the DO's maintained session_summaries view; leaf rows
// are whatever turns we've actually loaded for that session (active
// sessions on page load, or any session the user has expanded).
function buildGroups(
  summaries: SessionSummary[],
  turnsBySession: Record<string, TransactionRow[]>,
): GroupRow[] {
  return summaries.map((s) => {
    const raw = turnsBySession[s.id] ?? [];
    // Newest-first so the most recent activity is visible without scrolling.
    // Filter out auxiliary requests (e.g. count_tokens) — they carry no model,
    // zero tokens, and zero cost, so they're just noise in the turn list.
    const sorted = [...raw]
      .filter((tx) => tx.model || tx.in_flight === 1)
      .sort((a, b) => b.ts - a.ts);
    const subRows: LeafRow[] = sorted.map((tx, i) => ({
      kind: "leaf",
      id: `l:${tx.tx_id}`,
      tx,
      posInGroup: i,
      groupSize: sorted.length,
    }));
    const models = new Map<string, number>();
    for (const m of s.models) models.set(m.model, m.turns);
    return {
      kind: "group",
      id: `g:${s.id}`,
      sessionId: s.id,
      turns: s.turns,
      firstTs: s.firstTs,
      lastTs: s.lastTs,
      cost: s.costUsd,
      subRows,
      models,
    } satisfies GroupRow;
  });
}

const isLeaf = (r: UIRow): r is LeafRow => r.kind === "leaf";

/** Filter fn for the tools column: row value is a space-separated short-name list. */
function toolsFilterFn(
  row: { getValue: (id: string) => unknown },
  id: string,
  value: string[],
): boolean {
  if (!Array.isArray(value) || value.length === 0) return true;
  const hay = String(row.getValue(id) ?? "").split(" ").filter(Boolean);
  if (hay.length === 0) return false;
  return value.some((f) => hay.includes(f));
}

const columns: ColumnDef<UIRow>[] = [
  {
    id: "expand",
    header: () => null,
    enableSorting: false,
    enableHiding: false,
    cell: ({ row }) => {
      if (isLeaf(row.original)) {
        const last = row.original.posInGroup === row.original.groupSize - 1;
        return (
          <span className="font-mono text-[var(--color-subtle-foreground)] text-[0.6875rem] leading-none select-none">
            {last ? "└─" : "├─"}
          </span>
        );
      }
      return (
        <button
          type="button"
          onClick={row.getToggleExpandedHandler()}
          className="flex items-center justify-center text-[var(--color-subtle-foreground)] hover:text-[var(--color-foreground)]"
          aria-label={row.getIsExpanded() ? "Collapse session" : "Expand session"}
        >
          {row.getIsExpanded() ? (
            <ChevronDown size={14} />
          ) : (
            <ChevronRight size={14} />
          )}
        </button>
      );
    },
  },
  {
    id: "dot",
    header: () => null,
    enableSorting: false,
    enableHiding: false,
    accessorFn: (r) => (isLeaf(r) ? txAccessors.stop(r.tx) : ""),
    filterFn: "arrIncludesSome",
    cell: ({ row }) => {
      if (!isLeaf(row.original)) return null;
      return <StopDot tx={row.original.tx} />;
    },
  },
  {
    accessorFn: (r) =>
      isLeaf(r) ? txAccessors.when(r.tx) : (r as GroupRow).lastTs,
    id: "when",
    header: ({ column }) => <DataTableColumnHeader column={column} title="When" />,
    sortingFn: "basic",
    cell: ({ row }) => {
      if (!isLeaf(row.original)) return null;
      return <WhenCell tx={row.original.tx} />;
    },
  },
  {
    accessorFn: (r) => (isLeaf(r) ? txAccessors.duration(r.tx) : 0),
    id: "duration",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Duration" align="right" />
    ),
    cell: ({ row }) =>
      isLeaf(row.original) ? <DurationCell tx={row.original.tx} /> : null,
  },
  {
    accessorFn: (r) => (isLeaf(r) ? txAccessors.model(r.tx) : ""),
    id: "model",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Model" />,
    filterFn: "arrIncludesSome",
    cell: ({ row }) =>
      isLeaf(row.original) ? <ModelCell tx={row.original.tx} /> : null,
  },
  {
    accessorFn: (r) => (isLeaf(r) ? txAccessors.in(r.tx) : 0),
    id: "in",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="In" align="right" />
    ),
    cell: ({ row }) =>
      isLeaf(row.original) ? <InTokensCell tx={row.original.tx} /> : null,
  },
  {
    accessorFn: (r) => (isLeaf(r) ? txAccessors.out(r.tx) : 0),
    id: "out",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Out" align="right" />
    ),
    cell: ({ row }) =>
      isLeaf(row.original) ? <OutTokensCell tx={row.original.tx} /> : null,
  },
  {
    accessorFn: (r) => (isLeaf(r) ? txAccessors.cache_read(r.tx) : 0),
    id: "cache_read",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Cache R" align="right" />
    ),
    cell: ({ row }) =>
      isLeaf(row.original) ? <CacheReadCell tx={row.original.tx} /> : null,
  },
  {
    accessorFn: (r) => (isLeaf(r) ? txAccessors.cache_5m(r.tx) : 0),
    id: "cache_5m",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="CW 5m" align="right" />
    ),
    cell: ({ row }) =>
      isLeaf(row.original) ? <CacheWrite5mCell tx={row.original.tx} /> : null,
  },
  {
    accessorFn: (r) => (isLeaf(r) ? txAccessors.cache_1h(r.tx) : 0),
    id: "cache_1h",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="CW 1h" align="right" />
    ),
    cell: ({ row }) =>
      isLeaf(row.original) ? <CacheWrite1hCell tx={row.original.tx} /> : null,
  },
  {
    id: "tools",
    header: "Tools",
    enableSorting: false,
    accessorFn: (r) => (isLeaf(r) ? txAccessors.tools(r.tx) : ""),
    filterFn: toolsFilterFn,
    cell: ({ row }) =>
      isLeaf(row.original) ? <ToolsCell tx={row.original.tx} /> : null,
  },
  {
    accessorFn: (r) => (isLeaf(r) ? txAccessors.cost(r.tx) : 0),
    id: "cost",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Cost" align="right" />
    ),
    cell: ({ row }) =>
      isLeaf(row.original) ? <CostCell tx={row.original.tx} /> : null,
  },
];

const HIDDEN_PER_GROUP = 5;
const SHOW_MORE_CHUNK = 5;

function parentIdFor(leaf: LeafRow): string {
  return `g:${leaf.tx.session_id ?? "__unlinked__"}`;
}

function initialExpanded(summaries: SessionSummary[]): Record<string, boolean> {
  const out: Record<string, boolean> = {};
  for (const s of summaries) {
    if (s.active) out[`g:${s.id}`] = true;
  }
  return out;
}

export default function RecentTurnsTable({
  summaries: initialSummaries,
  initialTurns,
}: {
  summaries: SessionSummary[];
  initialTurns: Record<string, TransactionRow[]>;
  /** Initial pill-windowed rows (merged into turnsBySession for active sessions). */
  windowedRows?: TransactionRow[];
}) {
  const [summaries, setSummaries] =
    React.useState<SessionSummary[]>(initialSummaries);
  const [turnsBySession, setTurnsBySession] = React.useState<
    Record<string, TransactionRow[]>
  >(() => ({ ...initialTurns }));
  const [expanded, setExpanded] = React.useState<ExpandedState>(() =>
    initialExpanded(initialSummaries),
  );
  const seenIds = React.useRef<Set<string>>(
    new Set(initialSummaries.map((s) => `g:${s.id}`)),
  );
  const prevActiveIds = React.useRef<Set<string>>(
    new Set(initialSummaries.filter((s) => s.active).map((s) => s.id)),
  );
  const [sorting, setSorting] = React.useState<SortingState>([]);
  const [globalFilter, setGlobalFilter] = React.useState("");
  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>(
    [],
  );
  const [columnVisibility, setColumnVisibility] =
    React.useState<VisibilityState>({});
  const [shownMap, setShownMap] = React.useState<Record<string, number>>({});
  const [expandedLeaves, setExpandedLeaves] = React.useState<Record<string, boolean>>({});
  const [loadingSessions, setLoadingSessions] = React.useState<Set<string>>(
    () => new Set(),
  );
  const toggleLeaf = React.useCallback((txId: string) => {
    setExpandedLeaves((p) => ({ ...p, [txId]: !p[txId] }));
  }, []);
  const shownFor = React.useCallback(
    (id: string) => shownMap[id] ?? HIDDEN_PER_GROUP,
    [shownMap],
  );
  const revealMore = React.useCallback((id: string, total: number) => {
    setShownMap((p) => {
      const cur = p[id] ?? HIDDEN_PER_GROUP;
      return { ...p, [id]: Math.min(total, cur + SHOW_MORE_CHUNK) };
    });
  }, []);
  const revealAll = React.useCallback((id: string, total: number) => {
    setShownMap((p) => ({ ...p, [id]: total }));
  }, []);
  const collapseSome = React.useCallback((id: string) => {
    setShownMap((p) => {
      const cur = p[id] ?? HIDDEN_PER_GROUP;
      return { ...p, [id]: Math.max(HIDDEN_PER_GROUP, cur - SHOW_MORE_CHUNK) };
    });
  }, []);
  const collapseAll = React.useCallback((id: string) => {
    setShownMap((p) => ({ ...p, [id]: HIDDEN_PER_GROUP }));
  }, []);

  // Poll the sidebar's sessions endpoint on the same cadence as Sidebar.tsx
  // (5 s) so the session headers stay current — turns count, cost, active
  // state, and ordering all come from summaries, not from raw rows.
  React.useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      if (typeof document !== "undefined" && document.hidden) return;
      try {
        const res = await fetch("/api/sessions.json", { cache: "no-store" });
        if (!res.ok || cancelled) return;
        const data = (await res.json()) as SessionSummary[];
        if (!cancelled && Array.isArray(data)) setSummaries(data);
      } catch {
        /* next event will retry */
      }
    };
    window.addEventListener("cm:turn-complete", tick);
    window.addEventListener("cm:session-end", tick);
    const onVis = () => {
      if (!document.hidden) void tick();
    };
    document.addEventListener("visibilitychange", onVis);
    return () => {
      cancelled = true;
      window.removeEventListener("cm:turn-complete", tick);
      window.removeEventListener("cm:session-end", tick);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, []);

  // Merge live windowed rows (from the /api/recent poll) into the sessions
  // we've already loaded. Collapsed sessions stay collapsed with no
  // turn-level data — nothing from the bus loads them implicitly.
  React.useEffect(() => {
    return subscribeRows((rows) => {
      // Build a set of in-flight tx_ids currently live on the bus so we can
      // prune orphaned virtuals that the stale timer already removed.
      const liveInflight = new Set<string>();
      for (const r of rows) {
        if (r.in_flight === 1) liveInflight.add(r.tx_id);
      }

      setTurnsBySession((prev) => {
        const next = { ...prev };
        const touched = new Set<string>();
        for (const r of rows) {
          if (!r.session_id) continue;
          if (!(r.session_id in next)) continue; // never auto-load collapsed
          touched.add(r.session_id);
        }
        // Also check loaded sessions for stale virtuals that vanished from the bus.
        for (const sid of Object.keys(next)) {
          if (next[sid]?.some((r) => r.in_flight === 1 && !liveInflight.has(r.tx_id))) {
            touched.add(sid);
          }
        }
        for (const sid of touched) {
          const byId = new Map<string, TransactionRow>();
          // Carry over non-virtual rows; only keep virtuals still live on the bus.
          for (const r of next[sid] ?? []) {
            if (r.in_flight === 1 && !liveInflight.has(r.tx_id)) continue;
            byId.set(r.tx_id, r);
          }
          for (const r of rows) {
            if (r.session_id === sid) byId.set(r.tx_id, r);
          }
          next[sid] = [...byId.values()];
        }
        return next;
      });
    });
  }, []);

  // Lazy-fetch turns for a session on first expand. Idempotent; subsequent
  // expands of the same session hit the in-memory cache.
  const ensureTurns = React.useCallback(
    (sessionId: string) => {
      setTurnsBySession((prev) => {
        if (sessionId in prev) return prev;
        setLoadingSessions((s) => {
          const next = new Set(s);
          next.add(sessionId);
          return next;
        });
        fetch(`/api/session/turns?id=${encodeURIComponent(sessionId)}`, {
          cache: "no-store",
        })
          .then((r) => (r.ok ? r.json() : Promise.reject(r.status)))
          .then((rows: TransactionRow[]) => {
            setTurnsBySession((p) => ({ ...p, [sessionId]: rows }));
          })
          .catch(() => {
            setTurnsBySession((p) => ({ ...p, [sessionId]: [] }));
          })
          .finally(() => {
            setLoadingSessions((s) => {
              if (!s.has(sessionId)) return s;
              const next = new Set(s);
              next.delete(sessionId);
              return next;
            });
          });
        return { ...prev, [sessionId]: [] };
      });
    },
    [],
  );

  const data = React.useMemo(
    () => buildGroups(summaries, turnsBySession),
    [summaries, turnsBySession],
  );

  // Auto-expand newly active / resumed sessions and auto-collapse sessions
  // that just ended.  Merged into a single effect so transition detection
  // reads refs synchronously *before* React 18 batches the queued
  // setExpanded updater — two separate effects race because the updater
  // runs after ALL effect bodies complete, by which time the second effect
  // has already mutated prevActiveIds.
  React.useEffect(() => {
    const nowActive = new Set(
      summaries.filter((s) => s.active).map((s) => s.id),
    );

    // --- detect transitions using current (pre-mutation) refs ---
    const toExpand: string[] = [];
    for (const s of summaries) {
      const gid = `g:${s.id}`;
      if (!s.active) continue;
      const isNew = !seenIds.current.has(gid);
      const isResumed = !isNew && !prevActiveIds.current.has(s.id);
      if (isNew || isResumed) toExpand.push(gid);
    }

    const toCollapse: string[] = [];
    for (const id of prevActiveIds.current) {
      if (!nowActive.has(id)) toCollapse.push(`g:${id}`);
    }

    // --- update refs (before the queued updater runs) ---
    for (const s of summaries) seenIds.current.add(`g:${s.id}`);
    prevActiveIds.current = nowActive;

    // --- apply expand / collapse ---
    if (toExpand.length === 0 && toCollapse.length === 0) return;
    setExpanded((prev) => {
      if (typeof prev !== "object" || prev == null) return prev;
      const p = prev as Record<string, boolean>;
      const next = { ...p };
      for (const gid of toExpand) next[gid] = true;
      for (const gid of toCollapse) next[gid] = false;
      return next;
    });
  }, [summaries]);

  // When a group is expanded but has no loaded turns, fire the lazy fetch.
  React.useEffect(() => {
    if (typeof expanded !== "object" || expanded == null) return;
    for (const [gid, open] of Object.entries(expanded)) {
      if (!open) continue;
      const sid = gid.startsWith("g:") ? gid.slice(2) : null;
      if (!sid) continue;
      if (!(sid in turnsBySession)) ensureTurns(sid);
    }
  }, [expanded, turnsBySession, ensureTurns]);

  const table = useReactTable({
    data: data as unknown as UIRow[],
    columns,
    state: {
      expanded,
      sorting,
      globalFilter,
      columnFilters,
      columnVisibility,
    },
    getRowId: (r) => r.id,
    getSubRows: (r) => (r.kind === "group" ? (r as GroupRow).subRows : undefined),
    getRowCanExpand: (row) => row.original.kind === "group",
    onExpandedChange: setExpanded,
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    onColumnFiltersChange: setColumnFilters,
    onColumnVisibilityChange: setColumnVisibility,
    getCoreRowModel: getCoreRowModel(),
    getExpandedRowModel: getExpandedRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    filterFromLeafRows: true,
    globalFilterFn: (row, _id, value) => {
      const q = String(value).toLowerCase().trim();
      if (!q) return true;
      const o = row.original as UIRow;
      if (isLeaf(o)) {
        const tx = o.tx;
        const hay = [
          tx.model ?? "",
          tx.session_id ?? "",
          tx.stop_reason ?? "",
          tx.tools_json ?? "",
        ]
          .join(" ")
          .toLowerCase();
        return hay.includes(q);
      }
      const hay = [
        o.sessionId ?? "",
        shortSession(o.sessionId),
      ]
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    },
  });

  const leafCount = table.getRowModel().rows.filter((r) => r.depth > 0).length;
  const groupCount = data.length;
  const allExpanded = table.getIsAllRowsExpanded();

  // Stable global model → color mapping, sorted by total frequency across
  // all visible groups. Ensures a model always gets the same swatch color
  // regardless of which row it appears in.
  const modelColors = React.useMemo(() => {
    const counts = new Map<string, number>();
    for (const g of data) {
      for (const [m, n] of g.models) counts.set(m, (counts.get(m) ?? 0) + n);
    }
    const sorted = [...counts.entries()].sort((a, b) => b[1] - a[1]);
    const map = new Map<string, string>();
    sorted.forEach(([m], i) => {
      map.set(m, MODEL_PALETTE[i % MODEL_PALETTE.length]);
    });
    return map;
  }, [data]);

  // Pre-compute faceted filter options from the full leaf set. Using a stable
  // list (not TanStack's per-render facet counts) avoids flicker when the row
  // set changes rapidly via the bus.
  const allLeaves = React.useMemo<TransactionRow[]>(() => {
    const out: TransactionRow[] = [];
    for (const g of data) {
      for (const l of g.subRows) out.push(l.tx);
    }
    return out;
  }, [data]);

  const modelOptions = React.useMemo<FacetOption[]>(() => {
    const counts = new Map<string, number>();
    for (const tx of allLeaves) {
      const m = tx.model;
      if (!m) continue;
      counts.set(m, (counts.get(m) ?? 0) + 1);
    }
    return [...counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([m, n]) => ({ value: m, label: shortModel(m), count: n }));
  }, [allLeaves]);

  const stopOptions = React.useMemo<FacetOption[]>(() => {
    const counts = new Map<string, number>();
    for (const tx of allLeaves) {
      const key = txAccessors.stop(tx);
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    return [...counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([s, n]) => ({ value: s, label: s, count: n }));
  }, [allLeaves]);

  const toolOptions = React.useMemo<FacetOption[]>(() => {
    const counts = new Map<string, number>();
    for (const tx of allLeaves) {
      const arr: string[] = tx.tools_json ? JSON.parse(tx.tools_json) : [];
      for (const t of arr.map(shortToolName)) {
        counts.set(t, (counts.get(t) ?? 0) + 1);
      }
    }
    return [...counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 40)
      .map(([t, n]) => ({ value: t, label: t, count: n }));
  }, [allLeaves]);

  return (
    <section className="card overflow-hidden">
      <DataTableToolbar
        table={table}
        searchValue={globalFilter}
        onSearchChange={setGlobalFilter}
        placeholder="Filter model, tool, session…"
        columnLabels={COLUMN_LABELS}
        leading={
          <>
            <h2 className="text-sm font-medium">Recent turns</h2>
            <span className="text-xs text-[var(--color-subtle-foreground)] tabular-nums">
              {leafCount} turns · {groupCount} sessions
            </span>
            {modelColors.size > 0 && (
              <>
                <span className="text-[var(--color-subtle-foreground)] text-xs">
                  ·
                </span>
                <ul className="flex items-center gap-2.5 flex-wrap">
                  {[...modelColors.entries()].map(([m, c]) => (
                    <li
                      key={m}
                      className="inline-flex items-center gap-1.5 text-[0.6875rem] text-[var(--color-muted-foreground)] font-mono tabular-nums"
                    >
                      <span
                        className="inline-block w-2 h-2 rounded-sm"
                        style={{ background: c }}
                      />
                      {m}
                    </li>
                  ))}
                </ul>
              </>
            )}
          </>
        }
        filters={
          <>
            {modelOptions.length > 1 && (
              <DataTableFacetedFilter
                column={table.getColumn("model")}
                title="Model"
                options={modelOptions}
              />
            )}
            {stopOptions.length > 1 && (
              <DataTableFacetedFilter
                column={table.getColumn("dot")}
                title="Status"
                options={stopOptions}
              />
            )}
            {toolOptions.length > 0 && (
              <DataTableFacetedFilter
                column={table.getColumn("tools")}
                title="Tools"
                options={toolOptions}
                width="w-[16rem]"
              />
            )}
          </>
        }
        trailing={
          <Button
            variant="ghost"
            size="sm"
            className="h-7 text-xs font-normal text-[var(--color-muted-foreground)] hover:text-foreground hover:bg-[var(--color-card-elevated)]"
            onClick={() => table.toggleAllRowsExpanded(!allExpanded)}
          >
            {allExpanded ? "Collapse all" : "Expand all"}
          </Button>
        }
      />

      {summaries.length === 0 ? (
        <p className="px-5 py-10 text-[var(--color-muted-foreground)] text-sm text-center">
          No transactions yet.
        </p>
      ) : (
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((hg) => (
              <TableRow key={hg.id} className="border-t-0 hover:bg-transparent">
                {hg.headers.map((h) => {
                  const isRight = RIGHT_ALIGNED_COLS.has(h.id);
                  return (
                    <TableHead
                      key={h.id}
                      className={cn(
                        h.id === "expand" && "w-4 pl-4 pr-1",
                        h.id === "dot" && "w-3 px-2",
                        h.id === "when" && "w-16",
                        h.id === "duration" && "w-16",
                        isRight && "text-right",
                      )}
                    >
                      {h.isPlaceholder
                        ? null
                        : flexRender(h.column.columnDef.header, h.getContext())}
                    </TableHead>
                  );
                })}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {(() => {
              const out: React.ReactNode[] = [];
              const allRows = table.getRowModel().rows;
              const visibleColCount = table.getVisibleLeafColumns().length;
              const groupSizes = new Map<string, number>();
              for (const r of allRows) {
                if (isLeaf(r.original)) {
                  const pid = parentIdFor(r.original);
                  groupSizes.set(pid, (groupSizes.get(pid) ?? 0) + 1);
                }
              }
              const shownPerGroup = new Map<string, number>();
              for (const row of allRows) {
                if (!isLeaf(row.original)) {
                  const g = row.original;
                  /* ---- build visible-column list for session header ---- */
                  const visCols = table.getVisibleLeafColumns().map((c) => c.id);
                  out.push(
                    <TableRow
                      key={row.id}
                      className="bg-[var(--color-card-elevated)]/40 cursor-pointer"
                      onClick={() => {
                        setExpanded((prev) => {
                          const p = (prev ?? {}) as Record<string, boolean>;
                          return { ...p, [row.id]: !p[row.id] };
                        });
                      }}
                    >
                      {visCols.map((colId) => {
                        switch (colId) {
                          /* ── chevron ── */
                          case "expand":
                            return (
                              <TableCell key={colId} className="pl-4 pr-1 w-4 align-middle">
                                {row.getIsExpanded() ? (
                                  <ChevronDown
                                    size={14}
                                    className="text-[var(--color-subtle-foreground)]"
                                  />
                                ) : (
                                  <ChevronRight
                                    size={14}
                                    className="text-[var(--color-subtle-foreground)]"
                                  />
                                )}
                              </TableCell>
                            );
                          /* ── dot → empty ── */
                          case "dot":
                            return <TableCell key={colId} className="px-2 w-3" />;
                          /* ── when → session id + turns + duration + model mix (spans through cache cols) ── */
                          case "when":
                            return (
                              <TableCell key={colId} className="py-2" colSpan={
                                (() => {
                                  const from = visCols.indexOf("when");
                                  const spanIds = ["when", "duration", "model", "in", "out", "cache_read", "cache_5m", "cache_1h"];
                                  let count = 0;
                                  for (let i = from; i < visCols.length; i++) {
                                    if (spanIds.includes(visCols[i])) count++;
                                    else break;
                                  }
                                  return Math.max(1, count);
                                })()
                              }>
                                <div className="flex items-center gap-2 text-xs">
                                  {g.sessionId ? (
                                    <a
                                      href={`/session/${g.sessionId}`}
                                      onClick={(e) => e.stopPropagation()}
                                      className="font-mono text-[var(--color-volume)] hover:underline hover:text-[var(--color-foreground)] inline-flex items-center gap-1"
                                      title="Open session detail"
                                    >
                                      <code>{shortSession(g.sessionId)}</code>
                                      <ChevronRight size={11} className="opacity-60" />
                                    </a>
                                  ) : (
                                    <span className="text-[var(--color-subtle-foreground)] italic">
                                      (no session)
                                    </span>
                                  )}
                                  <span className="inline-block w-[4ch] text-right font-mono tabular-nums text-[var(--color-muted-foreground)]">
                                    {g.turns}
                                  </span>
                                  <span className="text-[var(--color-muted-foreground)]">
                                    {g.turns === 1 ? "turn" : "turns"}
                                  </span>
                                  {g.lastTs > g.firstTs && (
                                    <span className="inline-block w-[6ch] text-right font-mono tabular-nums text-[var(--color-muted-foreground)]">
                                      {fmtDuration(g.lastTs - g.firstTs)}
                                    </span>
                                  )}
                                  {g.models.size > 0 && (
                                    <ModelMixInline
                                      models={g.models}
                                      turns={g.turns}
                                      colorFor={(m) =>
                                        modelColors.get(m) ?? MODEL_PALETTE[0]
                                      }
                                    />
                                  )}
                                </div>
                              </TableCell>
                            );
                          /* ── skip columns consumed by model colSpan ── */
                          case "in":
                          case "out":
                          case "cache_read":
                          case "cache_5m":
                          case "cache_1h":
                            return null;
                          /* ── cost → session total cost ── */
                          case "cost":
                            return (
                              <TableCell key={colId} className="py-2">
                                {g.cost > 0 && (
                                  <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-money)]">
                                    {fmtUsd(g.cost)}
                                  </span>
                                )}
                              </TableCell>
                            );
                          /* ── spanned by the when cell's colSpan ── */
                          case "duration":
                          case "model":
                            return null;
                          /* ── every other column → empty cell ── */
                          default:
                            return <TableCell key={colId} />;
                        }
                      })}
                    </TableRow>,
                  );
                  const sidForLoad = g.sessionId;
                  const open = row.getIsExpanded();
                  const subCount = (g.subRows ?? []).length;
                  if (open && subCount === 0 && sidForLoad) {
                    out.push(
                      <TableRow
                        key={`${row.id}:load`}
                        className="hover:bg-transparent"
                      >
                        <TableCell
                          colSpan={visibleColCount}
                          className="py-3 text-center text-xs text-[var(--color-subtle-foreground)]"
                        >
                          <span className="inline-flex items-center gap-2">
                            <Loader2 size={12} className="animate-spin" />
                            {loadingSessions.has(sidForLoad)
                              ? "Loading turns…"
                              : "No turns loaded"}
                          </span>
                        </TableCell>
                      </TableRow>,
                    );
                  }
                  continue;
                }
                const pid = parentIdFor(row.original);
                const nShown = (shownPerGroup.get(pid) ?? 0) + 1;
                shownPerGroup.set(pid, nShown);
                const total = groupSizes.get(pid) ?? 0;
                const limit = shownFor(pid);
                if (nShown <= limit) {
                  const leafTx = (row.original as LeafRow).tx;
                  const isLeafOpen = !!expandedLeaves[leafTx.tx_id];
                  out.push(
                    <TableRow
                      key={row.id}
                      className="cursor-pointer"
                      onClick={() => toggleLeaf(leafTx.tx_id)}
                    >
                      {row.getVisibleCells().map((cell) => (
                        <TableCell
                          key={cell.id}
                          className={cn(
                            cell.column.id === "expand" && "pl-4 pr-1 w-4",
                            cell.column.id === "dot" && "px-2 w-3",
                            cell.column.id === "when" && "w-14",
                          )}
                        >
                          {flexRender(
                            cell.column.columnDef.cell,
                            cell.getContext(),
                          )}
                        </TableCell>
                      ))}
                    </TableRow>,
                  );
                  if (isLeafOpen) {
                    out.push(
                      <TableRow
                        key={`${row.id}:detail`}
                        className="hover:bg-transparent"
                      >
                        <TableCell
                          colSpan={visibleColCount}
                          className="bg-[var(--color-background)]/40 px-6 py-4"
                        >
                          <TurnDetail tx={leafTx} />
                        </TableCell>
                      </TableRow>,
                    );
                  }
                }
                // Control row after the last visible row of this group.
                // Renders 3 buttons by default — Show 5 | Collapse | Show all —
                // and splits into 4 when both collapse actions are distinct:
                // [Show 5] [Collapse 5] [Show X more] [Collapse all].
                if (
                  nShown === Math.min(limit, total) &&
                  total > HIDDEN_PER_GROUP
                ) {
                  const remaining = total - limit;
                  const showChunk = Math.min(SHOW_MORE_CHUNK, remaining);
                  const collapseChunk = Math.min(
                    SHOW_MORE_CHUNK,
                    limit - HIDDEN_PER_GROUP,
                  );
                  const canShowChunk = remaining > 0;
                  const canShowAll = remaining > 0;
                  const canCollapse = limit > HIDDEN_PER_GROUP;
                  // Split into a separate chunk-collapse button only when it
                  // would land at a distinct limit (>HIDDEN after collapsing).
                  const splitCollapse =
                    limit - HIDDEN_PER_GROUP > SHOW_MORE_CHUNK;
                  type Btn = {
                    key: string;
                    label: string;
                    icon: React.ReactNode;
                    onClick: () => void;
                    enabled: boolean;
                  };
                  const buttons: Btn[] = [
                    {
                      key: "show-chunk",
                      label: `Show ${canShowChunk ? showChunk : SHOW_MORE_CHUNK} more`,
                      icon: <ChevronDown size={12} />,
                      onClick: () => revealMore(pid, total),
                      enabled: canShowChunk,
                    },
                    splitCollapse
                      ? {
                          key: "collapse-chunk",
                          label: `Collapse ${collapseChunk}`,
                          icon: <ChevronUp size={12} />,
                          onClick: () => collapseSome(pid),
                          enabled: canCollapse,
                        }
                      : {
                          key: "collapse-one",
                          label: canCollapse ? "Collapse" : "Collapse session",
                          icon: <ChevronUp size={12} />,
                          // At default limit there's nothing to shrink back
                          // to within the group — so pressing Collapse folds
                          // the whole session group instead.
                          onClick: canCollapse
                            ? () => collapseAll(pid)
                            : () =>
                                setExpanded((prev) => {
                                  const next = {
                                    ...(prev as Record<string, boolean>),
                                  };
                                  next[pid] = false;
                                  return next;
                                }),
                          enabled: true,
                        },
                    {
                      key: "show-all",
                      label: canShowAll
                        ? `Show ${remaining} more ${remaining === 1 ? "turn" : "turns"}`
                        : "Show all",
                      icon: <ChevronDown size={12} />,
                      onClick: () => revealAll(pid, total),
                      enabled: canShowAll,
                    },
                  ];
                  if (splitCollapse) {
                    buttons.push({
                      key: "collapse-all",
                      label: "Collapse all",
                      icon: <ChevronUp size={12} />,
                      onClick: () => collapseAll(pid),
                      enabled: canCollapse,
                    });
                  }
                  out.push(
                    <TableRow
                      key={`ctl:${pid}:${limit}`}
                      className="bg-[var(--color-background)]/40 hover:bg-transparent"
                    >
                      <TableCell colSpan={visibleColCount} className="p-0">
                        <div
                          className="grid divide-x divide-[var(--color-border)]"
                          style={{
                            gridTemplateColumns: `repeat(${buttons.length}, minmax(0, 1fr))`,
                          }}
                        >
                          {buttons.map((b) => (
                            <button
                              key={b.key}
                              type="button"
                              onClick={b.enabled ? b.onClick : undefined}
                              disabled={!b.enabled}
                              className={cn(
                                "py-1.5 text-center text-xs",
                                b.enabled
                                  ? "text-[var(--color-subtle-foreground)] hover:text-[var(--color-foreground)] hover:bg-[var(--color-card-elevated)]/60 cursor-pointer"
                                  : "text-[var(--color-subtle-foreground)]/30 cursor-not-allowed",
                              )}
                            >
                              <span className="inline-flex items-center gap-1.5">
                                {b.icon}
                                {b.label}
                              </span>
                            </button>
                          ))}
                        </div>
                      </TableCell>
                    </TableRow>,
                  );
                }
              }
              if (out.length === 0) {
                out.push(
                  <TableRow key="empty" className="hover:bg-transparent">
                    <TableCell
                      colSpan={visibleColCount}
                      className="py-10 text-center text-xs text-[var(--color-muted-foreground)]"
                    >
                      No turns match the current filters.
                    </TableCell>
                  </TableRow>,
                );
              }
              return out;
            })()}
          </TableBody>
        </Table>
      )}
    </section>
  );
}

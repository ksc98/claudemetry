import * as React from "react";
import {
  flexRender,
  getCoreRowModel,
  getExpandedRowModel,
  getFilteredRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type ExpandedState,
  type SortingState,
  type VisibilityState,
  type Row,
} from "@tanstack/react-table";
import {
  ChevronDown,
  ChevronRight,
  ChevronUp,
  SlidersHorizontal,
  Search,
  Sparkles,
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
import { Input } from "@/components/ui/input";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import type { TransactionRow } from "@/lib/store";
import {
  estimateCostUsd,
  fmtAgo,
  fmtDuration,
  fmtInt,
  fmtUsd,
} from "@/lib/format";
import { shortToolName } from "@/lib/tools";
import { stopDotClass } from "@/lib/stop";
import { cn } from "@/lib/cn";
import { subscribeRows } from "@/lib/rowsBus";

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
function shortModel(m: string | null | undefined): string {
  if (!m) return "—";
  return m.replace(/-\d{8}$/, "").replace(/^claude-/, "");
}

function buildGroups(rows: TransactionRow[]): GroupRow[] {
  const groups: GroupRow[] = [];
  const idx = new Map<string, number>();
  for (const r of rows) {
    const sid = r.session_id ?? "__unlinked__";
    let i = idx.get(sid);
    if (i === undefined) {
      groups.push({
        kind: "group",
        id: `g:${sid}`,
        sessionId: r.session_id,
        turns: 0,
        firstTs: r.ts,
        lastTs: r.ts,
        cost: 0,
        subRows: [],
        models: new Map(),
      });
      i = groups.length - 1;
      idx.set(sid, i);
    }
    const g = groups[i];
    g.subRows.push({
      kind: "leaf",
      id: `l:${r.tx_id}`,
      tx: r,
      posInGroup: 0,
      groupSize: 0,
    });
    g.turns += 1;
    g.firstTs = Math.min(g.firstTs, r.ts);
    g.lastTs = Math.max(g.lastTs, r.ts);
    g.cost += estimateCostUsd(r);
    if (r.model) {
      const short = r.model.replace(/-\d{8}$/, "").replace(/^claude-/, "");
      g.models.set(short, (g.models.get(short) ?? 0) + 1);
    }
  }
  for (const g of groups) {
    g.subRows.forEach((lr, i) => {
      lr.posInGroup = i;
      lr.groupSize = g.subRows.length;
    });
  }
  return groups;
}

const isLeaf = (r: UIRow): r is LeafRow => r.kind === "leaf";

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
    cell: ({ row }) => {
      if (!isLeaf(row.original)) return null;
      const r = row.original.tx;
      return (
        <span
          className={cn("dot", stopDotClass(r.stop_reason))}
          title={r.stop_reason ?? "—"}
          style={{ marginRight: 0 }}
        />
      );
    },
  },
  {
    accessorFn: (r) => (isLeaf(r) ? r.tx.ts : (r as GroupRow).lastTs),
    id: "when",
    header: "When",
    sortingFn: "basic",
    cell: ({ row }) => {
      if (!isLeaf(row.original)) return null;
      const r = row.original.tx;
      return (
        <span
          data-ts={r.ts}
          className="text-[var(--color-muted-foreground)] font-mono text-xs tabular-nums whitespace-nowrap"
        >
          {fmtAgo(r.ts)}
        </span>
      );
    },
  },
  {
    accessorFn: (r) => (isLeaf(r) ? r.tx.model ?? "" : ""),
    id: "model",
    header: "Model",
    cell: ({ row }) => {
      if (!isLeaf(row.original)) return null;
      const tx = row.original.tx;
      const m = tx.model;
      const thought = (tx.thinking_blocks ?? 0) > 0;
      const budget = tx.thinking_budget ?? null;
      return (
        <span className="font-mono text-xs inline-flex items-center gap-1.5 whitespace-nowrap">
          <span title={m ?? "—"}>{shortModel(m)}</span>
          {(thought || budget != null) && (
            <span
              className="inline-flex items-center gap-0.5 text-[var(--color-chart-4)]"
              title={`extended thinking${thought ? ` · ${tx.thinking_blocks} block${(tx.thinking_blocks ?? 0) > 1 ? "s" : ""}` : " budget set, not used this turn"}${budget ? ` · budget ${fmtInt(budget)}` : ""}`}
            >
              <Sparkles size={10} className="shrink-0" aria-label="extended thinking" />
              {budget != null && (
                <span className="tabular-nums text-[0.625rem]">
                  {budget >= 1000 ? `${Math.round(budget / 1000)}k` : budget}
                </span>
              )}
            </span>
          )}
        </span>
      );
    },
  },
  {
    accessorFn: (r) => (isLeaf(r) ? r.tx.input_tokens : 0),
    id: "in",
    header: "In",
    cell: ({ row }) =>
      isLeaf(row.original) ? (
        <span className="block text-right font-mono text-xs tabular-nums">
          {fmtInt(row.original.tx.input_tokens)}
        </span>
      ) : null,
  },
  {
    accessorFn: (r) => (isLeaf(r) ? r.tx.output_tokens : 0),
    id: "out",
    header: "Out",
    cell: ({ row }) => {
      if (!isLeaf(row.original)) return null;
      const tx = row.original.tx;
      const mx = tx.max_tokens ?? 0;
      const util = mx > 0 ? tx.output_tokens / mx : 0;
      const atCeiling = util >= 0.95;
      return (
        <span
          className={cn(
            "block text-right font-mono text-xs tabular-nums",
            atCeiling && "text-[var(--color-warn)] font-medium",
          )}
          title={
            mx > 0
              ? `${fmtInt(tx.output_tokens)} / ${fmtInt(mx)} max (${(util * 100).toFixed(0)}%)`
              : undefined
          }
        >
          {fmtInt(tx.output_tokens)}
        </span>
      );
    },
  },
  {
    accessorFn: (r) => (isLeaf(r) ? r.tx.cache_read : 0),
    id: "cache_read",
    header: "Cache R",
    cell: ({ row }) =>
      isLeaf(row.original) ? (
        <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-volume)]/80">
          {fmtInt(row.original.tx.cache_read)}
        </span>
      ) : null,
  },
  {
    accessorFn: (r) => (isLeaf(r) ? r.tx.cache_creation : 0),
    id: "cache_creation",
    header: "Cache W",
    cell: ({ row }) => {
      if (!isLeaf(row.original)) return null;
      const tx = row.original.tx;
      const total = tx.cache_creation;
      const w5m = tx.cache_creation_5m ?? 0;
      const w1h = tx.cache_creation_1h ?? 0;
      const split = w5m + w1h > 0;
      return (
        <span
          className="block text-right font-mono text-xs tabular-nums text-[var(--color-volume)]/55"
          title={
            split
              ? `${fmtInt(w5m)} × 5m · ${fmtInt(w1h)} × 1h`
              : total > 0
                ? `${fmtInt(total)} cache writes`
                : undefined
          }
        >
          {fmtInt(total)}
        </span>
      );
    },
  },
  {
    accessorFn: (r) => (isLeaf(r) ? r.tx.elapsed_ms : 0),
    id: "latency",
    header: "Latency",
    cell: ({ row }) =>
      isLeaf(row.original) ? (
        <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-subtle-foreground)]">
          {fmtDuration(row.original.tx.elapsed_ms)}
        </span>
      ) : null,
  },
  {
    id: "tools",
    header: "Tools",
    enableSorting: false,
    accessorFn: (r) => {
      if (!isLeaf(r)) return "";
      const arr: string[] = r.tx.tools_json ? JSON.parse(r.tx.tools_json) : [];
      return arr.map(shortToolName).join(" ");
    },
    cell: ({ row }) => {
      if (!isLeaf(row.original)) return null;
      const raw: string[] = row.original.tx.tools_json
        ? JSON.parse(row.original.tx.tools_json)
        : [];
      const tools = raw.map(shortToolName);
      if (tools.length === 0)
        return (
          <span className="text-[var(--color-subtle-foreground)] text-xs">
            —
          </span>
        );
      return (
        <div className="flex flex-wrap gap-1 max-w-[22rem]">
          {tools.slice(0, 3).map((t) => (
            <span key={t} className="chip" title={t}>
              {t.length > 22 ? t.slice(0, 22) + "…" : t}
            </span>
          ))}
          {tools.length > 3 && (
            <span className="chip" title={tools.slice(3).join(", ")}>
              +{tools.length - 3}
            </span>
          )}
        </div>
      );
    },
  },
  {
    accessorFn: (r) => (isLeaf(r) ? estimateCostUsd(r.tx) : 0),
    id: "cost",
    header: "Cost",
    cell: ({ row }) =>
      isLeaf(row.original) ? (
        <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-money)]">
          {fmtUsd(estimateCostUsd(row.original.tx))}
        </span>
      ) : null,
  },
];

const COLUMN_LABELS: Record<string, string> = {
  when: "When",
  model: "Model",
  in: "Input tokens",
  out: "Output tokens",
  cache_read: "Cache read",
  cache_creation: "Cache write",
  latency: "Latency",
  tools: "Tools",
  cost: "Cost",
};

// Columns whose numeric/cost content is right-aligned. Header cell wrappers
// use these to match with the data cells so the text actually lines up.
const RIGHT_ALIGNED_COLS = new Set([
  "in",
  "out",
  "cache_read",
  "cache_creation",
  "latency",
  "cost",
]);

const ACTIVE_WINDOW_MS = 3 * 60_000;
const HIDDEN_PER_GROUP = 5;
const SHOW_MORE_CHUNK = 5;

function parentIdFor(leaf: LeafRow): string {
  return `g:${leaf.tx.session_id ?? "__unlinked__"}`;
}

function initialExpanded(rows: TransactionRow[]): Record<string, boolean> {
  const now = Date.now();
  const groups = buildGroups(rows);
  const out: Record<string, boolean> = {};
  for (let i = 0; i < groups.length; i++) {
    const g = groups[i];
    if (i === 0 || now - g.lastTs < ACTIVE_WINDOW_MS) out[g.id] = true;
  }
  return out;
}

export default function RecentTurnsTable({
  initialRows,
}: {
  initialRows: TransactionRow[];
}) {
  const [rows, setRows] = React.useState<TransactionRow[]>(initialRows);
  const [expanded, setExpanded] = React.useState<ExpandedState>(() =>
    initialExpanded(initialRows),
  );
  const seenIds = React.useRef<Set<string>>(
    new Set(buildGroups(initialRows).map((g) => g.id)),
  );
  const [sorting, setSorting] = React.useState<SortingState>([]);
  const [globalFilter, setGlobalFilter] = React.useState("");
  const [columnVisibility, setColumnVisibility] =
    React.useState<VisibilityState>({});
  const [shownMap, setShownMap] = React.useState<Record<string, number>>({});
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

  React.useEffect(() => subscribeRows(setRows), []);

  const data = React.useMemo(() => buildGroups(rows), [rows]);

  React.useEffect(() => {
    setExpanded((prev) => {
      if (typeof prev !== "object" || prev == null) return prev;
      const now = Date.now();
      const next = { ...(prev as Record<string, boolean>) };
      let changed = false;
      for (const g of data) {
        if (seenIds.current.has(g.id)) continue;
        seenIds.current.add(g.id);
        if (now - g.lastTs < ACTIVE_WINDOW_MS) {
          next[g.id] = true;
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [data]);

  const table = useReactTable({
    data: data as unknown as UIRow[],
    columns,
    state: { expanded, sorting, globalFilter, columnVisibility },
    getRowId: (r) => r.id,
    getSubRows: (r) => (r.kind === "group" ? (r as GroupRow).subRows : undefined),
    onExpandedChange: setExpanded,
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
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
  const visibleLeafColumns = table
    .getAllLeafColumns()
    .filter((c) => c.getCanHide());

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

  return (
    <section className="card overflow-hidden">
      <div className="flex items-center justify-between gap-3 px-5 py-3 border-b border-[var(--color-border)]">
        <div className="flex items-center gap-3 flex-wrap">
          <h2 className="text-sm font-medium">Recent turns</h2>
          <span className="text-xs text-[var(--color-subtle-foreground)] tabular-nums">
            {leafCount} turns · {groupCount} sessions
          </span>
          {modelColors.size > 0 && (
            <>
              <span className="text-[var(--color-subtle-foreground)] text-xs">·</span>
              <ul className="flex items-center gap-2.5">
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
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search
              size={12}
              className="absolute left-2 top-1/2 -translate-y-1/2 text-[var(--color-subtle-foreground)] pointer-events-none"
            />
            <Input
              placeholder="Filter model, tool, session…"
              value={globalFilter}
              onChange={(e) => setGlobalFilter(e.currentTarget.value)}
              className="h-7 w-56 pl-7 text-xs"
            />
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => table.toggleAllRowsExpanded(!allExpanded)}
          >
            {allExpanded ? "Collapse all" : "Expand all"}
          </Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm" aria-label="Column visibility">
                <SlidersHorizontal size={12} />
                Columns
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel>Toggle columns</DropdownMenuLabel>
              <DropdownMenuSeparator />
              {visibleLeafColumns.map((col) => (
                <DropdownMenuCheckboxItem
                  key={col.id}
                  checked={col.getIsVisible()}
                  onCheckedChange={(v) => col.toggleVisibility(!!v)}
                >
                  {COLUMN_LABELS[col.id] ?? col.id}
                </DropdownMenuCheckboxItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>

      {rows.length === 0 ? (
        <p className="px-5 py-10 text-[var(--color-muted-foreground)] text-sm text-center">
          No transactions yet.
        </p>
      ) : (
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((hg) => (
              <TableRow key={hg.id} className="border-t-0 hover:bg-transparent">
                {hg.headers.map((h) => {
                  const canSort = h.column.getCanSort();
                  const dir = h.column.getIsSorted();
                  const isRight = RIGHT_ALIGNED_COLS.has(h.id);
                  return (
                    <TableHead
                      key={h.id}
                      className={cn(
                        h.id === "expand" && "w-4 pl-4 pr-1",
                        h.id === "dot" && "w-3 px-2",
                        h.id === "when" && "w-14",
                        isRight && "text-right",
                        canSort && "cursor-pointer select-none",
                      )}
                      onClick={
                        canSort ? h.column.getToggleSortingHandler() : undefined
                      }
                    >
                      {h.isPlaceholder ? null : (
                        <span
                          className={cn(
                            "flex items-center gap-1",
                            isRight && "justify-end",
                          )}
                        >
                          {flexRender(
                            h.column.columnDef.header,
                            h.getContext(),
                          )}
                          {canSort && dir && (
                            <span className="text-[var(--color-foreground)]">
                              {dir === "asc" ? "↑" : "↓"}
                            </span>
                          )}
                        </span>
                      )}
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
                  out.push(
                    <TableRow
                      key={row.id}
                      className="bg-[var(--color-card-elevated)]/40 cursor-pointer"
                      onClick={row.getToggleExpandedHandler()}
                    >
                      <TableCell className="pl-4 pr-1 w-4 align-middle">
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
                      <TableCell
                        colSpan={Math.max(1, visibleColCount - 1)}
                        className="py-2"
                      >
                        <div className="flex items-center gap-3 text-xs">
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
                          <span className="text-[var(--color-muted-foreground)] tabular-nums">
                            {g.turns} {g.turns === 1 ? "turn" : "turns"}
                          </span>
                          {g.lastTs > g.firstTs && (
                            <>
                              <span className="text-[var(--color-subtle-foreground)]">
                                ·
                              </span>
                              <span className="text-[var(--color-muted-foreground)] tabular-nums">
                                {fmtDuration(g.lastTs - g.firstTs)}
                              </span>
                            </>
                          )}
                          {g.cost > 0 && (
                            <>
                              <span className="text-[var(--color-subtle-foreground)]">
                                ·
                              </span>
                              <span className="text-[var(--color-money)] tabular-nums font-mono">
                                {fmtUsd(g.cost)}
                              </span>
                            </>
                          )}
                          {g.models.size > 0 && (
                            <>
                              <span className="text-[var(--color-subtle-foreground)]">
                                ·
                              </span>
                              <ModelMixInline
                                models={g.models}
                                turns={g.turns}
                                colorFor={(m) =>
                                  modelColors.get(m) ?? MODEL_PALETTE[0]
                                }
                              />
                            </>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>,
                  );
                  continue;
                }
                const pid = parentIdFor(row.original);
                const nShown = (shownPerGroup.get(pid) ?? 0) + 1;
                shownPerGroup.set(pid, nShown);
                const total = groupSizes.get(pid) ?? 0;
                const limit = shownFor(pid);
                if (nShown <= limit) {
                  out.push(
                    <TableRow key={row.id}>
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
              return out;
            })()}
          </TableBody>
        </Table>
      )}
    </section>
  );
}

function ModelMixInline({
  models,
  turns,
  colorFor,
}: {
  models: Map<string, number>;
  turns: number;
  colorFor: (model: string) => string;
}) {
  const sorted = React.useMemo(
    () => [...models.entries()].sort((a, b) => b[1] - a[1]),
    [models],
  );
  const title = sorted
    .map(([m, n]) => `${m} ×${n} (${Math.round((n / turns) * 100)}%)`)
    .join("\n");
  return (
    <span
      className="inline-flex overflow-hidden rounded-sm bg-[var(--color-border)]"
      style={{ width: 72, height: 6 }}
      aria-hidden="true"
      title={title}
      onClick={(e) => e.stopPropagation()}
    >
      {sorted.map(([label, n]) => (
        <span
          key={label}
          style={{
            flex: `${Math.max((n / turns) * 100, 2)} 0 0`,
            minWidth: 1,
            background: colorFor(label),
          }}
        />
      ))}
    </span>
  );
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
type _RowType = Row<UIRow>;

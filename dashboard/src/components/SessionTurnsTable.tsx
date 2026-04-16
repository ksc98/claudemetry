import * as React from "react";
import {
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
  type VisibilityState,
} from "@tanstack/react-table";
import {
  ChevronDown,
  ChevronRight,
  Loader2,
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
import { TurnDetail } from "@/components/TurnDetail";

type TurnRow = {
  tx: TransactionRow;
  index: number; // 1-based turn position within this session
};

function shortModel(m: string | null | undefined): string {
  if (!m) return "—";
  return m.replace(/-\d{8}$/, "").replace(/^claude-/, "");
}


function toTurns(rows: TransactionRow[], sessionId: string): TurnRow[] {
  return rows
    .filter((r) => r.session_id === sessionId)
    // Filter out auxiliary requests (e.g. count_tokens) — zero tokens, zero cost.
    .filter((r) => r.model || r.in_flight === 1)
    .sort((a, b) => a.ts - b.ts)
    .map((tx, i) => ({ tx, index: i + 1 }));
}

const columns: ColumnDef<TurnRow>[] = [
  {
    id: "expand",
    header: () => null,
    enableSorting: false,
    enableHiding: false,
    cell: () => null, // rendered manually below to support row-level onClick
  },
  {
    accessorFn: (r) => r.index,
    id: "turn",
    header: "#",
    sortingFn: "basic",
    cell: ({ row }) => (
      <span className="font-mono text-xs text-[var(--color-subtle-foreground)] tabular-nums">
        {row.original.index}
      </span>
    ),
  },
  {
    id: "dot",
    header: () => null,
    enableSorting: false,
    enableHiding: false,
    cell: ({ row }) => {
      const r = row.original.tx;
      if (r.in_flight === 1) {
        return (
          <Loader2
            size={10}
            className="animate-spin text-[var(--color-subtle-foreground)]"
            aria-label="in flight"
          />
        );
      }
      const cls = stopDotClass(r.stop_reason);
      if (!cls) return null;
      return (
        <span
          className={cn("dot", cls)}
          title={r.stop_reason ?? "—"}
          style={{ marginRight: 0 }}
        />
      );
    },
  },
  {
    accessorFn: (r) => r.tx.ts,
    id: "when",
    header: "When",
    sortingFn: "basic",
    cell: ({ row }) => {
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
    accessorFn: (r) => r.tx.model ?? "",
    id: "model",
    header: "Model",
    cell: ({ row }) => {
      const tx = row.original.tx;
      const inflight = tx.in_flight === 1;
      const m = tx.model;
      const thought = (tx.thinking_blocks ?? 0) > 0;
      const budget = tx.thinking_budget ?? null;
      return (
        <span
          className={cn(
            "font-mono text-xs inline-flex items-center gap-1.5 whitespace-nowrap",
            inflight && "text-[var(--color-subtle-foreground)]",
          )}
        >
          <span title={m ?? "—"}>{shortModel(m)}</span>
          {(thought || budget != null) && (
            <span
              className="inline-flex items-center gap-0.5 text-[var(--color-chart-4)]"
              title={`extended thinking${thought ? ` · ${tx.thinking_blocks} block${(tx.thinking_blocks ?? 0) > 1 ? "s" : ""}` : inflight ? " budget set" : " budget set, not used this turn"}${budget ? ` · budget ${fmtInt(budget)}` : ""}`}
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
    accessorFn: (r) => r.tx.input_tokens,
    id: "in",
    header: "In",
    cell: ({ row }) => {
      const tx = row.original.tx;
      return (
        <span
          className={cn(
            "block text-right font-mono text-xs tabular-nums",
            tx.in_flight === 1 && "text-[var(--color-subtle-foreground)]",
          )}
        >
          {tx.in_flight === 1 ? "—" : fmtInt(tx.input_tokens)}
        </span>
      );
    },
  },
  {
    accessorFn: (r) => r.tx.output_tokens,
    id: "out",
    header: "Out",
    cell: ({ row }) => {
      const tx = row.original.tx;
      if (tx.in_flight === 1) {
        return (
          <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-subtle-foreground)]">
            —
          </span>
        );
      }
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
    accessorFn: (r) => r.tx.cache_read,
    id: "cache_read",
    header: "Cache R",
    cell: ({ row }) => {
      const tx = row.original.tx;
      return (
        <span
          className={cn(
            "block text-right font-mono text-xs tabular-nums",
            tx.in_flight === 1
              ? "text-[var(--color-subtle-foreground)]"
              : "text-[var(--color-volume)]/80",
          )}
        >
          {tx.in_flight === 1 ? "—" : fmtInt(tx.cache_read)}
        </span>
      );
    },
  },
  {
    accessorFn: (r) => r.tx.cache_creation_5m ?? 0,
    id: "cache_5m",
    header: "CW 5m",
    cell: ({ row }) => {
      const tx = row.original.tx;
      if (tx.in_flight === 1) {
        return (
          <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-subtle-foreground)]">
            —
          </span>
        );
      }
      const v = tx.cache_creation_5m ?? 0;
      return (
        <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-volume)]/55">
          {fmtInt(v)}
        </span>
      );
    },
  },
  {
    accessorFn: (r) => r.tx.cache_creation_1h ?? 0,
    id: "cache_1h",
    header: "CW 1h",
    cell: ({ row }) => {
      const tx = row.original.tx;
      if (tx.in_flight === 1) {
        return (
          <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-subtle-foreground)]">
            —
          </span>
        );
      }
      const v = tx.cache_creation_1h ?? 0;
      return (
        <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-volume)]/55">
          {fmtInt(v)}
        </span>
      );
    },
  },
  {
    accessorFn: (r) => r.tx.elapsed_ms,
    id: "latency",
    header: "Latency",
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-subtle-foreground)]">
        {row.original.tx.in_flight === 1
          ? "—"
          : fmtDuration(row.original.tx.elapsed_ms)}
      </span>
    ),
  },
  {
    id: "tools",
    header: "Tools",
    enableSorting: false,
    accessorFn: (r) => {
      const arr: string[] = r.tx.tools_json ? JSON.parse(r.tx.tools_json) : [];
      return arr.map(shortToolName).join(" ");
    },
    cell: ({ row }) => {
      const raw: string[] = row.original.tx.tools_json
        ? JSON.parse(row.original.tx.tools_json)
        : [];
      const tools = raw.map(shortToolName);
      if (tools.length === 0)
        return (
          <span className="text-[var(--color-subtle-foreground)] text-xs">—</span>
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
    accessorFn: (r) => estimateCostUsd(r.tx),
    id: "cost",
    header: "Cost",
    cell: ({ row }) => {
      const tx = row.original.tx;
      return (
        <span
          className={cn(
            "block text-right font-mono text-xs tabular-nums",
            tx.in_flight === 1
              ? "text-[var(--color-subtle-foreground)]"
              : "text-[var(--color-money)]",
          )}
        >
          {tx.in_flight === 1 ? "—" : fmtUsd(estimateCostUsd(tx))}
        </span>
      );
    },
  },
];

const COLUMN_LABELS: Record<string, string> = {
  turn: "Turn #",
  when: "When",
  model: "Model",
  in: "Input tokens",
  out: "Output tokens",
  cache_read: "Cache read",
  cache_5m: "Cache write 5m",
  cache_1h: "Cache write 1h",
  latency: "Latency",
  tools: "Tools",
  cost: "Cost",
};

const RIGHT_ALIGNED_COLS = new Set([
  "in",
  "out",
  "cache_read",
  "cache_5m",
  "cache_1h",
  "latency",
  "cost",
]);

export default function SessionTurnsTable({
  initialRows,
  sessionId,
}: {
  initialRows: TransactionRow[];
  sessionId: string;
}) {
  const [rows, setRows] = React.useState<TransactionRow[]>(initialRows);
  const [sorting, setSorting] = React.useState<SortingState>([
    { id: "turn", desc: true },
  ]);
  const [globalFilter, setGlobalFilter] = React.useState("");
  const [columnVisibility, setColumnVisibility] =
    React.useState<VisibilityState>({});
  const [expanded, setExpanded] = React.useState<Record<string, boolean>>({});
  const [highlightTxId, setHighlightTxId] = React.useState<string | null>(null);

  React.useEffect(() => subscribeRows(setRows), []);

  const data = React.useMemo(
    () => toTurns(rows, sessionId),
    [rows, sessionId],
  );

  // Deep-link handling: if the URL fragment is #<tx_id> (set by command palette
  // result links), auto-expand that turn, scroll it into view, and flash a
  // highlight so it's obvious which row matched. Re-runs when `data` grows
  // in case the row arrives via polling after first paint, but a ref guard
  // prevents re-scrolling the user away on every subsequent row update.
  const handledHashRef = React.useRef<string | null>(null);
  const highlightTimerRef = React.useRef<number | null>(null);
  React.useEffect(() => {
    const apply = () => {
      const raw = typeof window !== "undefined" ? window.location.hash.slice(1) : "";
      if (!raw) return;
      const txId = decodeURIComponent(raw);
      if (handledHashRef.current === txId) return;
      if (!data.some((d) => d.tx.tx_id === txId)) return;
      handledHashRef.current = txId;
      setExpanded((p) => ({ ...p, [txId]: true }));
      setHighlightTxId(txId);
      requestAnimationFrame(() => {
        document
          .getElementById(`row-${txId}`)
          ?.scrollIntoView({ block: "center", behavior: "smooth" });
      });
      if (highlightTimerRef.current != null) {
        window.clearTimeout(highlightTimerRef.current);
      }
      highlightTimerRef.current = window.setTimeout(() => {
        setHighlightTxId(null);
        highlightTimerRef.current = null;
      }, 2800);
    };
    apply();
    const onHash = () => {
      handledHashRef.current = null;
      apply();
    };
    window.addEventListener("hashchange", onHash);
    return () => {
      window.removeEventListener("hashchange", onHash);
      if (highlightTimerRef.current != null) {
        window.clearTimeout(highlightTimerRef.current);
      }
    };
  }, [data]);

  const table = useReactTable({
    data,
    columns,
    state: { sorting, globalFilter, columnVisibility },
    getRowId: (r) => r.tx.tx_id,
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    onColumnVisibilityChange: setColumnVisibility,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    globalFilterFn: (row, _id, value) => {
      const q = String(value).toLowerCase().trim();
      if (!q) return true;
      const tx = row.original.tx;
      const hay = [
        tx.model ?? "",
        tx.stop_reason ?? "",
        tx.tools_json ?? "",
        tx.url ?? "",
      ]
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    },
  });

  const visibleLeafColumns = table
    .getAllLeafColumns()
    .filter((c) => c.getCanHide());
  const visibleColCount = table.getVisibleLeafColumns().length;

  return (
    <section className="card overflow-hidden">
      <div className="flex items-center justify-between gap-3 px-5 py-3 border-b border-[var(--color-border)]">
        <div className="flex items-center gap-3">
          <h2 className="text-sm font-medium">Turns</h2>
          <span className="text-xs text-[var(--color-subtle-foreground)] tabular-nums">
            {data.length} {data.length === 1 ? "turn" : "turns"}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search
              size={12}
              className="absolute left-2 top-1/2 -translate-y-1/2 text-[var(--color-subtle-foreground)] pointer-events-none"
            />
            <Input
              placeholder="Filter model, tool, url…"
              value={globalFilter}
              onChange={(e) => setGlobalFilter(e.currentTarget.value)}
              className="h-7 w-56 pl-7 text-xs"
            />
          </div>
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

      {data.length === 0 ? (
        <p className="px-5 py-10 text-[var(--color-muted-foreground)] text-sm text-center">
          No turns yet.
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
                        h.id === "turn" && "w-10",
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
            {table.getRowModel().rows.map((row) => {
              const tx = row.original.tx;
              const isOpen = !!expanded[tx.tx_id];
              return (
                <React.Fragment key={row.id}>
                  <TableRow
                    id={`row-${tx.tx_id}`}
                    className={cn(
                      "cursor-pointer scroll-mt-24 transition-colors duration-700",
                      highlightTxId === tx.tx_id &&
                        "bg-amber-400/10 outline outline-2 -outline-offset-2 outline-amber-400/60",
                    )}
                    onClick={() =>
                      setExpanded((p) => ({ ...p, [tx.tx_id]: !p[tx.tx_id] }))
                    }
                  >
                    {row.getVisibleCells().map((cell) => (
                      <TableCell
                        key={cell.id}
                        className={cn(
                          cell.column.id === "expand" && "pl-4 pr-1 w-4 align-middle",
                          cell.column.id === "turn" && "w-10",
                          cell.column.id === "dot" && "px-2 w-3",
                          cell.column.id === "when" && "w-14",
                        )}
                      >
                        {cell.column.id === "expand" ? (
                          isOpen ? (
                            <ChevronDown
                              size={14}
                              className="text-[var(--color-subtle-foreground)]"
                            />
                          ) : (
                            <ChevronRight
                              size={14}
                              className="text-[var(--color-subtle-foreground)]"
                            />
                          )
                        ) : (
                          flexRender(
                            cell.column.columnDef.cell,
                            cell.getContext(),
                          )
                        )}
                      </TableCell>
                    ))}
                  </TableRow>
                  {isOpen && (
                    <TableRow className="hover:bg-transparent">
                      <TableCell
                        colSpan={visibleColCount}
                        className="bg-[var(--color-background)]/40 px-6 py-4"
                      >
                        <TurnDetail tx={tx} />
                      </TableCell>
                    </TableRow>
                  )}
                </React.Fragment>
              );
            })}
          </TableBody>
        </Table>
      )}
    </section>
  );
}

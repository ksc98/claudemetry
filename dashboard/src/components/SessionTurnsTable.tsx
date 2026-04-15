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
  fmtBytes,
  fmtDuration,
  fmtInt,
  fmtTs,
  fmtUsd,
} from "@/lib/format";
import { shortToolName } from "@/lib/tools";
import { stopDotClass } from "@/lib/stop";
import { cn } from "@/lib/cn";
import { subscribeRows } from "@/lib/rowsBus";

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
    accessorFn: (r) => r.tx.input_tokens,
    id: "in",
    header: "In",
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs tabular-nums">
        {fmtInt(row.original.tx.input_tokens)}
      </span>
    ),
  },
  {
    accessorFn: (r) => r.tx.output_tokens,
    id: "out",
    header: "Out",
    cell: ({ row }) => {
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
    accessorFn: (r) => r.tx.cache_read,
    id: "cache_read",
    header: "Cache R",
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-volume)]/80">
        {fmtInt(row.original.tx.cache_read)}
      </span>
    ),
  },
  {
    accessorFn: (r) => r.tx.cache_creation,
    id: "cache_creation",
    header: "Cache W",
    cell: ({ row }) => {
      const tx = row.original.tx;
      const w5m = tx.cache_creation_5m ?? 0;
      const w1h = tx.cache_creation_1h ?? 0;
      const split = w5m + w1h > 0;
      return (
        <span
          className="block text-right font-mono text-xs tabular-nums text-[var(--color-volume)]/55"
          title={
            split
              ? `${fmtInt(w5m)} × 5m · ${fmtInt(w1h)} × 1h`
              : tx.cache_creation > 0
                ? `${fmtInt(tx.cache_creation)} cache writes`
                : undefined
          }
        >
          {fmtInt(tx.cache_creation)}
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
        {fmtDuration(row.original.tx.elapsed_ms)}
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
    cell: ({ row }) => (
      <span className="block text-right font-mono text-xs tabular-nums text-[var(--color-money)]">
        {fmtUsd(estimateCostUsd(row.original.tx))}
      </span>
    ),
  },
];

const COLUMN_LABELS: Record<string, string> = {
  turn: "Turn #",
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

const RIGHT_ALIGNED_COLS = new Set([
  "in",
  "out",
  "cache_read",
  "cache_creation",
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
    { id: "turn", desc: false },
  ]);
  const [globalFilter, setGlobalFilter] = React.useState("");
  const [columnVisibility, setColumnVisibility] =
    React.useState<VisibilityState>({});
  const [expanded, setExpanded] = React.useState<Record<string, boolean>>({});

  React.useEffect(() => subscribeRows(setRows), []);

  const data = React.useMemo(
    () => toTurns(rows, sessionId),
    [rows, sessionId],
  );

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
                    className="cursor-pointer"
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

function TurnDetail({ tx }: { tx: TransactionRow }) {
  const tools: string[] = tx.tools_json ? JSON.parse(tx.tools_json) : [];
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2 text-xs">
      <DetailRow label="tx_id" value={<code className="font-mono">{tx.tx_id}</code>} />
      <DetailRow label="ts" value={<span className="font-mono">{fmtTs(tx.ts)}</span>} />
      <DetailRow
        label="url"
        value={
          <code
            className="font-mono text-[var(--color-muted-foreground)] break-all"
            title={tx.url ?? undefined}
          >
            {tx.method ? `${tx.method} ` : ""}{tx.url ?? "—"}
          </code>
        }
      />
      <DetailRow
        label="status"
        value={
          <span
            className={cn(
              "font-mono tabular-nums",
              tx.status >= 400
                ? "text-[var(--color-danger)]"
                : "text-[var(--color-good)]",
            )}
          >
            {tx.status}
          </span>
        }
      />
      <DetailRow
        label="stop_reason"
        value={
          <span className="font-mono">
            <span
              className={cn("dot", stopDotClass(tx.stop_reason))}
              style={{ marginRight: 6 }}
            />
            {tx.stop_reason ?? "—"}
          </span>
        }
      />
      <DetailRow
        label="req / resp bytes"
        value={
          <span className="font-mono tabular-nums">
            {fmtBytes(tx.req_body_bytes)} · {fmtBytes(tx.resp_body_bytes)}
          </span>
        }
      />
      {tx.max_tokens != null && (
        <DetailRow
          label="max_tokens"
          value={
            <span className="font-mono tabular-nums">
              {fmtInt(tx.output_tokens)} / {fmtInt(tx.max_tokens)}
              <span className="text-[var(--color-subtle-foreground)] ml-2">
                ({((tx.output_tokens / tx.max_tokens) * 100).toFixed(0)}%)
              </span>
            </span>
          }
        />
      )}
      {(tx.thinking_budget != null || (tx.thinking_blocks ?? 0) > 0) && (
        <DetailRow
          label="thinking"
          value={
            <span className="font-mono tabular-nums inline-flex items-center gap-2">
              <Sparkles size={11} className="text-[var(--color-chart-4)]" />
              {tx.thinking_budget != null && (
                <span>budget {fmtInt(tx.thinking_budget)}</span>
              )}
              {(tx.thinking_blocks ?? 0) > 0 && (
                <span className="text-[var(--color-muted-foreground)]">
                  · {tx.thinking_blocks} block
                  {(tx.thinking_blocks ?? 0) > 1 ? "s" : ""}
                </span>
              )}
            </span>
          }
        />
      )}
      {(tx.cache_creation_5m != null || tx.cache_creation_1h != null) && (
        <DetailRow
          label="cache writes"
          value={
            <span className="font-mono tabular-nums text-[var(--color-muted-foreground)]">
              {fmtInt(tx.cache_creation_5m ?? 0)} × 5m ·{" "}
              {fmtInt(tx.cache_creation_1h ?? 0)} × 1h
            </span>
          }
        />
      )}
      {(tx.rl_tok_remaining != null && tx.rl_tok_limit != null) && (
        <DetailRow
          label="rate-limit"
          value={
            <span className="font-mono tabular-nums text-[var(--color-muted-foreground)]">
              {fmtInt(tx.rl_tok_remaining)} / {fmtInt(tx.rl_tok_limit)} input tokens remaining
            </span>
          }
        />
      )}
      {tools.length > 0 && (
        <div className="md:col-span-2">
          <p className="text-[0.6875rem] uppercase tracking-[0.08em] text-[var(--color-muted-foreground)] mb-1">
            Tools ({tools.length})
          </p>
          <div className="flex flex-wrap gap-1">
            {tools.map((t, i) => (
              <span key={`${t}-${i}`} className="chip" title={t}>
                {shortToolName(t)}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function DetailRow({
  label,
  value,
}: {
  label: string;
  value: React.ReactNode;
}) {
  return (
    <div className="flex items-baseline gap-3 min-w-0">
      <span className="text-[0.6875rem] uppercase tracking-[0.08em] text-[var(--color-muted-foreground)] w-24 shrink-0">
        {label}
      </span>
      <span className="min-w-0 flex-1 truncate">{value}</span>
    </div>
  );
}

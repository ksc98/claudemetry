import * as React from "react";
import { type Table } from "@tanstack/react-table";
import { Search, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { DataTableViewOptions } from "./data-table-view-options";

export function DataTableToolbar<TData>({
  table,
  placeholder,
  searchValue,
  onSearchChange,
  columnLabels,
  leading,
  filters,
  trailing,
}: {
  table: Table<TData>;
  placeholder?: string;
  /** Controlled global filter value (most callers use `table.getState().globalFilter`). */
  searchValue: string;
  onSearchChange: (v: string) => void;
  columnLabels?: Readonly<Record<string, string>>;
  /** Rendered at the left of the toolbar (title, counts, model legend, etc.). */
  leading?: React.ReactNode;
  /** Faceted filters go between search and view options. */
  filters?: React.ReactNode;
  /** Extra buttons rendered between view options and the end. */
  trailing?: React.ReactNode;
}) {
  const hasFilters = searchValue.length > 0 || anyColumnFiltered(table);
  return (
    <div className="flex items-center justify-between gap-3 px-5 py-3 border-b border-[var(--color-border)] flex-wrap">
      <div className="flex items-center gap-3 flex-wrap min-w-0">{leading}</div>
      <div className="flex items-center gap-2 flex-wrap justify-end">
        <div className="relative">
          <Search
            size={12}
            className="absolute left-2 top-1/2 -translate-y-1/2 text-[var(--color-subtle-foreground)] pointer-events-none"
          />
          <Input
            placeholder={placeholder ?? "Filter…"}
            value={searchValue}
            onChange={(e) => onSearchChange(e.currentTarget.value)}
            className="h-7 w-56 pl-7 text-xs"
          />
        </div>
        {filters}
        {hasFilters && (
          <Button
            variant="ghost"
            size="sm"
            className="h-7 px-2 text-xs font-normal gap-1 text-[var(--color-muted-foreground)] hover:text-foreground hover:bg-[var(--color-card-elevated)]"
            onClick={() => {
              onSearchChange("");
              table.resetColumnFilters();
            }}
          >
            Reset
            <X size={12} className="opacity-60" />
          </Button>
        )}
        {trailing}
        <DataTableViewOptions table={table} labels={columnLabels} />
      </div>
    </div>
  );
}

function anyColumnFiltered<TData>(table: Table<TData>): boolean {
  return table.getState().columnFilters.length > 0;
}

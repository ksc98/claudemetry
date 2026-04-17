import * as React from "react";
import { type Column } from "@tanstack/react-table";
import { Check, Plus, X } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Separator } from "@/components/ui/separator";
import { cn } from "@/lib/cn";

export type FacetOption = {
  value: string;
  label: React.ReactNode;
  /** Optional pre-computed row count badge (defaults to TanStack's facet count). */
  count?: number;
  icon?: React.ComponentType<{ className?: string }>;
};

export function DataTableFacetedFilter<TData, TValue>({
  column,
  title,
  options,
  width = "w-[14rem]",
}: {
  column: Column<TData, TValue> | undefined;
  title: string;
  options: readonly FacetOption[];
  width?: string;
}) {
  const facets = column?.getFacetedUniqueValues();
  const selected = new Set(
    (column?.getFilterValue() as string[] | undefined) ?? [],
  );

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          className="h-7 gap-1.5 text-xs font-normal border-[var(--color-border-strong)] bg-[var(--color-card-elevated)] text-[var(--color-muted-foreground)] hover:text-foreground hover:bg-[var(--color-card-elevated)] hover:border-[var(--color-border-strong)] dark:bg-[var(--color-card-elevated)] dark:hover:bg-[var(--color-card-elevated)] data-[state=open]:bg-[var(--color-card-elevated)] data-[state=open]:text-foreground data-[state=open]:border-[var(--color-border-strong)]"
        >
          <Plus size={12} className="opacity-60" />
          {title}
          {selected.size > 0 && (
            <>
              <Separator orientation="vertical" className="mx-0.5 h-3" />
              <Badge
                variant="secondary"
                className="rounded-sm px-1 font-normal lg:hidden"
              >
                {selected.size}
              </Badge>
              <div className="hidden items-center gap-1 lg:flex">
                {selected.size > 2 ? (
                  <Badge
                    variant="secondary"
                    className="rounded-sm px-1 font-normal"
                  >
                    {selected.size} selected
                  </Badge>
                ) : (
                  options
                    .filter((opt) => selected.has(opt.value))
                    .map((opt) => (
                      <Badge
                        key={opt.value}
                        variant="secondary"
                        className="rounded-sm px-1 font-normal"
                      >
                        {opt.label}
                      </Badge>
                    ))
                )}
              </div>
            </>
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent className={cn("p-0", width)} align="start">
        <Command>
          <CommandInput placeholder={title} className="h-9 text-xs" />
          <CommandList>
            <CommandEmpty>No results.</CommandEmpty>
            <CommandGroup>
              {options.map((opt) => {
                const isSelected = selected.has(opt.value);
                const count = opt.count ?? facets?.get(opt.value);
                return (
                  <CommandItem
                    key={opt.value}
                    onSelect={() => {
                      const next = new Set(selected);
                      if (isSelected) next.delete(opt.value);
                      else next.add(opt.value);
                      column?.setFilterValue(
                        next.size ? Array.from(next) : undefined,
                      );
                    }}
                    className="text-xs"
                  >
                    <div
                      className={cn(
                        "mr-2 flex size-4 items-center justify-center rounded-[4px] border",
                        isSelected
                          ? "border-primary bg-primary text-primary-foreground"
                          : "border-input opacity-60",
                      )}
                    >
                      {isSelected && <Check className="size-3" />}
                    </div>
                    {opt.icon && (
                      <opt.icon className="mr-2 size-3.5 text-muted-foreground" />
                    )}
                    <span className="truncate">{opt.label}</span>
                    {count != null && (
                      <span className="ml-auto font-mono tabular-nums text-[0.6875rem] text-muted-foreground">
                        {count}
                      </span>
                    )}
                  </CommandItem>
                );
              })}
            </CommandGroup>
            {selected.size > 0 && (
              <>
                <CommandSeparator />
                <CommandGroup>
                  <CommandItem
                    onSelect={() => column?.setFilterValue(undefined)}
                    className="justify-center text-xs"
                  >
                    <X className="mr-2 size-3" />
                    Clear filters
                  </CommandItem>
                </CommandGroup>
              </>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

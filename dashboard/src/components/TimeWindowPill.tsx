import * as React from "react";
import {
  DEFAULT_WINDOW,
  WINDOWS,
  navigateToWindow,
  parseWindow,
  type Window,
} from "@/lib/pillWindow";
import { cn } from "@/lib/cn";

export default function TimeWindowPill() {
  const [win, setWin] = React.useState<Window>(DEFAULT_WINDOW);

  // Hydrate from URL after mount so the initial render matches SSR (which
  // doesn't see the URL on this island) before the user picks a window.
  React.useEffect(() => {
    const w = parseWindow(new URL(window.location.href).searchParams);
    setWin(w);
  }, []);

  return (
    <div
      role="tablist"
      aria-label="Time window"
      className="inline-flex items-center gap-1 rounded-full border border-[var(--color-border)] bg-[var(--color-card)] p-1"
    >
      {WINDOWS.map((w) => {
        const active = w === win;
        return (
          <button
            key={w}
            type="button"
            role="tab"
            aria-selected={active}
            onClick={() => navigateToWindow(w)}
            className={cn(
              "px-3 h-7 rounded-full text-xs font-medium tabular-nums transition-colors",
              active
                ? "bg-[var(--color-volume)] text-[var(--color-background)]"
                : "text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)] hover:bg-[var(--color-card-elevated)]",
            )}
          >
            {w}
          </button>
        );
      })}
    </div>
  );
}

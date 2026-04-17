import * as React from "react";
import {
  DEFAULT_WINDOW,
  WINDOWS,
  navigateToWindow,
  parseWindow,
  type Window,
} from "@/lib/pillWindow";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";

export default function TimeWindowPill() {
  const [win, setWin] = React.useState<Window>(DEFAULT_WINDOW);

  // Hydrate from URL after mount so the initial render matches SSR (which
  // doesn't see the URL on this island) before the user picks a window.
  React.useEffect(() => {
    const w = parseWindow(new URL(window.location.href).searchParams);
    setWin(w);
  }, []);

  return (
    <Tabs
      value={win}
      onValueChange={(v) => {
        if (v !== win) navigateToWindow(v as Window);
      }}
      aria-label="Time window"
    >
      <TabsList className="h-8 rounded-full border border-[var(--color-border)] bg-[var(--color-card)] p-[3px]">
        {WINDOWS.map((w) => (
          <TabsTrigger
            key={w}
            value={w}
            className="h-full rounded-full px-3 text-xs font-medium tabular-nums data-[state=active]:bg-[var(--color-volume-muted)] data-[state=active]:text-[var(--color-foreground)] data-[state=active]:shadow-[inset_0_0_0_1px_var(--color-border-strong)] dark:data-[state=active]:bg-[var(--color-volume-muted)]"
          >
            {w}
          </TabsTrigger>
        ))}
      </TabsList>
    </Tabs>
  );
}

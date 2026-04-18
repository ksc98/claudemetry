import * as React from "react";
import {
  DEFAULT_WINDOW,
  UNIT_MAX,
  UNITS,
  formatWindow,
  navigateToWindow,
  parseWindow,
  parseWindowParts,
  type Unit,
  type Window,
} from "@/lib/pillWindow";
import { cn } from "@/lib/cn";

// Sensible starting pick when the user hovers an inactive unit.
const UNIT_DEFAULT: Record<Unit, number> = { m: 15, h: 3, d: 1 };

export default function TimeWindowPill() {
  const [win, setWin] = React.useState<Window>(DEFAULT_WINDOW);

  // Hydrate from URL after mount so the initial render matches SSR (which
  // doesn't see the URL on this island) before the user picks a window.
  React.useEffect(() => {
    setWin(parseWindow(new URL(window.location.href).searchParams));
  }, []);

  const { n: activeN, u: activeU } = parseWindowParts(win);

  const commit = React.useCallback(
    (u: Unit, n: number) => {
      const next = formatWindow(n, u);
      if (next === win) return;
      setWin(next);
      navigateToWindow(next);
    },
    [win],
  );

  return (
    <div
      role="tablist"
      aria-label="Time window"
      className="flex h-8 items-stretch gap-0.5 rounded-full border border-[var(--color-border)] bg-[var(--color-card)] p-[3px]"
    >
      {UNITS.map((u) => (
        <UnitSegment
          key={u}
          unit={u}
          active={u === activeU}
          activeValue={activeN}
          startValue={u === activeU ? activeN : UNIT_DEFAULT[u]}
          onCommit={(n) => commit(u, n)}
        />
      ))}
    </div>
  );
}

function UnitSegment({
  unit,
  active,
  activeValue,
  startValue,
  onCommit,
}: {
  unit: Unit;
  active: boolean;
  activeValue: number;
  startValue: number;
  onCommit: (n: number) => void;
}) {
  const [open, setOpen] = React.useState(false);
  const [preview, setPreview] = React.useState(startValue);
  const closeTimer = React.useRef<number | undefined>(undefined);

  // Reset preview each time the popover opens so it starts from the
  // current-value-for-this-unit or the unit's default.
  React.useEffect(() => {
    if (open) setPreview(startValue);
  }, [open, startValue]);

  const clearClose = () => {
    if (closeTimer.current != null) {
      window.clearTimeout(closeTimer.current);
      closeTimer.current = undefined;
    }
  };

  const scheduleClose = () => {
    clearClose();
    closeTimer.current = window.setTimeout(() => setOpen(false), 120);
  };

  React.useEffect(() => () => clearClose(), []);

  const label = active ? `${activeValue}${unit}` : unit;

  return (
    <div
      className="relative"
      onMouseEnter={() => {
        clearClose();
        setOpen(true);
      }}
      onMouseLeave={scheduleClose}
    >
      <button
        type="button"
        role="tab"
        aria-selected={active}
        className={cn(
          "flex h-full min-w-[2.25rem] items-center justify-center rounded-full px-3 text-xs font-medium tabular-nums transition-colors",
          active
            ? "bg-[var(--color-volume-muted)] text-[var(--color-foreground)] shadow-[inset_0_0_0_1px_var(--color-border-strong)]"
            : "text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)]",
        )}
      >
        {label}
      </button>

      {open && (
        <UnitScrubber
          unit={unit}
          value={preview}
          onPreview={setPreview}
          onCommit={(n) => {
            onCommit(n);
            setOpen(false);
          }}
        />
      )}
    </div>
  );
}

const ITEM_H = 28;
const ROWS_VISIBLE = 5;

function UnitScrubber({
  unit,
  value,
  onPreview,
  onCommit,
}: {
  unit: Unit;
  value: number;
  onPreview: (n: number) => void;
  onCommit: (n: number) => void;
}) {
  const scrollRef = React.useRef<HTMLDivElement | null>(null);
  const commitTimer = React.useRef<number | undefined>(undefined);
  const suppressScrollRef = React.useRef(false);

  const max = UNIT_MAX[unit];
  const values = React.useMemo(
    () => Array.from({ length: max }, (_, i) => i + 1),
    [max],
  );

  // Initial centering — runs once when the popover mounts.
  React.useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    suppressScrollRef.current = true;
    el.scrollTo({ top: (value - 1) * ITEM_H, behavior: "auto" });
    // Release the suppression after the scroll settles.
    const id = window.setTimeout(() => {
      suppressScrollRef.current = false;
    }, 50);
    return () => window.clearTimeout(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  React.useEffect(() => {
    return () => {
      if (commitTimer.current != null) window.clearTimeout(commitTimer.current);
    };
  }, []);

  const handleScroll = () => {
    if (suppressScrollRef.current) return;
    const el = scrollRef.current;
    if (!el) return;
    const idx = Math.round(el.scrollTop / ITEM_H);
    const next = Math.min(Math.max(idx + 1, 1), max);
    if (next !== value) onPreview(next);

    // "Live" commit: applies after the user stops scrolling briefly so we
    // don't fire a reload on every wheel tick.
    if (commitTimer.current != null) window.clearTimeout(commitTimer.current);
    commitTimer.current = window.setTimeout(() => onCommit(next), 350);
  };

  const pad = ROWS_VISIBLE * ITEM_H / 2 - ITEM_H / 2;

  return (
    <div
      className="absolute left-1/2 top-1/2 z-50 -translate-x-1/2 -translate-y-1/2"
      role="presentation"
    >
      <div className="rounded-xl border border-[var(--color-border)] bg-[var(--color-card)] shadow-lg">
        <div className="relative">
          <div
            className="pointer-events-none absolute inset-x-1 top-1/2 -translate-y-1/2 rounded-md border border-[var(--color-border-strong)] bg-[var(--color-volume-muted)]"
            style={{ height: ITEM_H }}
            aria-hidden="true"
          />
          <div
            ref={scrollRef}
            onScroll={handleScroll}
            className="relative w-16 snap-y snap-mandatory overflow-y-auto [scrollbar-width:none] [&::-webkit-scrollbar]:hidden"
            style={{
              height: ROWS_VISIBLE * ITEM_H,
              paddingTop: pad,
              paddingBottom: pad,
            }}
          >
            {values.map((n) => (
              <button
                key={n}
                type="button"
                onClick={() => onCommit(n)}
                className={cn(
                  "flex w-full snap-center items-center justify-center text-xs tabular-nums transition-colors",
                  n === value
                    ? "font-medium text-[var(--color-foreground)]"
                    : "text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)]",
                )}
                style={{ height: ITEM_H }}
              >
                {n}
                {unit}
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

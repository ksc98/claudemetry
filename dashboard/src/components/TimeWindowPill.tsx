import * as React from "react";
import { lazy, Suspense } from "react";
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

// NumberFlow registers web-component globals on module evaluation and
// references DOM APIs missing in Cloudflare Workers SSR. Lazy-load so the
// module is only fetched client-side.
const NumberFlowLazy = lazy(() => import("@number-flow/react"));

// Sensible starting pick when the user scrolls on an inactive unit.
const UNIT_DEFAULT: Record<Unit, number> = { m: 15, h: 3, d: 1 };

// Min gap between ticks. Smooth-scroll mice / trackpads fire ~10+ wheel
// events per notch; without throttling, each notch jumps by 5+. With
// this, the value moves at most ~20/sec — fast enough to feel
// responsive while staying granular.
const MIN_MS_PER_TICK = 50;

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
  onCommit,
}: {
  unit: Unit;
  active: boolean;
  activeValue: number;
  onCommit: (n: number) => void;
}) {
  const [preview, setPreview] = React.useState<number | null>(null);
  const buttonRef = React.useRef<HTMLButtonElement | null>(null);
  const lastTickRef = React.useRef(0);
  const max = UNIT_MAX[unit];

  // Wheel handler is attached imperatively because React's onWheel is
  // passive — preventDefault wouldn't stop the page from scrolling
  // alongside the value change.
  React.useEffect(() => {
    const el = buttonRef.current;
    if (!el) return;
    const handler = (e: WheelEvent) => {
      e.preventDefault();
      if (e.deltaY === 0) return;
      const now = performance.now();
      if (now - lastTickRef.current < MIN_MS_PER_TICK) return;
      lastTickRef.current = now;
      const delta = e.deltaY > 0 ? 1 : -1;
      setPreview((prev) => {
        const start = prev ?? (active ? activeValue : UNIT_DEFAULT[unit]);
        const next = Math.min(Math.max(start + delta, 1), max);
        return next === start ? prev : next;
      });
    };
    el.addEventListener("wheel", handler, { passive: false });
    return () => el.removeEventListener("wheel", handler);
  }, [active, activeValue, max, unit]);

  const handleMouseLeave = () => {
    lastTickRef.current = 0;
    setPreview(null);
  };

  const handleClick = () => {
    onCommit(preview ?? (active ? activeValue : UNIT_DEFAULT[unit]));
    lastTickRef.current = 0;
    setPreview(null);
  };

  // While previewing, the segment shows its candidate value and lights up
  // alongside the (still-active) current selection so the user can see
  // both "what's set" and "what would be picked on click".
  const showValue = preview !== null || active;
  const valueToShow = preview ?? activeValue;
  const isHighlighted = active || preview !== null;

  return (
    <button
      ref={buttonRef}
      type="button"
      role="tab"
      aria-selected={active}
      onMouseLeave={handleMouseLeave}
      onClick={handleClick}
      className={cn(
        "flex h-full min-w-[2.25rem] items-center justify-center rounded-full px-3 text-xs font-medium tabular-nums transition-colors",
        isHighlighted
          ? "bg-[var(--color-volume-muted)] text-[var(--color-foreground)] shadow-[inset_0_0_0_1px_var(--color-border-strong)]"
          : "text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)]",
      )}
    >
      {showValue ? (
        <span className="inline-flex items-baseline">
          <Suspense fallback={<span>{valueToShow}</span>}>
            <NumberFlowLazy value={valueToShow} />
          </Suspense>
          {unit}
        </span>
      ) : (
        unit
      )}
    </button>
  );
}

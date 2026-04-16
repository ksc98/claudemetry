import * as React from "react";
import {
  Home,
  Search,
  PanelLeftClose,
  PanelLeftOpen,
} from "lucide-react";
import type { SessionSummary } from "@/lib/sessions";
import { fmtAgo, fmtUsd } from "@/lib/format";
import { cn } from "@/lib/cn";
import { CommandPalette } from "@/components/CommandPalette";

const COLLAPSE_KEY = "sidebar:collapsed";

function shortSession(id: string): string {
  return id.slice(0, 8);
}

const POLL_MS = 5_000;

export function Sidebar({
  sessions: initialSessions,
  currentSessionId,
}: {
  sessions: SessionSummary[];
  currentSessionId?: string | null;
}) {
  const [collapsed, setCollapsed] = React.useState(false);
  const [paletteOpen, setPaletteOpen] = React.useState(false);
  const [currentPath, setCurrentPath] = React.useState<string>("/");
  const [sessions, setSessions] = React.useState<SessionSummary[]>(initialSessions);

  // Load collapse state + capture current path after hydrate.
  React.useEffect(() => {
    try {
      setCollapsed(localStorage.getItem(COLLAPSE_KEY) === "1");
    } catch {
      /* localStorage blocked */
    }
    setCurrentPath(window.location.pathname);
  }, []);

  // 1s tick so relative timestamps ("3s", "1m") update live between polls.
  const [, setNowTick] = React.useState(0);
  React.useEffect(() => {
    const id = window.setInterval(() => setNowTick((t) => t + 1), 1000);
    return () => window.clearInterval(id);
  }, []);

  // Poll /api/sessions.json to keep the list live.
  React.useEffect(() => {
    let cancelled = false;
    let inflight = false;
    const tick = async () => {
      if (inflight || document.hidden) return;
      inflight = true;
      try {
        const res = await fetch("/api/sessions.json", {
          credentials: "include",
          cache: "no-store",
        });
        if (!res.ok) return;
        const data = (await res.json()) as SessionSummary[];
        if (!cancelled && Array.isArray(data)) setSessions(data);
      } catch {
        /* next tick will retry */
      } finally {
        inflight = false;
      }
    };
    const id = window.setInterval(tick, POLL_MS);
    const onVis = () => {
      if (!document.hidden) void tick();
    };
    document.addEventListener("visibilitychange", onVis);
    return () => {
      cancelled = true;
      window.clearInterval(id);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, []);

  const toggle = React.useCallback(() => {
    setCollapsed((prev) => {
      const next = !prev;
      try {
        localStorage.setItem(COLLAPSE_KEY, next ? "1" : "0");
      } catch {
        /* ignore */
      }
      return next;
    });
  }, []);

  // Global hotkeys: "/" and Cmd/Ctrl+K open the palette.
  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement | null)?.tagName;
      const editable = (e.target as HTMLElement | null)?.isContentEditable;
      const inField = tag === "INPUT" || tag === "TEXTAREA" || editable;
      if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) {
        e.preventDefault();
        setPaletteOpen(true);
        return;
      }
      if (e.key === "/" && !inField && !e.altKey && !e.metaKey && !e.ctrlKey) {
        e.preventDefault();
        setPaletteOpen(true);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const onOverview = currentPath === "/";
  const activeCount = sessions.filter((s) => s.active).length;

  return (
    <>
      <aside
        className={cn(
          "sticky top-0 h-screen flex flex-col border-r border-[var(--color-border)] bg-[var(--color-background)]/95 backdrop-blur-sm transition-[width] duration-200 ease-out shrink-0",
          collapsed ? "w-14" : "w-60",
        )}
        aria-label="Primary"
      >
        <div
          className={cn(
            "flex items-center h-14 px-3 border-b border-[var(--color-border)]",
            collapsed ? "justify-center" : "justify-between",
          )}
        >
          <a
            href="/"
            className="flex items-center gap-2 min-w-0"
            title="llmetry"
          >
            <span className="inline-block w-2.5 h-2.5 rounded-sm bg-[var(--color-money)] shrink-0" />
            {!collapsed && (
              <span className="text-[0.95rem] font-semibold tracking-tight truncate">
                llmetry
              </span>
            )}
          </a>
          {!collapsed && (
            <button
              type="button"
              onClick={toggle}
              className="text-[var(--color-subtle-foreground)] hover:text-[var(--color-foreground)]"
              aria-label="Collapse sidebar"
              title="Collapse"
            >
              <PanelLeftClose size={16} />
            </button>
          )}
        </div>

        <div className="p-2 flex flex-col gap-1">
          <button
            type="button"
            onClick={() => setPaletteOpen(true)}
            className={cn(
              "group flex items-center gap-2 rounded-md border border-[var(--color-border)] bg-[var(--color-card)] text-[var(--color-subtle-foreground)] hover:text-[var(--color-foreground)] hover:bg-[var(--color-card-elevated)]/60 transition-colors",
              collapsed ? "justify-center h-9 w-9 mx-auto" : "h-9 px-2.5",
            )}
            title="Search (/)"
            aria-label="Open search"
          >
            <Search size={14} />
            {!collapsed && (
              <>
                <span className="text-xs flex-1 text-left">Search…</span>
                <kbd className="inline-flex items-center rounded border border-[var(--color-border)] bg-[var(--color-card-elevated)] px-1.5 h-4 font-mono text-[9px] text-[var(--color-muted-foreground)]">
                  /
                </kbd>
              </>
            )}
          </button>

          <NavLink
            href="/"
            label="Overview"
            Icon={Home}
            active={onOverview}
            collapsed={collapsed}
          />
        </div>

        {!collapsed ? (
          <>
            <div className="px-3 pt-3 pb-1 flex items-baseline justify-between">
              <span className="text-[0.6875rem] uppercase tracking-[0.08em] text-[var(--color-muted-foreground)]">
                Sessions
              </span>
              <span className="text-[10px] tabular-nums font-mono text-[var(--color-subtle-foreground)]">
                {activeCount > 0 ? `${activeCount} active · ` : ""}
                {sessions.length}
              </span>
            </div>
            <div className="flex-1 overflow-y-auto px-2 pb-3 min-h-0">
              {sessions.length === 0 ? (
                <p className="px-2 py-4 text-[11px] text-[var(--color-subtle-foreground)]">
                  No sessions yet.
                </p>
              ) : (
                <ul className="flex flex-col gap-0.5">
                  {sessions.map((s) => (
                    <SessionItem
                      key={s.id}
                      s={s}
                      current={s.id === currentSessionId}
                    />
                  ))}
                </ul>
              )}
            </div>
          </>
        ) : (
          <div className="flex-1 flex flex-col items-center pt-3 gap-1 min-h-0">
            <div
              className="text-[10px] font-mono text-[var(--color-subtle-foreground)] tabular-nums"
              title={`${sessions.length} sessions${activeCount > 0 ? `, ${activeCount} active` : ""}`}
            >
              {sessions.length}
            </div>
            {activeCount > 0 && (
              <span
                className="live-dot"
                title={`${activeCount} active session${activeCount > 1 ? "s" : ""}`}
              />
            )}
          </div>
        )}

        {collapsed && (
          <div className="p-2 border-t border-[var(--color-border)] flex justify-center">
            <button
              type="button"
              onClick={toggle}
              className="text-[var(--color-subtle-foreground)] hover:text-[var(--color-foreground)]"
              aria-label="Expand sidebar"
              title="Expand"
            >
              <PanelLeftOpen size={16} />
            </button>
          </div>
        )}
      </aside>

      <CommandPalette open={paletteOpen} onClose={() => setPaletteOpen(false)} />
    </>
  );
}

function NavLink({
  href,
  label,
  Icon,
  active,
  collapsed,
}: {
  href: string;
  label: string;
  Icon: React.ComponentType<{ size?: number }>;
  active: boolean;
  collapsed: boolean;
}) {
  return (
    <a
      href={href}
      title={label}
      aria-current={active ? "page" : undefined}
      className={cn(
        "flex items-center gap-2 rounded-md transition-colors",
        collapsed ? "justify-center h-9 w-9 mx-auto" : "h-9 px-2.5",
        active
          ? "bg-[var(--color-card-elevated)] text-[var(--color-foreground)] shadow-[inset_0_0_0_1px_var(--color-border)]"
          : "text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)] hover:bg-[var(--color-card)]",
      )}
    >
      <Icon size={14} />
      {!collapsed && <span className="text-xs">{label}</span>}
    </a>
  );
}

function SessionItem({
  s,
  current,
}: {
  s: SessionSummary;
  current: boolean;
}) {
  return (
    <li>
      <a
        href={`/session/${encodeURIComponent(s.id)}`}
        className={cn(
          "block rounded-md px-2 py-1.5 transition-colors",
          current
            ? "bg-[var(--color-card-elevated)] shadow-[inset_2px_0_0_0_var(--color-volume)]"
            : "hover:bg-[var(--color-card)]",
        )}
        aria-current={current ? "page" : undefined}
      >
        <div className="flex items-center gap-1.5 min-w-0">
          {s.active && <span className="live-dot shrink-0" aria-label="active" />}
          <code
            className={cn(
              "font-mono text-[11px] truncate",
              current
                ? "text-[var(--color-foreground)]"
                : "text-[var(--color-volume)]",
            )}
            title={s.id}
          >
            {shortSession(s.id)}
          </code>
          <span className="ml-auto text-[9.5px] tabular-nums font-mono text-[var(--color-subtle-foreground)]">
            {fmtAgo(s.lastTs)}
          </span>
        </div>
        <div className="mt-0.5 flex items-center gap-2 text-[10px] font-mono text-[var(--color-subtle-foreground)] tabular-nums">
          <span>{s.turns}t</span>
          <span>·</span>
          <span className="text-[var(--color-money)]/80">{fmtUsd(s.costUsd)}</span>
          {s.topModel && (
            <>
              <span>·</span>
              <span className="truncate" title={s.topModel}>
                {s.topModel}
                {s.modelCount > 1 ? ` +${s.modelCount - 1}` : ""}
              </span>
            </>
          )}
        </div>
      </a>
    </li>
  );
}

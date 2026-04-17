import * as React from "react";
import {
  Search,
  Sparkles,
  Type,
  Layers,
  Loader2,
  AlertCircle,
} from "lucide-react";
import { Command as CommandPrimitive } from "cmdk";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Kbd, KbdGroup } from "@/components/ui/kbd";
import { fmtAgo } from "@/lib/format";
import { cn } from "@/lib/cn";

type Mode = "hybrid" | "fts" | "vector";

type Hit = {
  tx_id: string;
  ts: number;
  session_id: string | null;
  model: string | null;
  user_snip: string | null;
  asst_snip: string | null;
  score: number;
  match_source: "fts" | "vector" | "both" | "unknown";
};

type State =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "error"; message: string; retryAfter?: number }
  | { kind: "results"; mode: Mode; hits: Hit[]; query: string };

const MODES: {
  value: Mode;
  label: string;
  hint: string;
  Icon: React.ComponentType<{ className?: string }>;
}[] = [
  { value: "hybrid", label: "Hybrid", hint: "Both indexes, merged (RRF)", Icon: Layers },
  { value: "fts", label: "Keyword", hint: "Exact tokens via FTS5/bm25", Icon: Type },
  { value: "vector", label: "Semantic", hint: "Embedding cosine similarity", Icon: Sparkles },
];

export function CommandPalette({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  const [query, setQuery] = React.useState("");
  const [mode, setMode] = React.useState<Mode>("hybrid");
  const [state, setState] = React.useState<State>({ kind: "idle" });
  const reqIdRef = React.useRef(0);

  // Reset when closed.
  React.useEffect(() => {
    if (!open) {
      setQuery("");
      setState({ kind: "idle" });
    }
  }, [open]);

  // Debounced search — unchanged contract: 250ms debounce, abort + reqId gating,
  // 2-char minimum, 429 retry_after passthrough, 4xx message surfacing.
  React.useEffect(() => {
    if (!open) return;
    const q = query.trim();
    if (q.length < 2) {
      setState({ kind: "idle" });
      return;
    }
    const myReqId = ++reqIdRef.current;
    const ctrl = new AbortController();
    const t = window.setTimeout(async () => {
      setState({ kind: "loading" });
      try {
        const res = await fetch("/api/search", {
          method: "POST",
          signal: ctrl.signal,
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ q, mode, limit: 20 }),
        });
        if (myReqId !== reqIdRef.current) return;
        if (res.status === 429) {
          const body = (await res.json().catch(() => null)) as {
            retry_after_seconds?: number;
          } | null;
          setState({
            kind: "error",
            message: "Rate limit hit. Give it a moment.",
            retryAfter: body?.retry_after_seconds,
          });
          return;
        }
        if (!res.ok) {
          setState({ kind: "error", message: `Search failed (${res.status}).` });
          return;
        }
        const data = (await res.json()) as { mode: Mode; results: Hit[] };
        setState({ kind: "results", mode: data.mode, hits: data.results, query: q });
      } catch (e) {
        if (myReqId !== reqIdRef.current) return;
        if ((e as Error).name === "AbortError") return;
        setState({ kind: "error", message: (e as Error).message || "Network error" });
      }
    }, 250);
    return () => {
      ctrl.abort();
      window.clearTimeout(t);
    };
  }, [query, mode, open]);

  const openHit = (hit: Hit) => {
    if (!hit.session_id) return;
    window.location.href = `/session/${encodeURIComponent(hit.session_id)}#${encodeURIComponent(hit.tx_id)}`;
  };

  return (
    <CommandDialog
      open={open}
      onOpenChange={(v) => {
        if (!v) onClose();
      }}
      showCloseButton={false}
      title="Search sessions"
      description="Search sessions, prompts, and responses."
      className="overflow-visible p-0 sm:max-w-2xl"
      // shouldFilter=false: preserve server-side ranking; cmdk should not reorder hits.
      shouldFilter={false}
    >
      {/* Top row: search input + mode toggle + esc hint. */}
      <div className="flex items-center gap-2 border-b border-[var(--color-border)] px-3">
        <Search className="size-4 shrink-0 text-[var(--color-subtle-foreground)]" />
        <CommandPrimitive.Input
          value={query}
          onValueChange={setQuery}
          placeholder="Search sessions, prompts, responses…"
          autoFocus
          className="flex-1 h-12 bg-transparent text-sm outline-none placeholder:text-[var(--color-subtle-foreground)]"
        />
        <div className="flex items-center gap-0.5 rounded-md border border-[var(--color-border)] bg-[var(--color-card-elevated)]/40 p-0.5">
          {MODES.map((m) => {
            const isActive = mode === m.value;
            return (
              <button
                key={m.value}
                type="button"
                onClick={() => setMode(m.value)}
                title={m.hint}
                className={cn(
                  "inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[10.5px] font-medium transition-colors",
                  isActive
                    ? "bg-[var(--color-card)] text-[var(--color-foreground)] shadow-[inset_0_0_0_1px_var(--color-border)]"
                    : "text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)]",
                )}
              >
                <m.Icon className="size-3" />
                {m.label}
              </button>
            );
          })}
        </div>
        <button
          type="button"
          onClick={onClose}
          className="font-mono text-[10px] px-1.5 py-0.5 rounded border border-[var(--color-border)] text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)]"
          aria-label="Close (Esc)"
        >
          esc
        </button>
      </div>

      <CommandList className="max-h-[60vh] overflow-y-auto px-1 py-1">
        <Body state={state} mode={mode} query={query.trim()} onPick={openHit} />
      </CommandList>

      <div className="border-t border-[var(--color-border)] px-3 py-2 flex items-center gap-3 text-[10px] text-[var(--color-subtle-foreground)]">
        <KbdGroup>
          <Kbd className="text-[9px]">↑</Kbd>
          <Kbd className="text-[9px]">↓</Kbd>
          <span>navigate</span>
        </KbdGroup>
        <KbdGroup>
          <Kbd className="text-[9px]">enter</Kbd>
          <span>open</span>
        </KbdGroup>
        <KbdGroup>
          <Kbd className="text-[9px]">esc</Kbd>
          <span>close</span>
        </KbdGroup>
      </div>
    </CommandDialog>
  );
}

// Custom wrapper around CommandInput — the shadcn default renders its own
// icon and padding; we lay out icon/mode-toggle/esc in the outer flex, so we
// only want the input itself, stripped of the built-in wrapper.
function Body({
  state,
  mode,
  query,
  onPick,
}: {
  state: State;
  mode: Mode;
  query: string;
  onPick: (h: Hit) => void;
}) {
  if (state.kind === "idle") {
    return (
      <div className="px-3 py-6 text-center text-xs text-[var(--color-subtle-foreground)]">
        {query.length === 0
          ? "Start typing to search your sessions."
          : "Keep typing — at least 2 characters."}
      </div>
    );
  }
  if (state.kind === "loading") {
    return (
      <div className="flex items-center gap-2 px-3 py-4 text-xs text-[var(--color-subtle-foreground)]">
        <Loader2 className="size-3 animate-spin" />
        Searching…
      </div>
    );
  }
  if (state.kind === "error") {
    return (
      <div className="flex items-start gap-2 px-3 py-3 text-xs text-[var(--color-muted-foreground)]">
        <AlertCircle className="mt-0.5 size-3.5 text-amber-400/80" />
        <div>
          {state.message}
          {state.retryAfter ? ` Retry in ~${state.retryAfter}s.` : null}
        </div>
      </div>
    );
  }
  if (state.hits.length === 0) {
    return (
      <CommandEmpty>
        <div className="flex flex-col items-center justify-center gap-2 px-4 py-4 text-center">
          <Search className="size-5 text-[var(--color-subtle-foreground)]" />
          <div className="text-xs text-[var(--color-muted-foreground)]">
            No matches for{" "}
            <span className="font-medium text-[var(--color-foreground)]">
              “{state.query}”
            </span>
            .
          </div>
          {mode === "vector" ? (
            <div className="text-[11px] text-[var(--color-subtle-foreground)]">
              Try another phrasing, or switch to Keyword / Hybrid.
            </div>
          ) : null}
        </div>
      </CommandEmpty>
    );
  }
  return (
    <CommandGroup className="p-0">
      {state.hits.map((h) => (
        <HitItem key={h.tx_id} hit={h} onPick={onPick} />
      ))}
    </CommandGroup>
  );
}

function HitItem({
  hit,
  onPick,
}: {
  hit: Hit;
  onPick: (h: Hit) => void;
}) {
  return (
    <CommandItem
      // Include snippets in the searchable value so cmdk's internal key-based
      // selection still works, but shouldFilter=false on the Command means
      // order and inclusion are preserved from the server.
      value={`${hit.tx_id}`}
      onSelect={() => onPick(hit)}
      className="flex-col items-stretch gap-1 !p-2.5 data-[selected=true]:bg-[var(--color-card-elevated)]/80"
    >
      <div className="flex items-center gap-2 text-[10.5px]">
        <MatchBadge source={hit.match_source} />
        {hit.model ? (
          <span className="font-mono text-[var(--color-muted-foreground)]">
            {hit.model}
          </span>
        ) : null}
        <span className="text-[var(--color-subtle-foreground)]">·</span>
        <span className="text-[var(--color-subtle-foreground)]">
          {fmtAgo(hit.ts)}
        </span>
        <span className="ml-auto font-mono tabular-nums text-[var(--color-subtle-foreground)]">
          {hit.score.toFixed(3)}
        </span>
      </div>
      <div className="flex flex-col gap-0.5">
        {hit.user_snip ? <Snip role="you" text={hit.user_snip} /> : null}
        {hit.asst_snip ? <Snip role="asst" text={hit.asst_snip} /> : null}
      </div>
    </CommandItem>
  );
}

function Snip({ role, text }: { role: "you" | "asst"; text: string }) {
  return (
    <div className="flex gap-2.5 text-xs leading-relaxed">
      <span
        className={cn(
          "mt-[1px] shrink-0 select-none font-mono text-[9.5px] uppercase tracking-[0.08em]",
          role === "you"
            ? "text-sky-400/70"
            : "text-[var(--color-subtle-foreground)]",
        )}
      >
        {role}
      </span>
      <span
        className={cn(
          "text-[var(--color-muted-foreground)] line-clamp-2",
          "[&>mark]:rounded [&>mark]:bg-yellow-400/25 [&>mark]:px-0.5",
          "[&>mark]:text-[var(--color-foreground)] [&>mark]:font-medium",
        )}
        // Snippets contain <mark> tags from SQLite's snippet() function.
        dangerouslySetInnerHTML={{ __html: text }}
      />
    </div>
  );
}

function MatchBadge({ source }: { source: Hit["match_source"] }) {
  const styles: Record<Hit["match_source"], string> = {
    fts: "bg-sky-500/10 text-sky-300 ring-sky-500/20",
    vector: "bg-violet-500/10 text-violet-300 ring-violet-500/20",
    both: "bg-emerald-500/10 text-emerald-300 ring-emerald-500/20",
    unknown: "bg-neutral-500/10 text-neutral-400 ring-neutral-500/20",
  };
  const label: Record<Hit["match_source"], string> = {
    fts: "keyword",
    vector: "semantic",
    both: "both",
    unknown: "?",
  };
  return (
    <span
      className={cn(
        "inline-flex items-center rounded px-1.5 py-[1px] font-mono text-[9px] font-medium uppercase tracking-[0.08em] ring-1 ring-inset",
        styles[source],
      )}
    >
      {label[source]}
    </span>
  );
}

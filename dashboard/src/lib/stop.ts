export function stopDotClass(sr: string | null | undefined): string {
  if (sr === "end_turn") return "dot-good";
  if (sr === "tool_use") return "dot-muted";
  if (sr === "max_tokens" || sr === "error") return "dot-danger";
  if (sr === "stop_sequence") return "dot-warn";
  return "dot-muted";
}

// Matches stopDotClass but returns a CSS color value (for MixBar, legends,
// anywhere we can't hang a class off an element).
export function stopColorVar(sr: string | null | undefined): string {
  if (sr === "end_turn") return "var(--color-good)";
  if (sr === "tool_use") return "var(--color-subtle-foreground)";
  if (sr === "max_tokens" || sr === "error") return "var(--color-danger)";
  if (sr === "stop_sequence") return "var(--color-warn)";
  return "var(--color-subtle-foreground)";
}

export function stopLabel(sr: string | null | undefined): string {
  if (!sr) return "unknown";
  return sr;
}

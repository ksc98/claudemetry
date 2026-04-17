import NumberFlow from "@number-flow/react";

export type KpiValueKind = "int" | "usd" | "pct";

interface Props {
  num: number;
  kind: KpiValueKind;
  className?: string;
}

/**
 * Animated numeric value for KPI cards. Hydrates as a client island so
 * number-flow can tween between renders; renders a plain formatted string
 * on the server so SSR output matches first-paint.
 */
export default function KpiValue({ num, kind, className }: Props) {
  if (kind === "usd") {
    // Match the existing fmtUsd behavior: 4 decimals below $0.01, 2 otherwise.
    const digits = Math.abs(num) > 0 && Math.abs(num) < 0.01 ? 4 : 2;
    return (
      <NumberFlow
        className={className}
        value={num}
        format={{
          style: "currency",
          currency: "USD",
          minimumFractionDigits: digits,
          maximumFractionDigits: digits,
        }}
      />
    );
  }

  if (kind === "pct") {
    // Caller passes a whole-number percent (e.g. 34 for "34%"); we divide
    // so number-flow uses the native Intl percent formatter for crisp
    // digit-by-digit transitions.
    return (
      <NumberFlow
        className={className}
        value={num / 100}
        format={{
          style: "percent",
          maximumFractionDigits: 0,
        }}
      />
    );
  }

  return (
    <NumberFlow
      className={className}
      value={num}
      format={{ maximumFractionDigits: 0 }}
    />
  );
}

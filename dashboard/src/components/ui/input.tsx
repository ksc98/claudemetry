import * as React from "react";
import { cn } from "@/lib/cn";

export const Input = React.forwardRef<
  HTMLInputElement,
  React.InputHTMLAttributes<HTMLInputElement>
>(({ className, type, ...props }, ref) => (
  <input
    ref={ref}
    type={type}
    className={cn(
      "flex h-8 w-full rounded-md border border-[var(--color-border)] bg-transparent px-3 py-1 text-xs",
      "text-[var(--color-foreground)] placeholder:text-[var(--color-subtle-foreground)]",
      "focus-visible:outline-none focus-visible:border-[var(--color-border-strong)]",
      "disabled:cursor-not-allowed disabled:opacity-50",
      className,
    )}
    {...props}
  />
));
Input.displayName = "Input";

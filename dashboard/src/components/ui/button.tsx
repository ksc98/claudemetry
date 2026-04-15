import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/cn";

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-xs font-medium tracking-[0.01em] transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-[var(--color-border-strong)] disabled:pointer-events-none disabled:opacity-50",
  {
    variants: {
      variant: {
        default:
          "bg-[var(--color-card-elevated)] text-[var(--color-foreground)] border border-[var(--color-border)] hover:border-[var(--color-border-strong)]",
        ghost:
          "text-[var(--color-muted-foreground)] hover:bg-[var(--color-card-elevated)]/70 hover:text-[var(--color-foreground)]",
        outline:
          "border border-[var(--color-border)] bg-transparent hover:bg-[var(--color-card-elevated)]/60 text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)]",
      },
      size: {
        default: "h-8 px-3",
        sm: "h-7 px-2",
        icon: "h-7 w-7",
      },
    },
    defaultVariants: { variant: "default", size: "default" },
  },
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, asChild, ...props }, ref) => {
    const Comp = asChild ? Slot : "button";
    return (
      <Comp
        ref={ref}
        className={cn(buttonVariants({ variant, size }), className)}
        {...props}
      />
    );
  },
);
Button.displayName = "Button";

export { buttonVariants };

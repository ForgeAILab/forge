import { cn } from '@/lib/cn'

const variantStyles = {
  default:
    'bg-primary text-primary-foreground shadow-[0_0_8px_rgba(249,115,22,0.2)] hover:bg-primary/90',
  destructive: 'bg-destructive text-destructive-foreground hover:bg-destructive/90',
  outline:
    'border border-input bg-card text-secondary-foreground hover:bg-muted hover:text-foreground',
  secondary: 'bg-secondary text-secondary-foreground hover:bg-secondary/80',
  ghost: 'hover:bg-accent hover:text-accent-foreground',
  link: 'text-primary underline-offset-4 hover:underline',
}

const sizeStyles = {
  default: 'px-[14px] py-[7px] text-ui',
  sm: 'px-[9px] py-[5px] text-xs rounded-md',
  lg: 'h-11 rounded-md px-8 text-sm',
  icon: 'h-8 w-8',
  'icon-sm': 'h-7 w-7 rounded-md',
  'icon-xs': 'h-5 w-5 rounded-sm text-[11px]',
}

export type ButtonVariant = keyof typeof variantStyles
export type ButtonSize = keyof typeof sizeStyles

export function buttonClassName({
  variant = 'default',
  size = 'default',
  className,
}: {
  variant?: ButtonVariant
  size?: ButtonSize
  className?: string
} = {}): string {
  return cn(
    'inline-flex items-center justify-center gap-1.5 whitespace-nowrap rounded-md font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50',
    variantStyles[variant],
    sizeStyles[size],
    className,
  )
}

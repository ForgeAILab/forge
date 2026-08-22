import {
  createContext,
  useContext,
  useState,
  useRef,
  useEffect,
  useCallback,
  type ButtonHTMLAttributes,
  type ReactNode,
  type MouseEvent,
} from 'react'
import { createPortal } from 'react-dom'
import { cn } from '@/lib/cn'

interface DropdownCtx {
  open: boolean
  setOpen: (v: boolean) => void
  triggerRef: React.RefObject<HTMLButtonElement | null>
}

const Ctx = createContext<DropdownCtx>({
  open: false,
  setOpen: () => {},
  triggerRef: { current: null },
})

export function DropdownMenu({ children, className }: { children: ReactNode; className?: string }) {
  const [open, setOpen] = useState(false)
  const triggerRef = useRef<HTMLButtonElement>(null)
  return (
    <Ctx.Provider value={{ open, setOpen, triggerRef }}>
      <div className={cn('relative inline-block', className)}>{children}</div>
    </Ctx.Provider>
  )
}

export function DropdownMenuTrigger({
  children,
  className,
  onClick,
  ...rest
}: ButtonHTMLAttributes<HTMLButtonElement> & { children: ReactNode }) {
  const { open, setOpen, triggerRef } = useContext(Ctx)
  return (
    <button
      ref={triggerRef as React.RefObject<HTMLButtonElement>}
      type="button"
      className={className}
      onClick={(event) => {
        onClick?.(event)
        if (!event.defaultPrevented) setOpen(!open)
      }}
      {...rest}
    >
      {children}
    </button>
  )
}

export function DropdownMenuContent({
  children,
  className,
  align = 'start',
  side = 'bottom',
  anchor = 'trigger',
}: {
  children: ReactNode
  className?: string
  align?: 'start' | 'end'
  side?: 'top' | 'bottom'
  anchor?: 'trigger' | 'page'
}) {
  const { open, setOpen, triggerRef } = useContext(Ctx)
  const ref = useRef<HTMLDivElement>(null)
  const [style, setStyle] = useState<React.CSSProperties>({})

  const updatePosition = useCallback(() => {
    if (anchor === 'page') {
      setStyle({
        position: 'fixed',
        top: 0,
        left: 0,
        zIndex: 50,
      })
      return
    }

    if (!triggerRef.current) return
    const rect = triggerRef.current.getBoundingClientRect()
    const newStyle: React.CSSProperties = {
      position: 'fixed',
      zIndex: 50,
    }
    if (side === 'top') {
      newStyle.bottom = window.innerHeight - rect.top + 4
    } else {
      newStyle.top = rect.bottom + 4
    }
    if (align === 'end') {
      newStyle.right = window.innerWidth - rect.right
    } else {
      newStyle.left = rect.left
    }
    setStyle(newStyle)
  }, [triggerRef, align, side])

  useEffect(() => {
    if (!open) return
    updatePosition()
    const handler = (e: globalThis.MouseEvent) => {
      if (
        ref.current &&
        !ref.current.contains(e.target as Node) &&
        triggerRef.current &&
        !triggerRef.current.contains(e.target as Node)
      ) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open, setOpen, triggerRef, updatePosition])

  if (!open) return null

  return createPortal(
    <div
      ref={ref}
      style={style}
      data-drawer-owned-overlay
      className={cn(
        'min-w-[8rem] overflow-hidden rounded-lg border border-border-subtle bg-popover p-1 text-popover-foreground shadow-float animate-slide-in',
        className,
      )}
    >
      {children}
    </div>,
    document.body,
  )
}

export function DropdownMenuItem({
  children,
  className,
  onClick,
  disabled,
  keepOpen,
}: {
  children: ReactNode
  className?: string
  onClick?: (e: MouseEvent) => void
  disabled?: boolean
  keepOpen?: boolean
}) {
  const { setOpen } = useContext(Ctx)
  return (
    <button
      type="button"
      disabled={disabled}
      className={cn(
        'relative flex w-full cursor-pointer select-none items-center rounded-sm px-2 py-1.5 text-ui outline-none transition-colors hover:bg-accent hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground disabled:pointer-events-none disabled:opacity-50',
        className,
      )}
      onClick={(e) => {
        onClick?.(e)
        if (!keepOpen) setOpen(false)
      }}
    >
      {children}
    </button>
  )
}

export function DropdownMenuSeparator({ className }: { className?: string }) {
  return <div className={cn('-mx-1 my-1 h-px bg-muted', className)} />
}

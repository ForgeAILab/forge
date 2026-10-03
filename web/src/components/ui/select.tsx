import { useCallback, useEffect, useId, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { CaretDown, Check } from '@phosphor-icons/react'
import { cn } from '@/lib/cn'

export type SelectOption = {
  value: string
  label: string
  disabled?: boolean
}

type SelectProps = {
  id?: string
  value: string
  options: SelectOption[]
  onChange: (value: string) => void
  placeholder?: string
  disabled?: boolean
  className?: string
  'aria-label'?: string
  title?: string
}

const MENU_GAP = 4
const MENU_MAX_HEIGHT = 256
const MENU_PADDING = 10
const OPTION_HEIGHT = 32

export function Select({
  id,
  value,
  options,
  onChange,
  placeholder = 'Select...',
  disabled,
  className,
  'aria-label': ariaLabel,
  title,
}: SelectProps) {
  const [open, setOpen] = useState(false)
  const generatedId = useId().replaceAll(':', '')
  const triggerId = id ?? `${generatedId}-trigger`
  const listboxId = `${generatedId}-listbox`
  const triggerRef = useRef<HTMLButtonElement>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const optionRefs = useRef<Array<HTMLButtonElement | null>>([])
  const [style, setStyle] = useState<React.CSSProperties>({})

  const selectedOption = options.find((o) => o.value === value)
  const displayLabel = selectedOption?.label ?? (value || placeholder)
  const showPlaceholder = !selectedOption && !value

  const firstEnabledIndex = options.findIndex((option) => !option.disabled)
  const lastEnabledIndex = options.reduce(
    (lastIndex, option, index) => (option.disabled ? lastIndex : index),
    -1,
  )
  const selectedEnabledIndex = options.findIndex(
    (option) => option.value === value && !option.disabled,
  )

  const focusOption = useCallback(
    (index: number) => {
      if (index < 0 || index >= options.length || options[index]?.disabled) return
      optionRefs.current[index]?.focus()
    },
    [options],
  )

  const nextEnabledIndex = useCallback(
    (index: number, direction: 1 | -1) => {
      if (options.length === 0) return -1
      let next = index + direction
      while (next >= 0 && next < options.length) {
        if (!options[next]?.disabled) return next
        next += direction
      }
      return direction === 1 ? firstEnabledIndex : lastEnabledIndex
    },
    [firstEnabledIndex, lastEnabledIndex, options],
  )

  const closeSelect = useCallback((restoreFocus: boolean) => {
    setOpen(false)
    if (restoreFocus) {
      window.setTimeout(() => triggerRef.current?.focus(), 0)
    }
  }, [])

  const updatePosition = useCallback(() => {
    if (!triggerRef.current) return
    const rect = triggerRef.current.getBoundingClientRect()
    // Open upwards when the menu would not fit below the trigger and there is
    // more room above; either way cap the height to the space available.
    const spaceBelow = window.innerHeight - rect.bottom - MENU_GAP * 2
    const spaceAbove = rect.top - MENU_GAP * 2
    const wanted = Math.min(MENU_MAX_HEIGHT, options.length * OPTION_HEIGHT + MENU_PADDING)
    const above = wanted > spaceBelow && spaceAbove > spaceBelow
    setStyle({
      position: 'fixed',
      zIndex: 9999,
      left: rect.left,
      minWidth: rect.width,
      maxHeight: Math.max(
        OPTION_HEIGHT,
        Math.min(MENU_MAX_HEIGHT, above ? spaceAbove : spaceBelow),
      ),
      ...(above
        ? { bottom: window.innerHeight - rect.top + MENU_GAP }
        : { top: rect.bottom + MENU_GAP }),
    })
  }, [options.length])

  useEffect(() => {
    if (!open) return
    updatePosition()
    const focusTimer = window.setTimeout(() => {
      focusOption(selectedEnabledIndex >= 0 ? selectedEnabledIndex : firstEnabledIndex)
    }, 0)
    const onMouseDown = (e: globalThis.MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target as Node) &&
        triggerRef.current &&
        !triggerRef.current.contains(e.target as Node)
      ) {
        setOpen(false)
      }
    }
    const onScroll = () => updatePosition()
    document.addEventListener('mousedown', onMouseDown)
    window.addEventListener('scroll', onScroll, true)
    return () => {
      window.clearTimeout(focusTimer)
      document.removeEventListener('mousedown', onMouseDown)
      window.removeEventListener('scroll', onScroll, true)
    }
  }, [firstEnabledIndex, focusOption, open, selectedEnabledIndex, updatePosition])

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (disabled) return
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      if (open) closeSelect(true)
      else setOpen(true)
    }
    if (e.key === 'Escape' && open) {
      e.preventDefault()
      closeSelect(true)
    }
    if (e.key === 'ArrowDown' && !open) {
      e.preventDefault()
      setOpen(true)
    }
    if (e.key === 'ArrowUp' && !open) {
      e.preventDefault()
      setOpen(true)
    }
  }

  const handleOptionKeyDown = (e: React.KeyboardEvent, index: number) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      focusOption(nextEnabledIndex(index, 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      focusOption(nextEnabledIndex(index, -1))
    } else if (e.key === 'Home') {
      e.preventDefault()
      focusOption(firstEnabledIndex)
    } else if (e.key === 'End') {
      e.preventDefault()
      focusOption(lastEnabledIndex)
    } else if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      const option = options[index]
      if (!option?.disabled) {
        onChange(option.value)
        closeSelect(true)
      }
    } else if (e.key === 'Escape') {
      e.preventDefault()
      closeSelect(true)
    } else if (e.key === 'Tab') {
      closeSelect(false)
    }
  }

  return (
    <>
      <button
        ref={triggerRef}
        id={triggerId}
        type="button"
        disabled={disabled}
        aria-label={ariaLabel}
        aria-haspopup="listbox"
        aria-controls={listboxId}
        aria-expanded={open}
        title={title}
        className={cn(
          'flex h-9 w-full cursor-pointer items-center justify-between gap-2 rounded-md border border-input bg-background px-3 py-2 text-ui ring-offset-background transition-colors',
          'hover:bg-accent/40 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2',
          'disabled:cursor-not-allowed disabled:opacity-50',
          className,
        )}
        onKeyDown={handleKeyDown}
        onClick={() => {
          if (!disabled) {
            if (!open) {
              updatePosition()
              setOpen(true)
            } else {
              closeSelect(true)
            }
          }
        }}
      >
        <span className={cn('truncate text-left', showPlaceholder && 'text-muted-foreground')}>
          {displayLabel}
        </span>
        <CaretDown
          size={12}
          className={cn(
            'shrink-0 text-muted-foreground transition-transform',
            open && 'rotate-180',
          )}
        />
      </button>

      {open &&
        createPortal(
          <div
            ref={dropdownRef}
            id={listboxId}
            role="listbox"
            aria-labelledby={triggerId}
            style={style}
            className="overflow-y-auto rounded-lg border border-border-subtle bg-popover p-1 text-popover-foreground shadow-float animate-slide-in"
          >
            {options.map((option, index) => (
              <button
                key={option.value}
                ref={(element) => {
                  optionRefs.current[index] = element
                }}
                role="option"
                type="button"
                disabled={option.disabled}
                aria-selected={option.value === value}
                onKeyDown={(event) => handleOptionKeyDown(event, index)}
                className={cn(
                  'relative flex w-full cursor-pointer select-none items-center gap-2 rounded-sm px-2 py-1.5 text-ui outline-none transition-colors',
                  'hover:bg-accent hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground',
                  'disabled:pointer-events-none disabled:opacity-40',
                  option.value === value && 'bg-accent/50',
                )}
                onClick={() => {
                  onChange(option.value)
                  closeSelect(true)
                }}
              >
                <Check
                  size={12}
                  className={cn('shrink-0', option.value === value ? 'opacity-100' : 'opacity-0')}
                />
                <span className="truncate">{option.label}</span>
              </button>
            ))}
          </div>,
          document.body,
        )}
    </>
  )
}

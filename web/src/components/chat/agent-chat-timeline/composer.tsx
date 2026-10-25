// Agent Chat commands and message composer.

import { useEffect, useId, useRef, useState } from 'react'
import { CircleNotch, PaperPlaneTilt } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { cn } from '@/lib/cn'

export type ChatCommand = {
  name: string
  description: string
  run: (argument: string) => Promise<void>
}

/** The `/command` currently being typed, or null when the draft is not one. */
function parseCommandToken(draft: string): string | null {
  const match = /^\/([\w-]*)$/.exec(draft.trimStart())
  return match ? match[1].toLowerCase() : null
}

export function ChatComposer({
  disabled,
  disabledReason,
  isSending,
  onSend,
  commands = [],
  placeholder = 'Message this agent…',
}: {
  disabled?: boolean
  disabledReason?: string
  isSending?: boolean
  onSend: (content: string) => Promise<void>
  commands?: ChatCommand[]
  placeholder?: string
}) {
  const [content, setContent] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [commandRunning, setCommandRunning] = useState(false)
  const [menuDismissed, setMenuDismissed] = useState(false)
  const [activeCommand, setActiveCommand] = useState(0)
  const formRef = useRef<HTMLFormElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const statusId = useId()
  const describedBy = [disabledReason ? `${statusId}-reason` : null, error ? `${statusId}-error` : null]
    .filter(Boolean)
    .join(' ')
  const busy = isSending || commandRunning

  const commandQuery = menuDismissed ? null : parseCommandToken(content)
  const menuRows =
    commandQuery !== null ? commands.filter((command) => command.name.startsWith(commandQuery)) : []
  const menuOpen = menuRows.length > 0

  useEffect(() => {
    setActiveCommand(0)
  }, [commandQuery])

  function pickCommand(command: ChatCommand) {
    setContent(`/${command.name} `)
    setMenuDismissed(true)
    textareaRef.current?.focus()
  }

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    const value = content.trim()
    if (!value || disabled || busy) return
    setError(null)
    const commandMatch = /^\/([\w-]+)\s*([\s\S]*)$/.exec(value)
    if (commandMatch && commands.length > 0) {
      const command = commands.find(
        (candidate) => candidate.name === commandMatch[1].toLowerCase(),
      )
      if (!command) {
        setError(`Unknown command /${commandMatch[1]}.`)
        return
      }
      setCommandRunning(true)
      try {
        await command.run(commandMatch[2].trim())
        setContent('')
      } catch (cause) {
        setError(
          cause instanceof Error ? cause.message : `The /${command.name} command failed.`,
        )
      } finally {
        setCommandRunning(false)
      }
      return
    }
    try {
      await onSend(value)
      setContent('')
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The message could not be sent.')
    }
  }

  return (
    <form
      ref={formRef}
      onSubmit={submit}
      className="shrink-0 border-t border-border-subtle bg-background px-4 pb-4 pt-3 sm:px-5"
    >
      <div className="mx-auto w-full max-w-3xl">
        {disabledReason ? (
          <p id={`${statusId}-reason`} className="mb-2 text-xs text-muted-foreground" role="status">
            {disabledReason}
          </p>
        ) : null}
        {error ? (
          <p id={`${statusId}-error`} className="mb-2 text-xs text-destructive" role="alert">
            {error}
          </p>
        ) : null}
        <div className="relative">
          {menuOpen ? (
            <div
              role="listbox"
              aria-label="Commands"
              className="absolute inset-x-0 bottom-full z-10 mb-2 rounded-xl border border-border bg-card p-1 shadow-md"
            >
              {menuRows.map((command, index) => (
                <button
                  key={command.name}
                  type="button"
                  role="option"
                  aria-selected={index === activeCommand}
                  onMouseDown={(event) => event.preventDefault()}
                  onMouseEnter={() => setActiveCommand(index)}
                  onClick={() => pickCommand(command)}
                  className={cn(
                    'flex w-full items-center gap-2.5 rounded-lg px-2.5 py-2 text-left transition-colors',
                    index === activeCommand && 'bg-muted/60',
                  )}
                >
                  <span className="shrink-0 font-mono text-xs font-medium text-foreground">
                    /{command.name}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-xs text-muted-foreground">
                    {command.description}
                  </span>
                </button>
              ))}
              <div className="mt-1 border-t border-border-subtle px-2.5 pb-1 pt-1.5 text-micro text-muted-foreground">
                ↑↓ to navigate · Enter to insert · Esc to dismiss
              </div>
            </div>
          ) : null}
          <div
            role="presentation"
            onClick={() => textareaRef.current?.focus()}
            className="flex cursor-text flex-col gap-1 rounded-2xl border border-border bg-card p-2 shadow-xs transition-colors focus-within:border-primary/40"
          >
            <Textarea
              ref={textareaRef}
              value={content}
              onChange={(event) => {
                setContent(event.target.value)
                setMenuDismissed(false)
              }}
              onKeyDown={(event) => {
                if (menuOpen) {
                  if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
                    event.preventDefault()
                    setActiveCommand(
                      (current) =>
                        (current + (event.key === 'ArrowDown' ? 1 : menuRows.length - 1)) %
                        menuRows.length,
                    )
                    return
                  }
                  if ((event.key === 'Enter' && !event.shiftKey) || event.key === 'Tab') {
                    event.preventDefault()
                    pickCommand(menuRows[activeCommand])
                    return
                  }
                  if (event.key === 'Escape') {
                    setMenuDismissed(true)
                    return
                  }
                }
                if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent.isComposing) return
                event.preventDefault()
                formRef.current?.requestSubmit()
              }}
              placeholder={placeholder}
              rows={2}
              disabled={disabled || busy}
              aria-label="Chat message"
              aria-describedby={describedBy || undefined}
              className="min-h-[40px] resize-none border-0 bg-transparent px-2 py-1.5 shadow-none focus-visible:ring-0"
            />
            <div className="flex items-center justify-between gap-2 px-1 pb-0.5">
              <span className="text-micro text-muted-foreground">
                Enter to send · Shift+Enter for a new line
                {commands.length > 0 ? ' · / for commands' : ''}
              </span>
              <Button
                type="submit"
                size="icon"
                disabled={disabled || busy || !content.trim()}
                aria-label={busy ? 'Sending message' : 'Send message'}
                className="h-8 w-8 rounded-full"
              >
                {busy ? (
                  <CircleNotch size={15} className="animate-spin" aria-hidden />
                ) : (
                  <PaperPlaneTilt size={15} aria-hidden />
                )}
              </Button>
            </div>
          </div>
        </div>
      </div>
    </form>
  )
}

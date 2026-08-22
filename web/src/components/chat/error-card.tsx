import { useState } from 'react'
import {
  ArrowClockwise,
  CaretDown,
  CaretRight,
  CircleNotch,
  Warning,
  WarningCircle,
  WarningOctagon,
} from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/cn'

export type ErrorCardSeverity = 'error' | 'conflict' | 'warning'

export type ErrorCardAction = {
  label: string
  onClick: () => void
  isPending?: boolean
}

export type ErrorCardProps = {
  title: string
  description?: string
  severity?: ErrorCardSeverity
  action?: ErrorCardAction
  technicalDetails?: unknown
  className?: string
}

export type SafeErrorDetail = {
  label: string
  value: string
}

export type SafeErrorSummary = {
  message?: string
  details: SafeErrorDetail[]
  isStructured: boolean
}

const MAX_SAFE_TEXT_LENGTH = 240
const MAX_SAFE_DETAILS = 12

const safeFieldLabels: Record<string, string> = {
  safe_message: 'Message',
  safeMessage: 'Message',
  status: 'Status',
  code: 'Code',
  operation: 'Operation',
  correlation_id: 'Correlation',
  request_id: 'Request ID',
  replayed: 'Replay',
  receipt_id: 'Receipt',
  event_id: 'Event',
  attempt: 'Attempt',
  attempt_count: 'Attempt',
  max_attempts: 'Max attempts',
  next_attempt_at: 'Next attempt',
  after_seconds: 'Retry after',
  retryable: 'Retryable',
  action: 'Next action',
  authority_domain: 'Authority',
  expected_version: 'Expected version',
  current_version: 'Current version',
  expected_revision: 'Expected revision',
  current_revision: 'Current revision',
  resource_type: 'Resource',
  resource_id: 'Resource ID',
  target_type: 'Target',
  target_id: 'Target ID',
  version: 'Version',
  revision_id: 'Revision',
  revision: 'Revision',
  content_digest: 'Content digest',
  rendered_digest: 'Rendered digest',
  scope_type: 'Scope',
  scope_id: 'Scope ID',
  policy_result: 'Policy',
  requirement_type: 'Requirement',
  role: 'Role',
  capability: 'Capability',
}

const nestedSafeKeys = [
  'response',
  'outcome',
  'retry',
  'approval_target',
  'current_version_or_revision',
  'scope',
]

function boundedText(value: string): string | undefined {
  const trimmed = value.trim()
  if (!trimmed) return undefined
  return trimmed.length > MAX_SAFE_TEXT_LENGTH
    ? `${trimmed.slice(0, MAX_SAFE_TEXT_LENGTH - 1)}…`
    : trimmed
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined
}

function parseJson(value: string): unknown {
  const trimmed = value.trim()
  if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return undefined
  try {
    return JSON.parse(trimmed) as unknown
  } catch {
    return undefined
  }
}

function scalarText(value: unknown): string | undefined {
  if (typeof value === 'string') return boundedText(value)
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return String(value)
  }
  return undefined
}

function collectRecords(value: unknown): Record<string, unknown>[] {
  const records: Record<string, unknown>[] = []
  const seen = new Set<object>()

  function visit(candidate: unknown, depth: number) {
    if (depth > 3 || candidate === null || candidate === undefined) return
    if (typeof candidate === 'string') {
      const parsed = parseJson(candidate)
      if (parsed !== undefined) visit(parsed, depth + 1)
      return
    }
    if (Array.isArray(candidate)) return
    const record = asRecord(candidate)
    if (!record || seen.has(record)) return
    seen.add(record)
    records.push(record)
    for (const key of nestedSafeKeys) {
      if (record[key] !== undefined) visit(record[key], depth + 1)
    }
  }

  if (value instanceof Error) visit(value.message, 0)
  visit(value, 0)
  return records
}

function safeMessageFromRecords(records: Record<string, unknown>[]): string | undefined {
  for (const record of records) {
    for (const key of ['safe_message', 'safeMessage']) {
      const message = scalarText(record[key])
      if (message) return message
    }
  }

  // A plain error object may provide a user-facing message, but once the
  // root is an outcome envelope every other message/cause field is
  // untrusted. Never walk nested `details`/`error` records looking for prose.
  const root = records[0]
  const isOutcomeEnvelope =
    root &&
    ['status', 'operation', 'scope', 'result', 'approval_target', 'setup_requirements'].some(
      (key) => root[key] !== undefined,
    )
  if (!isOutcomeEnvelope) {
    const message = scalarText(root?.message)
    if (message && parseJson(message) === undefined) return message
  }

  return undefined
}

/** Return only the server-authorized safe_message field, never a raw message/cause. */
export function getSafeErrorMessage(value: unknown): string | undefined {
  return safeMessageFromRecords(collectRecords(value))
}

function addSafeDetail(
  details: SafeErrorDetail[],
  seen: Set<string>,
  label: string,
  value: unknown,
) {
  if (details.length >= MAX_SAFE_DETAILS) return
  const text = scalarText(value)
  if (!text) return
  const key = `${label}:${text}`
  if (seen.has(key)) return
  seen.add(key)
  details.push({ label, value: text })
}

/**
 * Extract the bounded fields the server explicitly authorizes for a user-facing
 * outcome. Protected causes, result payloads, and unknown persistence fields are
 * intentionally ignored rather than serialized for display.
 */
export function getSafeErrorDetails(value: unknown): SafeErrorSummary {
  const records = collectRecords(value)
  const details: SafeErrorDetail[] = []
  const seen = new Set<string>()
  const directMessage =
    typeof value === 'string' && parseJson(value) === undefined
      ? boundedText(value)
      : value instanceof Error && parseJson(value.message) === undefined
        ? boundedText(value.message)
        : undefined
  const message = safeMessageFromRecords(records) ?? directMessage
  const isStructured = records.some((record) =>
    ['status', 'operation', 'scope', 'result', 'approval_target', 'setup_requirements'].some(
      (key) => record[key] !== undefined,
    ),
  )

  for (const record of records) {
    for (const [key, label] of Object.entries(safeFieldLabels)) {
      addSafeDetail(details, seen, label, record[key])
    }

    const requirements = record.setup_requirements
    if (Array.isArray(requirements)) {
      const summary = requirements
        .map((requirement) => {
          const item = asRecord(requirement)
          if (!item) return undefined
          return [item.requirement_type, item.role, item.action]
            .map(scalarText)
            .filter((part): part is string => Boolean(part))
            .join(' · ')
        })
        .filter((item): item is string => Boolean(item))
        .slice(0, 4)
        .join(', ')
      addSafeDetail(details, seen, 'Setup requirements', summary)
    }
  }

  return { message, details, isStructured }
}

function severityStyles(severity: ErrorCardSeverity) {
  switch (severity) {
    case 'conflict':
      return {
        card: 'border-warning/30 bg-warning/5 text-foreground',
        icon: <Warning className="h-4 w-4 shrink-0 text-warning" aria-hidden />,
        badge: 'border-warning/30 bg-warning/10 text-warning',
        badgeText: 'Conflict',
      }
    case 'warning':
      return {
        card: 'border-warning/30 bg-warning/5 text-foreground',
        icon: <WarningCircle className="h-4 w-4 shrink-0 text-warning" aria-hidden />,
        badge: 'border-warning/30 bg-warning/10 text-warning',
        badgeText: 'Warning',
      }
    case 'error':
    default:
      return {
        card: 'border-destructive/30 bg-destructive/5 text-foreground',
        icon: <WarningOctagon className="h-4 w-4 shrink-0 text-destructive" aria-hidden />,
        badge: 'border-destructive/30 bg-destructive/10 text-destructive',
        badgeText: 'Error',
      }
  }
}

export function ErrorCard({
  title,
  description,
  severity = 'error',
  action,
  technicalDetails,
  className,
}: ErrorCardProps) {
  const [detailsOpen, setDetailsOpen] = useState(false)
  const styles = severityStyles(severity)
  // Error instances may carry provider/database response bodies in their
  // enumerable fields. Callers should pass the authorized response envelope
  // when they want inspectable details; never inspect an Error object here.
  const safeSummary =
    technicalDetails instanceof Error
      ? { message: undefined, details: [], isStructured: false }
      : getSafeErrorDetails(technicalDetails)
  const details = safeSummary.details.filter(
    (detail) =>
      !description?.trim() || detail.label !== 'Message' || detail.value !== description.trim(),
  )
  const hasTechnicalDetails = details.length > 0 || (!description && Boolean(safeSummary.message))
  const renderedDetails =
    details.length > 0
      ? details
      : safeSummary.message
        ? [{ label: 'Message', value: safeSummary.message }]
        : []

  return (
    <div
      role="alert"
      className={cn(
        'animate-pop-in flex flex-col gap-3 rounded-lg border p-3.5 shadow-xs transition-colors',
        styles.card,
        className,
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-start gap-2.5 min-w-0">
          <div className="mt-0.5">{styles.icon}</div>
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <h4 className="text-xs font-semibold text-foreground tracking-tight">{title}</h4>
              <span
                className={cn(
                  'inline-flex items-center rounded px-1.5 py-0.5 text-micro font-medium uppercase tracking-wider',
                  styles.badge,
                )}
              >
                {styles.badgeText}
              </span>
            </div>
            {description ? (
              <p className="mt-1 text-xs text-muted-foreground leading-relaxed">{description}</p>
            ) : null}
          </div>
        </div>
        {action ? (
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={action.onClick}
            disabled={action.isPending}
            className="shrink-0 h-7 text-xs font-medium gap-1.5"
          >
            {action.isPending ? (
              <CircleNotch size={13} className="animate-spin" aria-hidden />
            ) : (
              <ArrowClockwise size={13} aria-hidden />
            )}
            {action.label}
          </Button>
        ) : null}
      </div>

      {hasTechnicalDetails ? (
        <details
          className="mt-1 rounded-md border border-border-subtle bg-background/50 text-xs"
          open={detailsOpen}
          onToggle={(e) => setDetailsOpen(e.currentTarget.open)}
        >
          <summary className="flex cursor-pointer items-center gap-1.5 px-2.5 py-1.5 text-micro font-medium text-muted-foreground hover:text-foreground select-none">
            {detailsOpen ? <CaretDown size={12} /> : <CaretRight size={12} />}
            <span>Technical details</span>
          </summary>
          <div className="border-t border-border-subtle p-2.5">
            <dl className="grid min-w-0 gap-x-4 gap-y-2 sm:grid-cols-2">
              {renderedDetails.map((detail) => (
                <div key={`${detail.label}:${detail.value}`} className="min-w-0">
                  <dt className="font-mono text-micro font-semibold uppercase tracking-[0.08em] text-muted-foreground">
                    {detail.label}
                  </dt>
                  <dd className="mt-1 break-words font-mono text-micro text-foreground/90">
                    {detail.value}
                  </dd>
                </div>
              ))}
            </dl>
          </div>
        </details>
      ) : null}
    </div>
  )
}

// Provider pricing request and display utilities.

export function newPricingIdempotencyKey(prefix: string): string {
  const randomUuid =
    typeof crypto !== 'undefined' && 'randomUUID' in crypto ? crypto.randomUUID() : null
  return `${prefix}:${randomUuid ?? `${Date.now()}`}`
}

export function boundedErrorMessage(cause: unknown, fallback: string): string {
  const message = cause instanceof Error ? cause.message : fallback
  const compact = message.replace(/\s+/g, ' ').trim()
  return compact.length > 160 ? `${compact.slice(0, 157)}…` : compact
}

export function timestampLabel(value: string | null): string {
  return value ?? 'Not available'
}

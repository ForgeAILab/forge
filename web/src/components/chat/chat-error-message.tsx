import { ErrorCard, getSafeErrorDetails, getSafeErrorMessage } from '@/components/chat/error-card'
import type { ChatErrorEntry } from '@/components/chat/types'

type ChatErrorMessageProps = {
  entry: ChatErrorEntry
  onRetry?: () => void
}

export function ChatErrorMessage({ entry, onRetry }: ChatErrorMessageProps) {
  const payloadSummary = getSafeErrorDetails(entry.payload)
  const messageSummary = getSafeErrorDetails(entry.message)
  const description =
    getSafeErrorMessage(entry.payload) ??
    (payloadSummary.isStructured ? undefined : messageSummary.message) ??
    'The agent turn could not complete.'

  return (
    <ErrorCard
      title={entry.title || 'Turn execution failed'}
      description={description}
      severity="error"
      action={onRetry ? { label: 'Retry', onClick: onRetry } : undefined}
      technicalDetails={entry.payload}
    />
  )
}

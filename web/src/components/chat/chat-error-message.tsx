import { ErrorCard } from '@/components/chat/error-card'
import type { ChatErrorEntry } from '@/components/chat/types'

type ChatErrorMessageProps = {
  entry: ChatErrorEntry
  onRetry?: () => void
}

export function ChatErrorMessage({ entry, onRetry }: ChatErrorMessageProps) {
  return (
    <ErrorCard
      title={entry.title || 'Turn execution failed'}
      description={entry.message || 'The agent turn could not complete.'}
      severity="error"
      action={onRetry ? { label: 'Retry', onClick: onRetry } : undefined}
      technicalDetails={entry.payload}
    />
  )
}


import { Button } from '@/components/ui/button'
import { getApiErrorMessage, isApiStatus } from '@/lib/api-error'

export function ErrorBanner({
  error,
  fallback,
  onRetry,
  showRetry,
}: {
  error: unknown
  fallback?: string
  onRetry?: () => void
  showRetry?: boolean
}) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">
      <p>{getApiErrorMessage(error, fallback)}</p>
      {onRetry && (showRetry ?? isApiStatus(error, 503)) ? (
        <Button size="sm" variant="outline" onClick={onRetry}>
          Retry
        </Button>
      ) : null}
    </div>
  )
}

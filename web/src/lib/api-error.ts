import { toast } from 'sonner'
import { ApiError } from '@/api/client'

export function getApiErrorMessage(error: unknown, fallback = 'Request failed'): string {
  if (error instanceof ApiError) {
    let message = error.message || fallback
    let requestId = error.requestId
    if (error.response?.message) message = error.response.message
    if (error.response?.request_id) requestId = error.response.request_id
    try {
      const parsed = JSON.parse(error.message) as {
        message?: unknown
        request_id?: unknown
      }
      if (typeof parsed.message === 'string' && parsed.message) {
        message = parsed.message
      }
      if (typeof parsed.request_id === 'string' && parsed.request_id) {
        requestId = parsed.request_id
      }
    } catch {
      // Non-JSON error bodies are already usable as-is.
    }
    return `${message}${requestId ? ` Request ID: ${requestId}` : ''}`
  }
  if (error instanceof Error) return error.message
  return fallback
}

export function getApiErrorCode(error: unknown): string | undefined {
  if (!(error instanceof ApiError)) return undefined
  if (error.code) return error.code
  try {
    const parsed = JSON.parse(error.message) as { code?: unknown }
    return typeof parsed.code === 'string' ? parsed.code : undefined
  } catch {
    return undefined
  }
}

export type ApiConflictDetails = Record<string, unknown>

function isRecord(value: unknown): value is ApiConflictDetails {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/** Return server-provided conflict metadata without exposing arbitrary error bodies in the UI. */
export function getApiConflictDetails(error: unknown): ApiConflictDetails | undefined {
  if (!(error instanceof ApiError)) return undefined
  if (isRecord(error.details)) return error.details
  try {
    const parsed = JSON.parse(error.message) as { details?: unknown }
    return isRecord(parsed.details) ? parsed.details : undefined
  } catch {
    return undefined
  }
}

export function toastApiError(error: unknown, fallback?: string): void {
  toast.error(getApiErrorMessage(error, fallback))
}

export function isApiStatus(error: unknown, status: number): boolean {
  return error instanceof ApiError && error.status === status
}

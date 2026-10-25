// Pagination and API error types.

// --- Paginated wrapper (matches api_types::PaginatedResponse<T>) ---

export interface PaginatedResponse<T> {
  items: T[]
  next_cursor?: string | null
  has_more: boolean
  total_count?: number | null
}

// --- Error (matches api_types::ErrorResponse) ---

export interface ErrorResponse {
  code: string
  message: string
  details?: unknown | null
  request_id: string
}

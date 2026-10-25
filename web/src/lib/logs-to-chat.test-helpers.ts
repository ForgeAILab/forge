// Log entry fixtures shared by log conversion tests.

import type { LogEntry } from '@/types/generated'

export function log(
  kind: LogEntry['kind'] | string,
  payload: unknown,
  sequence: number,
  overrides: Partial<LogEntry> = {},
): LogEntry {
  return {
    schema_version: 1,
    sequence,
    timestamp: `2026-04-15T00:00:${String(sequence).padStart(2, '0')}Z`,
    execution_id: 'exec-1',
    kind: kind as LogEntry['kind'],
    stream: 'main',
    payload,
    truncated: false,
    ...overrides,
  }
}

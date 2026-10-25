// Log file change conversion and diff counts.

import type { LogEntry } from '@/types/generated'
import {
  type FileEditEntry,
  asRecord,
  stringField,
  entryBase,
  shortPath,
  type PayloadRecord,
} from './shared'

export function fileEditEntry(log: LogEntry, payload: unknown): FileEditEntry {
  const record = asRecord(payload)
  const diff = stringField(record, 'diff')
  const path = stringField(record, 'path')
  const counts = diff ? diffCounts(diff) : undefined

  return {
    ...entryBase(log),
    kind: 'file_edit',
    path: path ? shortPath(path) : 'unknown',
    action: fileAction(record),
    newPath: stringField(record, 'new_path') ?? stringField(record, 'newPath'),
    diff,
    before: stringField(record, 'before'),
    after: stringField(record, 'after'),
    additions: counts?.additions,
    deletions: counts?.deletions,
    payload,
  }
}

function fileAction(record: PayloadRecord | undefined): FileEditEntry['action'] {
  const action = (
    stringField(record, 'op') ??
    stringField(record, 'action') ??
    stringField(asRecord(record?.kind), 'type') ??
    'edit'
  ).toLowerCase()

  if (
    action === 'edit' ||
    action === 'update' ||
    action === 'write' ||
    action === 'add' ||
    action === 'delete' ||
    action === 'rename'
  ) {
    if (action === 'update') return 'edit'
    if (action === 'add') return 'write'
    return action
  }

  return 'edit'
}

function diffCounts(diff: string): { additions: number; deletions: number } {
  return diff.split('\n').reduce(
    (counts, line) => {
      if (line.startsWith('+') && !line.startsWith('+++')) {
        counts.additions += 1
      }
      if (line.startsWith('-') && !line.startsWith('---')) {
        counts.deletions += 1
      }
      return counts
    },
    { additions: 0, deletions: 0 },
  )
}

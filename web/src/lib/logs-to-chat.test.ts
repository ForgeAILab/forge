import { log } from './logs-to-chat.test-helpers'
import { describe, expect, it } from 'vitest'
import { logsToChatEntries } from './logs-to-chat'

describe('logsToChatEntries', () => {
  it('renders turn divider system logs as lightweight dividers', () => {
    const entries = logsToChatEntries([
      log('system', { type: 'turn_divider', label: 'Follow-up' }, 1),
    ])

    expect(entries).toEqual([
      {
        kind: 'divider',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        label: 'Follow-up',
      },
    ])
  })

  it('1. Single assistant message', () => {
    const entries = logsToChatEntries([log('assistant', { text: 'Hello' }, 1)])

    expect(entries).toEqual([
      {
        kind: 'assistant',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        text: 'Hello',
      },
    ])
  })

  it('5. Consecutive stdout run grouping', () => {
    const entries = logsToChatEntries([
      log('stdout', 'one', 1),
      log('stderr', 'two', 2),
      log('stdout', 'three', 3),
    ])

    expect(entries).toEqual([
      {
        kind: 'shell_output',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        lines: [
          { stream: 'stdout', text: 'one' },
          { stream: 'stderr', text: 'two' },
          { stream: 'stdout', text: 'three' },
        ],
      },
    ])
  })

  it('6. shell_command + stdout folding', () => {
    const entries = logsToChatEntries([
      log('shell_command', { command: 'pnpm test', cwd: '/repo/web' }, 1),
      log('stdout', 'ok', 2),
      log('stderr', 'warn', 3),
    ])

    expect(entries).toEqual([
      {
        kind: 'shell_output',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        command: 'pnpm test',
        cwd: '/repo/web',
        lines: [
          { stream: 'stdout', text: 'ok' },
          { stream: 'stderr', text: 'warn' },
        ],
      },
    ])
  })

  it('7. assistant_delta coalescing (3 deltas -> single entry with concatenated text)', () => {
    const entries = logsToChatEntries([
      log('assistant_delta', { text: 'Hel' }, 1),
      log('assistant_delta', { text: 'lo' }, 2),
      log('assistant_delta', '!', 3),
    ])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'assistant',
      sequence: 1,
      timestamp: '2026-04-15T00:00:01Z',
      text: 'Hello!',
      isStreaming: true,
    })
  })

  it('7b. final assistant event replaces matching streamed delta message', () => {
    const entries = logsToChatEntries([
      log('assistant_delta', { delta: 'Hel', itemId: 'msg-1' }, 1),
      log('assistant_delta', { delta: 'lo', itemId: 'msg-1' }, 2),
      log('assistant', { text: 'Hello', itemId: 'msg-1' }, 3),
    ])

    expect(entries).toEqual([
      {
        kind: 'assistant',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        text: 'Hello',
      },
    ])
  })

  it('8. approval_question rendering', () => {
    const payload = {
      question: 'Proceed?',
      decision: 'approved',
      rationale: 'Looks safe',
    }
    const entries = logsToChatEntries([log('approval_question', payload, 1)])

    expect(entries[0]).toMatchObject({
      kind: 'approval',
      question: 'Proceed?',
      decision: 'approved',
      rationale: 'Looks safe',
      payload,
    })
  })

  it('9. file_change with diff payload (additions/deletions counted)', () => {
    const diff = [
      '--- a/file.ts',
      '+++ b/file.ts',
      ' unchanged',
      '+added',
      '-removed',
      '+again',
    ].join('\n')
    const entries = logsToChatEntries([
      log('file_change', { path: 'file.ts', op: 'edit', diff }, 1),
    ])

    expect(entries[0]).toMatchObject({
      kind: 'file_edit',
      path: 'file.ts',
      action: 'edit',
      diff,
      additions: 2,
      deletions: 1,
    })
  })

  it('10. file_change with before/after payload (no diff)', () => {
    const entries = logsToChatEntries([
      log('file_change', { path: 'file.ts', action: 'write', before: 'old', after: 'new' }, 1),
    ])

    expect(entries[0]).toMatchObject({
      kind: 'file_edit',
      path: 'file.ts',
      action: 'write',
      before: 'old',
      after: 'new',
    })
    expect(entries[0]).not.toHaveProperty('diff', expect.any(String))
    expect(entries[0]).not.toHaveProperty('additions', expect.any(Number))
    expect(entries[0]).not.toHaveProperty('deletions', expect.any(Number))
  })

  it('11. unknown kind fallback', () => {
    const payload = { raw: true }
    const entries = logsToChatEntries([log('future_kind', payload, 1)])

    expect(entries).toEqual([
      {
        kind: 'system',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        title: 'Unknown',
        payload,
      },
    ])
  })

  it('12. heartbeat filtering (default off, opt-in on)', () => {
    const logs = [
      log('assistant', { text: 'hidden' }, 1, { stream: 'heartbeat' }),
      log('user', { text: 'visible' }, 2),
    ]

    expect(logsToChatEntries(logs)).toEqual([
      {
        kind: 'user',
        sequence: 2,
        timestamp: '2026-04-15T00:00:02Z',
        text: 'visible',
      },
    ])
    expect(logsToChatEntries(logs, { showHeartbeats: true })).toHaveLength(2)
  })

  it('12b. hiddenKinds filters shell and session noise', () => {
    const entries = logsToChatEntries(
      [
        log('assistant', { text: 'visible' }, 1),
        log('session_info', { thread_id: 'thread-1' }, 2),
        log('shell_command', { command: 'pnpm test' }, 3),
        log('stdout', 'ok', 4),
        log('stderr', 'warn', 5),
      ],
      { hiddenKinds: ['session_info', 'shell_command', 'stdout', 'stderr'] },
    )

    expect(entries).toEqual([
      {
        kind: 'assistant',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        text: 'visible',
      },
    ])
  })
})

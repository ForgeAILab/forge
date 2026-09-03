import { describe, expect, it } from 'vitest'

import { logsToChatEntries } from './logs-to-chat'
import type { LogEntry } from '@/types/generated'

function log(
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

  it('2. tool_call + tool_result pairing by call_id', () => {
    const result = { call_id: 'call-1', success: true, output: 'ok' }
    const entries = logsToChatEntries([
      log('tool_call', { tool: 'read', call_id: 'call-1', params: { path: 'a' } }, 1),
      log('tool_result', result, 2),
    ])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'tool_call',
      toolName: 'read',
      callId: 'call-1',
      input: { path: 'a' },
      result,
      status: 'success',
    })
  })

  it('2b. tool_call input preview takes priority over params when present', () => {
    const payload = {
      call_id: 'call-1',
      name: 'forge_task_command',
      argument_keys: ['command', 'timeout_secs'],
      input: { command: 'cargo test -p db', timeout_secs: '120' },
    }
    const entries = logsToChatEntries([log('tool_call', payload, 1)])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'tool_call',
      toolName: 'forge_task_command',
      callId: 'call-1',
      input: { command: 'cargo test -p db', timeout_secs: '120' },
      status: 'pending',
    })
  })

  it('2c. tool_call falls back to argument_keys when input did not survive filtering', () => {
    const payload = {
      call_id: 'call-1',
      name: 'forge_task_command',
      argument_keys: ['command'],
    }
    const entries = logsToChatEntries([log('tool_call', payload, 1)])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'tool_call',
      toolName: 'forge_task_command',
      input: payload,
    })
  })

  it('3. tool_call + tool_result pairing by proximity (no call_id)', () => {
    const result = { success: false, error: 'nope' }
    const entries = logsToChatEntries([
      log('tool_call', { name: 'write', params: { path: 'a' } }, 1),
      log('tool_result', result, 2),
    ])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'tool_call',
      toolName: 'write',
      result,
      status: 'failed',
    })
  })

  it('3b. tool_result summary drives status and label over the generic success heuristic', () => {
    const result = {
      call_id: 'call-1',
      name: 'forge_project_orchestration_propose',
      is_error: true,
      success: false,
      summary: {
        status: 'failed',
        code: 'version_conflict',
        safe_message: 'the authorized resource changed; refresh current state and retry',
        retryable: true,
        recovery_action: 'refresh_and_retry',
        correlation_id: 'corr-1',
      },
    }
    const entries = logsToChatEntries([
      log('tool_call', { tool: 'forge_project_orchestration_propose', call_id: 'call-1' }, 1),
      log('tool_result', result, 2),
    ])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'tool_call',
      status: 'failed',
      resultLabel: 'the authorized resource changed; refresh current state and retry',
    })
  })

  it('3c. tool_result summary never leaks a redacted internal cause into the label', () => {
    // The durable log entry only ever carries the bounded summary fields, so
    // there is nothing for the tool card to accidentally surface here — this
    // guards the frontend contract even though redaction itself happens
    // server-side before the entry is written.
    const result = {
      call_id: 'call-1',
      name: 'forge_task_command',
      is_error: true,
      success: false,
      summary: {
        status: 'failed',
        code: 'internal_failure',
        safe_message: 'the tool call did not complete successfully',
        retryable: false,
        correlation_id: 'call-1',
      },
    }
    const entries = logsToChatEntries([log('tool_result', result, 1)])

    const entry = entries.find((candidate) => candidate.kind === 'tool_call')
    expect(entry).toMatchObject({
      status: 'failed',
      resultLabel: 'the tool call did not complete successfully',
    })
    expect(JSON.stringify(entry)).not.toContain('SECRET')
  })

  it('4. Orphan tool_result without a tool name -> standalone ChatSystemEntry', () => {
    const payload = { success: true, output: 'late' }
    const entries = logsToChatEntries([log('tool_result', payload, 1)])

    expect(entries).toEqual([
      {
        kind: 'system',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        title: 'Tool Result',
        payload,
      },
    ])
  })

  it('4b. Orphan tool_result with a tool name renders as a tool entry', () => {
    const payload = {
      tool: 'forge_list_tasks',
      call_id: 'call-1',
      success: true,
      output: '2 tasks',
    }
    const entries = logsToChatEntries([log('tool_result', payload, 1)])

    expect(entries).toEqual([
      {
        kind: 'tool_call',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        toolName: 'forge_list_tasks',
        callId: 'call-1',
        result: payload,
        resultLabel: '2 tasks',
        status: 'success',
      },
    ])
  })

  it('4c. Tool result labels render text content arrays without object coercion', () => {
    const payload = {
      call_id: 'call-1',
      success: true,
      content: [
        { type: 'text', text: 'The task has been cancelled.' },
        { type: 'text', text: ' agentId: abc123' },
      ],
    }
    const entries = logsToChatEntries([
      log('tool_call', { name: 'Agent', call_id: 'call-1', params: {} }, 1),
      log('tool_result', payload, 2),
    ])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'tool_call',
      toolName: 'Agent',
      resultLabel: 'The task has been cancelled. agentId: abc123',
      status: 'success',
    })
    expect(JSON.stringify(entries)).not.toContain('[object Object]')
  })

  it('4d. MCP elicitation responses render as accepted tool calls', () => {
    const payload = {
      type: 'mcp_elicitation_response',
      decision: 'accept',
      params: {
        message: 'Allow the mcp_router MCP server to run tool "forge_create_task"?',
        _meta: {
          codex_approval_kind: 'mcp_tool_call',
          tool_params: {
            project_id: 'project-1',
            title: 'Create me',
          },
        },
      },
    }
    const entries = logsToChatEntries([log('tool_call', payload, 1)])

    expect(entries).toEqual([
      {
        kind: 'tool_call',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        toolName: 'forge_create_task',
        input: {
          project_id: 'project-1',
          title: 'Create me',
        },
        result: payload,
        status: 'success',
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

  it('12c. hidden session_info still renders derived tool calls', () => {
    const entries = logsToChatEntries(
      [
        log(
          'session_info',
          {
            method: 'item/started',
            params: {
              item: {
                id: 'call-1',
                type: 'toolCall',
                name: 'forge_list_tasks',
                input: { project_id: 'project-1' },
                status: 'inProgress',
              },
            },
          },
          1,
        ),
        log(
          'session_info',
          {
            method: 'item/completed',
            params: {
              item: {
                id: 'call-1',
                type: 'toolResult',
                name: 'forge_list_tasks',
                output: '2 tasks',
                status: 'completed',
              },
            },
          },
          2,
        ),
      ],
      { hiddenKinds: ['session_info', 'shell_command', 'stdout', 'stderr'] },
    )

    expect(entries).toEqual([
      {
        kind: 'tool_call',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        toolName: 'forge_list_tasks',
        callId: 'call-1',
        input: { project_id: 'project-1' },
        result: {
          id: 'call-1',
          type: 'toolResult',
          name: 'forge_list_tasks',
          output: '2 tasks',
          status: 'completed',
        },
        resultLabel: '2 tasks',
        status: 'success',
      },
    ])
  })

  it('12d. hidden session_info still renders derived MCP tool calls', () => {
    const completedItem = {
      id: 'call-1',
      type: 'mcpToolCall',
      tool: 'forge_create_task',
      server: 'mcp_router',
      arguments: { project_id: 'project-1', title: 'Create me' },
      result: { content: [{ type: 'text', text: '{"id":"task-1"}' }] },
      status: 'completed',
    }
    const entries = logsToChatEntries(
      [
        log(
          'session_info',
          {
            method: 'item/started',
            params: {
              item: {
                ...completedItem,
                result: null,
                status: 'inProgress',
              },
            },
          },
          1,
        ),
        log(
          'session_info',
          {
            method: 'item/completed',
            params: {
              item: completedItem,
            },
          },
          2,
        ),
      ],
      { hiddenKinds: ['session_info', 'shell_command', 'stdout', 'stderr'] },
    )

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'tool_call',
      toolName: 'forge_create_task',
      callId: 'call-1',
      input: { project_id: 'project-1', title: 'Create me' },
      result: completedItem,
      status: 'success',
    })
  })

  it('12e. MCP tool errors use the nested error message as the result label', () => {
    const failedItem = {
      id: 'call-1',
      type: 'mcpToolCall',
      tool: 'forge_create_task',
      arguments: { title: 'Create me' },
      error: { message: 'invalid params: unknown field `type`' },
      status: 'failed',
    }
    const entries = logsToChatEntries([
      log(
        'session_info',
        {
          method: 'item/started',
          params: {
            item: {
              ...failedItem,
              error: null,
              status: 'inProgress',
            },
          },
        },
        1,
      ),
      log(
        'session_info',
        {
          method: 'item/completed',
          params: {
            item: failedItem,
          },
        },
        2,
      ),
    ])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'tool_call',
      toolName: 'forge_create_task',
      resultLabel: 'invalid params: unknown field `type`',
      status: 'failed',
    })
  })

  it("13. Aggregation of 3 same-tool calls all success -> aggregated entry worstStatus 'success'", () => {
    const entries = logsToChatEntries([
      log('tool_call', { tool: 'file_read', call_id: '1' }, 1),
      log('tool_call', { tool: 'file_read', call_id: '2' }, 2),
      log('tool_call', { tool: 'file_read', call_id: '3' }, 3),
      log('tool_result', { call_id: '1', success: true }, 4),
      log('tool_result', { call_id: '2', success: true }, 5),
      log('tool_result', { call_id: '3', success: true }, 6),
    ])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'aggregated_tool_calls',
      toolName: 'file_read',
      status: 'success',
      worstStatus: 'success',
      calls: [
        { kind: 'tool_call', status: 'success' },
        { kind: 'tool_call', status: 'success' },
        { kind: 'tool_call', status: 'success' },
      ],
    })
  })

  it("14. Aggregation of 3 same-tool calls with one failed -> worstStatus 'failed'", () => {
    const entries = logsToChatEntries([
      log('tool_call', { tool: 'file_read', call_id: '1' }, 1),
      log('tool_call', { tool: 'file_read', call_id: '2' }, 2),
      log('tool_call', { tool: 'file_read', call_id: '3' }, 3),
      log('tool_result', { call_id: '1', success: true }, 4),
      log('tool_result', { call_id: '2', success: false }, 5),
      log('tool_result', { call_id: '3', success: true }, 6),
    ])

    expect(entries[0]).toMatchObject({
      kind: 'aggregated_tool_calls',
      status: 'failed',
      worstStatus: 'failed',
    })
  })

  it('15. Single tool call (N=1) NOT aggregated', () => {
    const entries = logsToChatEntries([log('tool_call', { tool: 'file_read', call_id: '1' }, 1)])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'tool_call',
      toolName: 'file_read',
      status: 'pending',
    })
  })

  it('16. Codex commandExecution session events render as one shell output', () => {
    const entries = logsToChatEntries([
      log(
        'session_info',
        {
          method: 'item/started',
          params: {
            item: {
              id: 'call-1',
              type: 'commandExecution',
              command: "/bin/zsh -lc 'pnpm build'",
              cwd: '/repo',
              status: 'inProgress',
            },
          },
        },
        1,
      ),
      log(
        'session_info',
        {
          method: 'item/commandExecution/outputDelta',
          params: {
            itemId: 'call-1',
            delta: 'building...\n',
          },
        },
        2,
      ),
      log(
        'session_info',
        {
          method: 'item/completed',
          params: {
            item: {
              id: 'call-1',
              type: 'commandExecution',
              command: "/bin/zsh -lc 'pnpm build'",
              cwd: '/repo',
              status: 'completed',
              exitCode: 0,
              aggregatedOutput: 'building...\n',
            },
          },
        },
        3,
      ),
    ])

    expect(entries).toEqual([
      {
        kind: 'shell_output',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        command: "/bin/zsh -lc 'pnpm build'",
        cwd: '/repo',
        status: 'success',
        lines: [{ stream: 'stdout', text: 'building...\n' }],
      },
    ])
  })

  it('17. Codex completed command uses aggregated output when no deltas arrived', () => {
    const entries = logsToChatEntries([
      log(
        'session_info',
        {
          method: 'item/completed',
          params: {
            item: {
              id: 'call-1',
              type: 'commandExecution',
              command: 'pnpm build',
              status: 'failed',
              exitCode: 1,
              aggregatedOutput: 'tsc: command not found\n',
            },
          },
        },
        1,
      ),
    ])

    expect(entries[0]).toMatchObject({
      kind: 'shell_output',
      command: 'pnpm build',
      status: 'failed',
      lines: [{ stream: 'stderr', text: 'tsc: command not found\n' }],
    })
  })

  it('18. Codex user and file change session events become readable entries', () => {
    const entries = logsToChatEntries([
      log(
        'session_info',
        {
          method: 'item/completed',
          params: {
            item: {
              type: 'userMessage',
              content: [{ type: 'text', text: 'Do the task' }],
            },
          },
        },
        1,
      ),
      log(
        'session_info',
        {
          method: 'item/completed',
          params: {
            item: {
              type: 'fileChange',
              changes: [
                {
                  path: '/tmp/worktree/src/App.tsx',
                  kind: { type: 'update' },
                  diff: '@@ -1 +1\n-old\n+new\n',
                },
              ],
            },
          },
        },
        2,
      ),
    ])

    expect(entries[0]).toMatchObject({ kind: 'user', text: 'Do the task' })
    expect(entries[1]).toMatchObject({
      kind: 'file_edit',
      action: 'edit',
      path: 'src/App.tsx',
      additions: 1,
      deletions: 1,
    })
  })

  it('19. Noisy adapter telemetry is suppressed', () => {
    const entries = logsToChatEntries([
      log('stderr', { method: 'account/rateLimits/updated', params: {} }, 1),
      log('session_info', { method: 'thread/tokenUsage/updated', params: {} }, 2),
      log('session_info', { method: 'thread/status/changed', params: {} }, 3),
      log('stderr', { id: 1, result: { turn: { status: 'inProgress' } } }, 4),
      log('stderr', { method: 'skills/changed', params: {} }, 5),
    ])

    expect(entries).toEqual([])
  })

  it('20. Codex approval requests render as approval cards', () => {
    const payload = {
      method: 'item/commandExecution/requestApproval',
      params: {
        command: "/bin/zsh -lc 'pnpm build'",
        reason: 'Allow build output?',
      },
    }
    const entries = logsToChatEntries([log('tool_call', payload, 1)])

    expect(entries).toEqual([
      {
        kind: 'approval',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        question: 'Allow build output?',
        decision: 'approved',
        rationale: "/bin/zsh -lc 'pnpm build'",
        payload,
        status: 'success',
      },
    ])
  })

  it('21. Codex file change request/output noise is suppressed', () => {
    const entries = logsToChatEntries([
      log('file_change', { method: 'item/fileChange/requestApproval', params: {} }, 1),
      log('file_change', { method: 'item/fileChange/outputDelta', params: {} }, 2),
    ])

    expect(entries).toEqual([])
  })

  it('22. Successful no-output commands are hidden', () => {
    const entries = logsToChatEntries([
      log(
        'session_info',
        {
          method: 'item/completed',
          params: {
            item: {
              id: 'call-1',
              type: 'commandExecution',
              command: 'pnpm install',
              status: 'completed',
              exitCode: 0,
              aggregatedOutput: null,
            },
          },
        },
        1,
      ),
      log(
        'session_info',
        {
          method: 'item/completed',
          params: {
            item: {
              id: 'call-2',
              type: 'commandExecution',
              command: 'false',
              status: 'failed',
              exitCode: 1,
              aggregatedOutput: null,
            },
          },
        },
        2,
      ),
    ])

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      kind: 'shell_output',
      command: 'false',
      status: 'failed',
      lines: [],
    })
  })

  it('23. Synthesizes the execution prompt before log-derived entries', () => {
    const entries = logsToChatEntries([log('assistant', { text: 'Working on it' }, 1)], {
      userPrompt: 'Do the task',
    })

    expect(entries).toEqual([
      {
        kind: 'user',
        sequence: 0,
        timestamp: '2026-04-15T00:00:01Z',
        text: 'Do the task',
      },
      {
        kind: 'assistant',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        text: 'Working on it',
      },
    ])
  })

  it('24. Skips duplicate Forge user prompt logs', () => {
    const entries = logsToChatEntries(
      [log('user', { text: 'Do the task' }, 1), log('assistant', { text: 'Done' }, 2)],
      { userPrompt: 'Do the task' },
    )

    expect(entries).toEqual([
      {
        kind: 'user',
        sequence: 0,
        timestamp: '2026-04-15T00:00:01Z',
        text: 'Do the task',
      },
      {
        kind: 'assistant',
        sequence: 2,
        timestamp: '2026-04-15T00:00:02Z',
        text: 'Done',
      },
    ])
  })

  it('25. Skips duplicate Codex userMessage events but keeps later user turns', () => {
    const entries = logsToChatEntries(
      [
        log(
          'session_info',
          {
            method: 'item/completed',
            params: {
              item: {
                type: 'userMessage',
                content: [{ type: 'text', text: 'Do the task' }],
              },
            },
          },
          1,
        ),
        log(
          'session_info',
          {
            method: 'item/completed',
            params: {
              item: {
                type: 'userMessage',
                content: [{ type: 'text', text: 'Please commit it' }],
              },
            },
          },
          2,
        ),
      ],
      { userPrompt: 'Do the task' },
    )

    expect(entries).toEqual([
      {
        kind: 'user',
        sequence: 0,
        timestamp: '2026-04-15T00:00:01Z',
        text: 'Do the task',
      },
      {
        kind: 'user',
        sequence: 2,
        timestamp: '2026-04-15T00:00:02Z',
        text: 'Please commit it',
      },
    ])
  })

  it('26. Skips consecutive duplicate user prompt echoes while preserving tool calls', () => {
    const promptEcho = {
      method: 'item/completed',
      params: {
        item: {
          type: 'userMessage',
          content: [{ type: 'text', text: 'Use a tool' }],
        },
      },
    }
    const entries = logsToChatEntries([
      log('user', { text: 'Use a tool' }, 1),
      log('session_info', promptEcho, 2),
      log('tool_call', { tool: 'forge_list_tasks', call_id: 'call-1', params: {} }, 3),
      log('tool_result', { call_id: 'call-1', success: true, content: [] }, 4),
    ])

    expect(entries).toHaveLength(2)
    expect(entries[0]).toMatchObject({
      kind: 'user',
      text: 'Use a tool',
    })
    expect(entries[1]).toMatchObject({
      kind: 'tool_call',
      toolName: 'forge_list_tasks',
      callId: 'call-1',
      status: 'success',
    })
  })

  it('27. Skips follow-up prompt echoes separated by an empty assistant placeholder', () => {
    const promptEcho = {
      method: 'item/completed',
      params: {
        item: {
          type: 'userMessage',
          content: [{ type: 'text', text: 'Continue this' }],
        },
      },
    }
    const entries = logsToChatEntries([
      log('user', { text: 'Continue this' }, 1),
      log('assistant', { text: '' }, 2),
      log('session_info', promptEcho, 3),
      log('assistant', { text: 'Actual reply' }, 4),
    ])

    expect(entries).toEqual([
      {
        kind: 'user',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        text: 'Continue this',
      },
      {
        kind: 'assistant',
        sequence: 4,
        timestamp: '2026-04-15T00:00:04Z',
        text: 'Actual reply',
      },
    ])
  })

  it('26. Renders Codex session system errors instead of hiding them as noisy metadata', () => {
    const payload = {
      method: 'thread/status/changed',
      params: {
        threadId: 'thread-1',
        status: {
          type: 'systemError',
          message: 'Model rejected the request',
        },
      },
    }
    const entries = logsToChatEntries([log('session_info', payload, 1)])

    expect(entries).toEqual([
      {
        kind: 'error',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        title: 'Session Error',
        message: 'Model rejected the request',
        payload,
      },
    ])
  })

  it('27. Skips reconstructed full prompt echoes from fallback userMessage events', () => {
    const reconstructedPrompt = [
      'You are a helpful project assistant.',
      '',
      'Available Forge MCP tools: forge_create_task, forge_list_tasks.',
      '',
      'Conversation history:',
      '',
      'user: Create it',
      '',
      'user: delete it',
    ].join('\n')
    const promptEcho = {
      method: 'item/completed',
      params: {
        item: {
          type: 'userMessage',
          content: [{ type: 'text', text: reconstructedPrompt }],
        },
      },
    }
    const entries = logsToChatEntries([
      log('user', { text: 'Create it' }, 1),
      log('assistant', { text: 'task-id' }, 2),
      log('user', { text: 'delete it' }, 3),
      log('session_info', promptEcho, 4),
      log('tool_call', { tool: 'forge_cancel_task', call_id: 'call-1', params: {} }, 5),
    ])

    expect(entries).toHaveLength(4)
    expect(entries.filter((entry) => entry.kind === 'user')).toEqual([
      {
        kind: 'user',
        sequence: 1,
        timestamp: '2026-04-15T00:00:01Z',
        text: 'Create it',
      },
      {
        kind: 'user',
        sequence: 3,
        timestamp: '2026-04-15T00:00:03Z',
        text: 'delete it',
      },
    ])
    expect(JSON.stringify(entries)).not.toContain('Available Forge MCP tools:')
  })

  it('28. Extracts nested session error notification messages', () => {
    const payload = {
      method: 'error',
      params: {
        error: {
          message:
            '{"type":"error","error":{"message":"The selected model requires a newer CLI."}}',
        },
      },
    }
    const entries = logsToChatEntries([log('session_info', payload, 1)])

    expect(entries[0]).toMatchObject({
      kind: 'error',
      title: 'Session Error',
      message: 'The selected model requires a newer CLI.',
    })
  })
})

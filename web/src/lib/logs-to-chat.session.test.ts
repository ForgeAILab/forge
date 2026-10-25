// Log session, command execution, telemetry, and error tests.

import { log } from './logs-to-chat.test-helpers'
import { describe, expect, it } from 'vitest'
import { logsToChatEntries } from './logs-to-chat'

describe('logsToChatEntries', () => {

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

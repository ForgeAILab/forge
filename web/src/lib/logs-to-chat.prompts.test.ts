// Log execution prompt synthesis and echo deduplication tests.

import { log } from './logs-to-chat.test-helpers'
import { describe, expect, it } from 'vitest'
import { logsToChatEntries } from './logs-to-chat'

describe('logsToChatEntries', () => {

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
})

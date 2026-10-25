// Log tool call pairing, result labels, and aggregation tests.

import { log } from './logs-to-chat.test-helpers'
import { describe, expect, it } from 'vitest'
import { logsToChatEntries } from './logs-to-chat'

describe('logsToChatEntries', () => {

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
})

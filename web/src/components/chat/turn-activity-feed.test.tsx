import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import {
  activitySummaryLabel,
  currentActivityLabel,
  describeToolCall,
  summarizeTurnActivity,
  toolCallArgumentKeys,
  toolCallDetail,
  toolCallOperation,
  toolCallPreview,
  TurnActivityFeed,
  turnActivityEntries,
} from './turn-activity-feed'
import type { LogEntry } from '@/types/generated'

function log(sequence: number, kind: LogEntry['kind'], payload: unknown): LogEntry {
  return {
    schema_version: 1,
    sequence,
    timestamp: `2026-09-02T10:00:${String(sequence).padStart(2, '0')}Z`,
    execution_id: 'turn-1',
    kind,
    stream: 'main',
    payload,
    truncated: false,
  }
}

const toolCall = (sequence: number, callId: string, name: string) =>
  log(sequence, 'tool_call', { call_id: callId, name, argument_keys: ['operation', 'arguments'] })

// The new native-sink wire shape: a flat `input` preview object of bounded
// strings alongside the pre-existing `argument_keys` key-name list.
const toolCallWithInput = (
  sequence: number,
  callId: string,
  name: string,
  input: Record<string, string>,
) => log(sequence, 'tool_call', { call_id: callId, name, argument_keys: Object.keys(input), input })

function firstToolCall(entries: ReturnType<typeof turnActivityEntries>) {
  const call = entries.find((entry) => entry.kind === 'tool_call')
  if (!call || call.kind !== 'tool_call') throw new Error('expected a tool call entry')
  return call
}

const toolResult = (
  sequence: number,
  callId: string,
  name: string,
  operation: string,
  status: 'succeeded' | 'failed' = 'succeeded',
) =>
  log(sequence, 'tool_result', {
    call_id: callId,
    name,
    is_error: status === 'failed',
    success: status === 'succeeded',
    summary: {
      status,
      code: status === 'succeeded' ? 'ok' : 'version_conflict',
      safe_message:
        status === 'succeeded' ? 'the tool call completed successfully' : 'refresh and retry',
      retryable: false,
      recovery_action: null,
      correlation_id: callId,
      operation,
    },
  })

describe('turnActivityEntries', () => {
  it('pairs tool calls with their bounded results and keeps reasoning', () => {
    const entries = turnActivityEntries(
      [
        log(0, 'thinking', { text: 'Need the skill first. ' }),
        log(1, 'thinking', { text: 'Then propose.' }),
        toolCall(2, 'call-1', 'forge_project_orchestration_read'),
        toolResult(3, 'call-1', 'forge_project_orchestration_read', 'skill.section'),
        log(4, 'assistant_delta', { text: 'Done' }),
      ],
      { includeReply: false },
    )

    expect(entries.map((entry) => entry.kind)).toEqual(['thinking', 'tool_call'])
    const thinking = entries[0]
    if (thinking.kind !== 'thinking') throw new Error('expected thinking')
    expect(thinking.text).toBe('Need the skill first. Then propose.')
    const call = entries[1]
    if (call.kind !== 'tool_call') throw new Error('expected tool call')
    expect(call.status).toBe('success')
    expect(call.callId).toBe('call-1')
  })

  it('keeps the streaming reply only while the turn is live', () => {
    const logs = [log(0, 'assistant_delta', { text: 'Hello' })]
    expect(turnActivityEntries(logs, { includeReply: true }).map((entry) => entry.kind)).toEqual([
      'assistant',
    ])
    expect(turnActivityEntries(logs, { includeReply: false })).toEqual([])
  })
})

describe('describeToolCall', () => {
  it('prefers the settled operation over the multiplexed tool name', () => {
    expect(describeToolCall('forge_project_orchestration_read', 'skill.section').done).toBe(
      'Read the operating skill',
    )
    expect(describeToolCall('forge_project_orchestration_propose', 'task.propose').pending).toBe(
      'Proposing a Task',
    )
  })

  it('falls back to the tool family, then to the raw name', () => {
    expect(describeToolCall('forge_main_orchestration_read').pending).toBe('Reading Forge state')
    expect(describeToolCall('forge_task_command').done).toBe('Ran a command')
    expect(describeToolCall('some_custom_tool').done).toBe('Used some_custom_tool')
  })
})

describe('toolCallPreview / toolCallDetail', () => {
  it('has no preview or detail for old logs without an input preview', () => {
    const call = firstToolCall(
      turnActivityEntries([toolCall(0, 'call-1', 'forge_task_command')], { includeReply: true }),
    )
    expect(toolCallPreview(call)).toEqual({})
    expect(toolCallDetail(call)).toBeUndefined()
    expect(toolCallArgumentKeys(call)).toEqual(['operation', 'arguments'])
  })

  it('maps the wire input object to a flat string preview', () => {
    const call = firstToolCall(
      turnActivityEntries(
        [
          toolCallWithInput(0, 'call-1', 'forge_task_command', {
            command: 'cargo test -p db',
            timeout_secs: '120',
          }),
        ],
        { includeReply: true },
      ),
    )
    expect(toolCallPreview(call)).toEqual({ command: 'cargo test -p db', timeout_secs: '120' })
  })

  it('falls back to argument_keys when nothing survived server-side filtering', () => {
    const payload = { call_id: 'call-1', name: 'forge_task_command', argument_keys: ['command'] }
    const call = firstToolCall(
      turnActivityEntries([log(0, 'tool_call', payload)], { includeReply: true }),
    )
    expect(toolCallPreview(call)).toEqual({})
    expect(call.input).toEqual(payload)
  })

  it('surfaces the command for forge_task_command', () => {
    const call = firstToolCall(
      turnActivityEntries(
        [toolCallWithInput(0, 'call-1', 'forge_task_command', { command: 'cargo test -p db' })],
        { includeReply: true },
      ),
    )
    expect(toolCallDetail(call)).toBe('cargo test -p db')
  })

  it('surfaces the path (or file_path) for forge_task_read/write/validate', () => {
    const entries = turnActivityEntries(
      [
        toolCallWithInput(0, 'call-1', 'forge_task_read', { path: 'src/App.tsx' }),
        toolCallWithInput(1, 'call-2', 'forge_task_write', { file_path: 'src/lib/x.ts' }),
      ],
      { includeReply: true },
    )
    const [readCall, writeCall] = entries.filter((entry) => entry.kind === 'tool_call')
    if (readCall.kind !== 'tool_call' || writeCall.kind !== 'tool_call') {
      throw new Error('expected tool calls')
    }
    expect(toolCallDetail(readCall)).toBe('src/App.tsx')
    expect(toolCallDetail(writeCall)).toBe('src/lib/x.ts')
  })

  it('surfaces the first params.* field for a typed Forge tool, stripping the prefix', () => {
    const call = firstToolCall(
      turnActivityEntries(
        [
          toolCallWithInput(0, 'call-1', 'forge_scope_read', {
            operation: 'task.read',
            'params.task_id': 'abc',
          }),
        ],
        { includeReply: true },
      ),
    )
    expect(toolCallDetail(call)).toBe('task_id: abc')
  })

  it('prefers a telling nested field over paging fields for a typed Forge tool', () => {
    const call = firstToolCall(
      turnActivityEntries(
        [
          toolCallWithInput(0, 'call-1', 'forge_project_orchestration_read', {
            operation: 'task.read',
            'arguments.limit': '100',
            'arguments.task_id': 'abc',
          }),
        ],
        { includeReply: true },
      ),
    )
    expect(toolCallDetail(call)).toBe('task_id: abc')
  })

  it('still names a paging field when it is all a typed call carries', () => {
    const call = firstToolCall(
      turnActivityEntries(
        [
          toolCallWithInput(0, 'call-1', 'forge_scope_read', {
            operation: 'delivery.read',
            'arguments.limit': '100',
          }),
        ],
        { includeReply: true },
      ),
    )
    expect(toolCallDetail(call)).toBe('limit: 100')
  })

  it('falls back to the first non-operation preview field, as key: value, for other tools', () => {
    const call = firstToolCall(
      turnActivityEntries(
        [
          toolCallWithInput(0, 'call-1', 'some_custom_tool', {
            operation: 'noop',
            label: 'hello world',
          }),
        ],
        { includeReply: true },
      ),
    )
    expect(toolCallDetail(call)).toBe('label: hello world')
  })
})

describe('toolCallOperation', () => {
  it('reads the input preview operation while the call is still pending', () => {
    const call = firstToolCall(
      turnActivityEntries(
        [toolCallWithInput(0, 'call-1', 'forge_scope_read', { operation: 'task.read' })],
        { includeReply: true },
      ),
    )
    expect(call.status).toBe('pending')
    expect(toolCallOperation(call)).toBe('task.read')
  })

  it('prefers the settled result operation once the call has a result', () => {
    const call = firstToolCall(
      turnActivityEntries(
        [
          toolCallWithInput(0, 'call-1', 'forge_scope_read', { operation: 'task.read' }),
          toolResult(1, 'call-1', 'forge_scope_read', 'skill.section'),
        ],
        { includeReply: true },
      ),
    )
    expect(toolCallOperation(call)).toBe('skill.section')
  })
})

describe('currentActivityLabel', () => {
  it('names the in-flight tool call, then thinking, then the streaming reply', () => {
    const pendingCall = turnActivityEntries(
      [log(0, 'thinking', { text: 'hmm' }), toolCall(1, 'call-1', 'forge_task_command')],
      { includeReply: true },
    )
    expect(currentActivityLabel(pendingCall)).toBe('Running a command…')

    const thinking = turnActivityEntries([log(0, 'thinking', { text: 'hmm' })], {
      includeReply: true,
    })
    expect(currentActivityLabel(thinking)).toBe('Thinking…')

    const writing = turnActivityEntries(
      [
        toolCall(0, 'call-1', 'forge_scope_read'),
        toolResult(1, 'call-1', 'forge_scope_read', 'project.summary'),
        log(2, 'assistant_delta', { text: 'The project' }),
      ],
      { includeReply: true },
    )
    expect(currentActivityLabel(writing)).toBe('Writing a reply…')
  })

  it('is silent once everything has settled', () => {
    const settled = turnActivityEntries(
      [
        toolCall(0, 'call-1', 'forge_scope_read'),
        toolResult(1, 'call-1', 'forge_scope_read', 'project.summary'),
      ],
      { includeReply: true },
    )
    expect(currentActivityLabel(settled)).toBeNull()
  })

  it('appends a short detail to the pending label when the input preview has one', () => {
    const entries = turnActivityEntries(
      [toolCallWithInput(0, 'call-1', 'forge_task_command', { command: 'cargo test -p db' })],
      { includeReply: true },
    )
    expect(currentActivityLabel(entries)).toBe('Running a command · cargo test -p db')
  })

  it('truncates a long detail to 48 characters, with an ellipsis, in the live label', () => {
    const longCommand = 'cargo test -p db --all-features --verbose --no-capture --release'
    expect(longCommand.length).toBeGreaterThan(48)
    const entries = turnActivityEntries(
      [toolCallWithInput(0, 'call-1', 'forge_task_command', { command: longCommand })],
      { includeReply: true },
    )
    expect(currentActivityLabel(entries)).toBe(`Running a command · ${longCommand.slice(0, 48)}…`)
  })

  it('has no detail suffix for old logs without an input preview', () => {
    const entries = turnActivityEntries([toolCall(0, 'call-1', 'forge_task_command')], {
      includeReply: true,
    })
    expect(currentActivityLabel(entries)).toBe('Running a command…')
  })
})

describe('summarizeTurnActivity', () => {
  it('counts tool calls, including aggregated runs, and failed ones', () => {
    const entries = turnActivityEntries(
      [
        log(0, 'thinking', { text: 'plan' }),
        toolCall(1, 'call-1', 'forge_scope_read'),
        toolResult(2, 'call-1', 'forge_scope_read', 'project.summary'),
        toolCall(3, 'call-2', 'forge_scope_read'),
        toolResult(4, 'call-2', 'forge_scope_read', 'skill.section'),
        toolCall(5, 'call-3', 'forge_scope_propose'),
        toolResult(6, 'call-3', 'forge_scope_propose', 'task.propose', 'failed'),
      ],
      { includeReply: false },
    )
    const summary = summarizeTurnActivity(entries)
    expect(summary).toEqual({ toolCalls: 3, failedToolCalls: 1, thought: true })
    expect(activitySummaryLabel(summary)).toBe('3 tool calls (1 failed) · thought it through')
    expect(activitySummaryLabel({ toolCalls: 1, failedToolCalls: 0, thought: false })).toBe(
      '1 tool call',
    )
  })
})

describe('TurnActivityFeed', () => {
  it('renders each tool call with its operation and expands to the bounded result', () => {
    const entries = turnActivityEntries(
      [
        toolCall(0, 'call-1', 'forge_project_orchestration_read'),
        toolResult(1, 'call-1', 'forge_project_orchestration_read', 'skill.section'),
        toolCall(2, 'call-2', 'forge_task_command'),
      ],
      { includeReply: true },
    )
    render(<TurnActivityFeed entries={entries} live />)

    expect(screen.getByText('Read the operating skill')).toBeTruthy()
    expect(screen.getByText('skill.section')).toBeTruthy()
    expect(screen.getByText('Running a command')).toBeTruthy()
    expect(screen.getByLabelText('In progress')).toBeTruthy()

    // A boilerplate success message says nothing the status icon does not,
    // so the row omits it; the expanded details still show it next to the
    // outcome code, operation, and argument key names.
    expect(screen.queryAllByText('the tool call completed successfully')).toHaveLength(0)
    fireEvent.click(screen.getByRole('button', { name: /Read the operating skill/ }))
    expect(screen.getAllByText('the tool call completed successfully')).toHaveLength(1)
    expect(screen.getByText('ok')).toBeTruthy()
    expect(screen.getByText('operation, arguments')).toBeTruthy()
  })

  it('renders nothing for an empty log', () => {
    const view = render(<TurnActivityFeed entries={[]} />)
    expect(view.container.firstChild).toBeNull()
  })

  it('shows the input preview detail while pending, and lists preview fields instead of the keys row once expanded', () => {
    const entries = turnActivityEntries(
      [toolCallWithInput(0, 'call-1', 'forge_task_command', { command: 'cargo test -p db' })],
      { includeReply: true },
    )
    render(<TurnActivityFeed entries={entries} live />)

    expect(screen.getByText('cargo test -p db')).toBeTruthy()
    expect(screen.queryByText('command')).toBeNull() // the field key isn't shown until expanded

    fireEvent.click(screen.getByRole('button', { name: /Running a command/ }))
    expect(screen.getByText('command')).toBeTruthy()
    expect(screen.getAllByText('cargo test -p db')).toHaveLength(2) // row detail + dl value
    expect(screen.queryByText('Arguments')).toBeNull()
  })

  it('shows both the detail and the result label once settled, and keeps the old Arguments keys row for old logs', () => {
    const entries = turnActivityEntries(
      [
        toolCallWithInput(0, 'call-1', 'forge_task_command', { command: 'cargo test -p db' }),
        toolResult(1, 'call-1', 'forge_task_command', 'command.run'),
        toolCall(2, 'call-2', 'forge_scope_read'),
        toolResult(3, 'call-2', 'forge_scope_read', 'project.summary'),
      ],
      { includeReply: true },
    )
    render(<TurnActivityFeed entries={entries} live />)

    // The detail sits in its own monospace <span> nested inside the label
    // span, so assert on the row's full text rather than an exact node match.
    const commandRow = screen.getByRole('button', { name: /Ran a command/ })
    expect(commandRow.textContent).toContain('cargo test -p db')
    expect(commandRow.textContent).not.toContain('completed successfully')

    fireEvent.click(screen.getByRole('button', { name: /Read Forge state/ }))
    expect(screen.getByText('operation, arguments')).toBeTruthy()
  })

  it('shows a specific result message as-is and a boilerplate failure through its outcome code', () => {
    const entries = turnActivityEntries(
      [
        toolCallWithInput(0, 'call-1', 'forge_scope_read', { operation: 'delivery.read' }),
        log(1, 'tool_result', {
          call_id: 'call-1',
          name: 'forge_scope_read',
          is_error: false,
          success: true,
          summary: {
            status: 'succeeded',
            code: 'ok',
            safe_message: 'read 3 deliveries',
            retryable: false,
            recovery_action: null,
            correlation_id: 'call-1',
            operation: 'delivery.read',
          },
        }),
        toolCallWithInput(2, 'call-2', 'forge_project_orchestration_read', {
          operation: 'project.current_state',
        }),
        log(3, 'tool_result', {
          call_id: 'call-2',
          name: 'forge_project_orchestration_read',
          is_error: true,
          success: false,
          summary: {
            status: 'failed',
            code: 'internal_failure',
            safe_message: 'the tool call did not complete successfully',
            retryable: true,
            recovery_action: null,
            correlation_id: 'call-2',
            operation: null,
          },
        }),
      ],
      { includeReply: true },
    )
    render(<TurnActivityFeed entries={entries} />)

    expect(screen.getByRole('button', { name: /delivery\.read/ }).textContent).toContain(
      'read 3 deliveries',
    )
    const failedRow = screen.getByRole('button', { name: /project\.current_state/ })
    expect(failedRow.textContent).toContain('internal failure')
    expect(failedRow.textContent).not.toContain('did not complete successfully')
  })
})

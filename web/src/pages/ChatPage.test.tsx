import { describe, expect, it } from 'vitest'
import { handoffProjectIdsForScope } from './ChatPage'
import type { AgentChatEntry } from '@/features/agent-chat/types'

const entries: AgentChatEntry[] = [
  {
    chat_id: 'main-chat',
    kind: 'main',
    project_id: null,
    project_name: null,
    identity_id: 'main-agent',
    identity_name: 'Main Agent',
    binding_state: 'active',
    chat_status: 'ready',
    unread_count: 0n,
    pending_turn_count: 0n,
    last_message_at: null,
  },
  {
    chat_id: 'project-a-chat',
    kind: 'project',
    project_id: 'project-a',
    project_name: 'Project A',
    identity_id: 'project-agent-a',
    identity_name: 'Project Agent A',
    binding_state: 'active',
    chat_status: 'ready',
    unread_count: 0n,
    pending_turn_count: 0n,
    last_message_at: null,
  },
  {
    chat_id: 'project-b-chat',
    kind: 'project',
    project_id: 'project-b',
    project_name: 'Project B',
    identity_id: 'project-agent-b',
    identity_name: 'Project Agent B',
    binding_state: 'active',
    chat_status: 'ready',
    unread_count: 0n,
    pending_turn_count: 0n,
    last_message_at: null,
  },
]

describe('ChatPage scope isolation', () => {
  it('limits Project chat handoff reads to the current Project', () => {
    expect(handoffProjectIdsForScope('project-a', entries)).toEqual(['project-a'])
  })

  it('allows Main chat to read only explicit handoff metadata for authorized Projects', () => {
    expect(handoffProjectIdsForScope(undefined, entries)).toEqual(['project-a', 'project-b'])
  })
})

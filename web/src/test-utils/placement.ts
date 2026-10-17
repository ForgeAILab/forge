import type { WorkspacePlacementResponse } from '@/types/generated'

export function serverPlacement(
  overrides: Partial<WorkspacePlacementResponse> = {},
): WorkspacePlacementResponse {
  return {
    id: 'placement-1',
    workspace_id: 'workspace-1',
    task_id: 'task-1',
    agent_id: null,
    owner_kind: 'server',
    daemon_id: null,
    runtime_id: null,
    repo_location_id: 'location-1',
    execution_daemon_id: null,
    workspace_handle: '/tmp/forge/workspace-1',
    generation: 1,
    state: 'ready',
    selected_by: 'backfill',
    selection_reason: {},
    reserved_until: null,
    disconnected_at: null,
    failure_cause: null,
    version: 1,
    created_at: '2026-09-30T12:00:00Z',
    updated_at: '2026-09-30T12:00:00Z',
    ...overrides,
  }
}

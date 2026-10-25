// Daemon, workspace, terminal, and MCP configuration types.

import type { MachineDisk } from '../bindings/MachineDisk'
import type { DiskFloor } from '../bindings/DiskFloor'

export type DaemonStatus = 'online' | 'offline'

export type TerminalSessionStatus =
  | 'starting'
  | 'running'
  | 'exited'
  | 'terminated'
  | 'timed_out'
  | 'orphaned'
  | 'cleanup_terminated'

export interface TerminalSessionResponse {
  id: string
  task_id: string
  workspace_id: string
  daemon_id?: string | null
  status: TerminalSessionStatus
  rows: number
  cols: number
  exit_code?: number | null
  exit_signal?: string | null
  exit_reason?: string | null
  created_at: string
  started_at?: string | null
  last_activity_at?: string | null
  ended_at?: string | null
  created_by_user_id: string
}

export interface CreateTerminalSessionRequest {
  rows?: number | null
  cols?: number | null
}

export interface ResizeTerminalSessionRequest {
  rows: number
  cols: number
}

export interface TerminalAttachTokenResponse {
  attach_token: string
  expires_at: string
  ws_url: string
  session_id: string
}

export interface TerminalAvailability {
  enabled: boolean
  workspace_ready: boolean
  daemon_reachable: boolean
  active_execution: boolean
  session_count_for_task: number
  session_count_for_user: number
  max_sessions_per_task: number
  max_sessions_per_user: number
  can_create: boolean
  reason?: string | null
}

export type TerminalClientFrame =
  | { type: 'input'; data: string }
  | { type: 'resize'; rows: number; cols: number }
  | { type: 'ping' }

export type TerminalServerFrame =
  | { type: 'output'; data: string }
  | {
      type: 'exit'
      exit_code?: number | null
      signal?: string | null
      reason?: string | null
    }
  | { type: 'error'; code: string; message: string }
  | { type: 'pong' }

export interface WorkspacePlacementResponse {
  id: string
  workspace_id: string
  task_id: string
  agent_id: string | null
  owner_kind: string
  daemon_id: string | null
  runtime_id: string | null
  repo_location_id: string
  execution_daemon_id: string | null
  workspace_handle: string | null
  generation: number
  state: string
  selected_by: string
  selection_reason: Record<string, unknown>
  reserved_until: string | null
  disconnected_at: string | null
  failure_cause: string | null
  version: number
  created_at: string
  updated_at: string
}

export interface Workspace {
  pending_remote_cancel: import('../bindings/PendingRemoteCancel').PendingRemoteCancel[]
  id: string
  task_id: string
  repo_id: string
  worktree_path: string
  placement: WorkspacePlacementResponse
  branch: string
  status: string
  before_sha?: string | null
  error?: string | null
  created_at: string
  updated_at: string
}

export interface Daemon {
  owner_id: string | null
  max_concurrent_runs: number | null
  run_limit: number | null
  effective_max_concurrent_runs: number | null
  /** The machine's last disk reading with the free-space floor applied. */
  disk?: MachineDisk | null
  /** The free-space floor the server applies to this machine. */
  workspace_floor?: DiskFloor | null
  /** The server's check-run wall limit; a daemon's collector derives its check-checkout age from it. */
  check_run_timeout_seconds?: number | null
  id: string
  machine_id: string
  hostname: string
  os: string
  arch: string
  agent_version?: string | null
  status: DaemonStatus
  last_report_at?: string | null
  detected_clis: Array<{
    kind: string
    availability: string
    path?: string | null
    version?: string | null
    config_path?: string | null
  }>
  labels: Record<string, string>
  version: number
  created_at: string
  updated_at: string
}

export interface Runtime {
  id: string
  daemon_id: string
  kind: string
  workspace_root: string
  status: string
  created_at: string
  updated_at: string
}

export interface ExecutorType {
  type: string
  display_name: string
  config_schema: Record<string, unknown>
  default_config: Record<string, unknown>
}

export interface McpConfigResponse {
  installed: boolean
  url: string | null
  expected_url: string
  config_path: string
  agents: string[]
}

export interface McpConfigActionRequest {
  agent: string
  scope?: string
  project_id?: string
  public_base_url?: string | null
  action: 'install' | 'uninstall'
}

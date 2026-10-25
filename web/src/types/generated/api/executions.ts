// Execution status, usage, and plan types.

import type { UsageBreakdown } from '../bindings/UsageBreakdown'
import type { PaginatedResponse } from './common'

export type ExecutionRole = string

export type ExecutionStatus = 'running' | 'completed' | 'failed' | 'cancelled'

export type ExecutionOwnerHealth = 'unowned' | 'healthy' | 'expired' | 'unknown'

export interface ExecutionInterruptionResponse {
  reason: string
  kind: string | null
  created_at: string
}

export type StopReason =
  | 'user_cancelled'
  | 'task_cancelled'
  | 'role_reassigned'
  | 'graceful_shutdown'
  | 'crash_recovery'
  | 'agent_timeout'
  | 'executor_cancelled'
  | 'executor_failed'
  | 'execution_stalled'
  | 'daemon_disconnected'
  | 'legacy_unknown'

export type ResumePolicy = 'auto' | 'manual' | 'none'

export type ExecutionBehaviorKind =
  | 'manual_launch'
  | 'session_follow_up'
  | 'workflow_resume'
  | 're_execute'

export type ExecutionBehavior = {
  kind: ExecutionBehaviorKind
  propagates: boolean
  cascade_role: string | null
  cascade_state: string | null
  description: string
}

// --- Execution (matches api_types::ExecutionResponse) ---

export interface Execution {
  id: string
  task_id: string
  agent_id?: string | null
  role: ExecutionRole
  status: ExecutionStatus
  parent_execution_id?: string | null
  agent_session_id?: string | null
  prompt?: string | null
  summary?: string | null
  before_sha?: string | null
  after_sha?: string | null
  error?: string | null
  stop_reason?: StopReason | null
  stopped_by?: string | null
  resume_policy?: ResumePolicy | null
  stopped_at?: string | null
  executor_config_snapshot?: Record<string, unknown> | null
  /** Present on collection projections when this run resumes a provider session. */
  is_resume?: boolean
  workspace_id?: string | null
  plan_progress?: PlanProgressSummary | null
  plan_artifact?: PlanArtifactDetail | null
  usage?: UsageBreakdown[] | null
  execution_version?: number
  /** Stable server-owned owner reference; never a credential or bearer token. */
  lease_owner?: string | null
  owner_health?: ExecutionOwnerHealth
  lease_expires_at?: string | null
  hard_deadline_at?: string | null
  last_heartbeat_at?: string | null
  /** Semantic progress is separate from the owner heartbeat. */
  last_progress_at?: string | null
  liveness_warning?: string | null
  interruption?: ExecutionInterruptionResponse | null
  created_at: string
  updated_at: string
}

/** Compact execution projection returned by list and task-detail endpoints. */
export interface ExecutionSummary {
  id: string
  task_id: string
  agent_id: string | null
  role: ExecutionRole
  status: ExecutionStatus
  parent_execution_id: string | null
  agent_session_id: string | null
  /** At most 500 Unicode characters. */
  summary: string | null
  is_resume: boolean
  workspace_id: string | null
  created_at: string
  updated_at: string
}

export type ExecutionResponse = Execution

export type ExecutionsResponse = PaginatedResponse<ExecutionSummary>

export interface PlanProgressSummary {
  total: number
  completed: number
  remaining: number
  available: boolean
  warnings: string[]
}

export interface PlanArtifactDetail {
  items: PlanChecklistItem[]
  warnings: string[]
  source_path: string | null
  last_modified: string | null
}

export interface PlanChecklistItem {
  checked: boolean
  label: string
  nesting_level: number
  line_number: number
}

// Events and operations status types.

import type { MachineDisk } from '../bindings/MachineDisk'
import type { ActivityCounts } from '../bindings/ActivityCounts'
import type { CostSummary } from '../bindings/CostSummary'
import type { TokenCounters } from '../bindings/TokenCounters'
import type { TaskMovedEventPayload } from './tasks'
import type { PlanProgressSummary } from './executions'

// --- Events (matches events::ForgeEvent/EventContext) ---

export interface ForgeEventBase {
  event_type: string
  entity_id: string
  timestamp: string
}

export type EventContext =
  | {
      project_id: string
      title: string
    }
  | {
      old_status: string
      new_status: string
    }
  | TaskMovedEventPayload
  | {
      task_id: string
      from: string
      to: string
      reason: string
    }
  | {
      agent_id: string
      execution_id: string
    }
  | {
      reason: string
    }
  | {}
  | {
      reason: string
    }
  | {}
  | {}
  | {
      task_id: string
      depends_on_id: string
      timestamp: string
    }
  | {
      task_id: string
      assignee_type: string | null
      assignee_id: string | null
    }
  | {
      task_id: string
      lines: string[]
    }
  | {
      task_id: string
    }
  | {
      task_id: string
      error: string
    }
  | {
      old_status: string
      new_status: string
    }
  | {
      name: string
    }
  | {}
  | {
      name: string
    }
  | {}
  | {}
  | {
      last_heartbeat: string
    }
  | {
      task_id: string
      path: string
    }
  | {
      workspace_id: string
    }
  | {
      task_id: string
      attempt_number: number
    }
  | {
      task_id: string
      status: string
    }
  | {
      task_id: string
      review_id: string
      attempt_number: number
    }
  | {
      task_id: string
      review_id: string
      attempt_number: number
      failed_step_index: number
    }
  | {
      task_id: string
    }
  | {
      task_id: string
    }
  | {
      task_id: string
      reason: string
    }
  | {
      task_id: string
      parent_execution_id: string
      execution_id: string
      trigger: string
    }
  | {
      name: string
    }
  | {}
  | {}
  | {}
  | {
      detected_clis_count: number
    }
  | {}
  | {}

export type ForgeEvent = ForgeEventBase & EventContext

// --- Log types (execution logs are JSONL, parsed client-side) ---

export interface LogEntry {
  schema_version: number
  sequence: number
  timestamp: string
  execution_id: string
  kind:
    | 'stdout'
    | 'stderr'
    | 'tool_call'
    | 'tool_result'
    | 'assistant'
    | 'assistant_delta'
    | 'thinking'
    | 'user'
    | 'system'
    | 'file_change'
    | 'shell_command'
    | 'approval_question'
    | 'session_info'
    | 'unknown'
  stream: 'main' | 'heartbeat'
  payload: unknown
  truncated: boolean
}

export type OperatorSeverity = 'healthy' | 'attention' | 'blocked' | 'error'

export interface EventRelayStatus {
  running: boolean
  position: number | null
  head: number | null
  last_error: string | null
  last_error_at: string | null
}

export interface WorkerDeadLetterSummary {
  replayable: boolean
  event_created_at: string | null
  events_since: number
  consumer_name: string
  event_type: string
  attempts: number
  id: string
  item_key: string
  event_sequence: number | null
  reason: string
  occurred_at: string
}

export interface EventConsumerStatus {
  consumer_name: string
  last_sequence: number
  lag: number
  oldest_unprocessed_at: string | null
  oldest_unprocessed_age_seconds: number | null
  last_advanced_at: string | null
  stalled: boolean
  dead_letter_count: number
  recent_dead_letters: WorkerDeadLetterSummary[]
}

export interface DatabaseStorageStatus {
  incremental_vacuum: boolean
  free_pages: number
}

export interface UsageIndexStatus {
  current_size_bytes: number
  budget_bytes: number
  fallback: boolean
}

export interface PeriodicWorkerStatus {
  worker_name: string
  running: boolean
  last_tick_at: string | null
  last_error: string | null
  last_error_at: string | null
  restart_count: number
}

export interface OperatorStatusResponse {
  integration_queues: import('../bindings/IntegrationQueueStatus').IntegrationQueueStatus
  checks: import('../bindings/ChecksStatus').ChecksStatus
  pending_remote_cancels: number
  usage_index: UsageIndexStatus
  event_relay: EventRelayStatus
  event_consumers: EventConsumerStatus[]
  periodic_workers: PeriodicWorkerStatus[]
  task_steps: import('../bindings/TaskStepQueueStatus').TaskStepQueueStatus
  database: DatabaseStorageStatus
  overall_severity: OperatorSeverity
  active_executions: ActiveExecutionSummary[]
  blocked_tasks: BlockedTaskSummary[]
  daemon_issues: DaemonIssueSummary[]
  daemon_pressure: DaemonPressureSummary[]
  agent_pressure: AgentPressureSummary[]
  workspace_cleanup: WorkspaceCleanupSummary[]
  retry_pressure: RetryPressureSummary[]
  usage_summary: UsageSummary | null
  recent_errors: RecentErrorSummary[]
  computed_at: string
}

export interface ActiveExecutionSummary {
  execution_id: string
  task_id: string
  task_title: string | null
  role: string
  agent_id: string | null
  agent_name: string | null
  daemon_id: string | null
  workspace_id: string | null
  workspace_path: string | null
  session_id: string | null
  started_at: string
  runtime_seconds: number
  elapsed_seconds: number
  latest_event: string | null
  last_event: string | null
  last_event_time: string | null
  turn_count: number
  token_totals: TokenTotalsSummary | null
  rate_limit_snapshot: Record<string, unknown> | null
  effective_policy: EffectiveExecutionPolicy | null
  plan_progress: PlanProgressSummary | null
}

export interface DaemonPressureSummary {
  check_runs: number
  borrowed_check_runs: number
  logical_cores: number | null
  build_jobs_per_run: number | null
  run_nice: number | null
  daemon_id: string
  hostname: string | null
  active_runs: number
  max_concurrent_runs: number | null
  at_capacity: boolean
  /** While `disk.pressure` is set the machine starts no new worktree or check. */
  disk?: MachineDisk | null
}

export interface AgentPressureSummary {
  agent_id: string
  agent_name: string
  daemon_id: string | null
  active_tasks: number
  max_concurrent_tasks: number
  at_capacity: boolean
}

export interface TokenTotalsSummary {
  tokens: TokenCounters
  cost: CostSummary
}

export interface BlockedTaskSummary {
  task_id: string
  title: string
  blocked_reason: string | null
  blocked_since: string | null
}

export interface DaemonIssueSummary {
  daemon_id: string
  hostname: string | null
  issue: string
  severity: OperatorSeverity
  detected_at: string | null
}

export interface WorkspaceCleanupSummary {
  workspace_id: string
  task_id: string
  worktree_path: string | null
  cleanup_after: string | null
}

export interface RetryPressureSummary {
  task_id: string
  title: string
  attempt_count: number
  max_attempts: number | null
  current_state: string
  retry_reason: string | null
  due_time: string | null
  last_error: string | null
}

export interface UsageSummary {
  counts: ActivityCounts
  tokens: TokenCounters
  cost: CostSummary
  active_execution_count: number
}

export interface OperationsRefreshResponse {
  dispatched_tasks: number
  refreshed_at: string
}

export interface RecentErrorSummary {
  entity_type: string
  entity_id: string
  error: string
  occurred_at: string
  severity: OperatorSeverity
}

export interface EffectiveExecutionPolicy {
  executor_kind: string
  permission_policy: string
  isolation_posture: string
  is_high_risk: boolean
  effective_cwd: string | null
  workspace_root: string | null
  environment_posture: string
  scoped_tools: string[]
  mcp_servers: string[]
}

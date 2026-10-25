// Agent configuration and availability types.

import type { UsageAggregate } from '../bindings/UsageAggregate'
import type { AgentRunnableOn } from '../bindings/AgentRunnableOn'
import type { Daemon } from './runtime'
import type { PaginatedResponse } from './common'

export type AgentStatus = 'idle' | 'busy' | 'error' | 'offline'

// --- Agent (matches api_types::AgentResponse) ---

export interface Agent {
  runnable_on?: AgentRunnableOn
  id: string
  name: string
  description: string | null
  profile_id: string
  backend_kind: string
  executor_type: string
  provider: string | null
  model: string | null
  reasoning_effort: string | null
  permission_policy: string | null
  prompt_template: string | null
  capabilities: string[]
  config_json: Record<string, unknown>
  credential_handle_id?: string | null
  daemon_id: string | null
  daemon?: Daemon
  max_concurrent_tasks: number
  status: AgentStatus
  /** Tasks assigned to this identity in an active or gate state. Assigned
   * workload, not live concurrency: not bounded by max_concurrent_tasks. */
  active_assigned_task_count?: number | null
  /** Executions currently running. This is what max_concurrent_tasks gates. */
  running_execution_count?: number | null
  effective_status?: string | null
  avg_duration_ms: number | null
  success_rate: number | null
  usage: UsageAggregate
  is_default: boolean
  paused: boolean
  owner_id: string | null
  visibility: string
  version: number
  created_at: string
  updated_at: string
}

export interface CreateAgentRequest {
  name: string
  description?: string | null
  executor_type: string
  model?: string | null
  reasoning_effort?: string | null
  permission_policy?: string | null
  prompt_template?: string | null
  capabilities?: string[]
  config_json?: Record<string, unknown>
  daemon_id?: string | null
  max_concurrent_tasks?: number
  heartbeat_interval_seconds?: number
  max_missed_heartbeats?: number
  is_default?: boolean
  /** Provider entry powering this harness agent (dispatch-time key injection). */
  credential_id?: string | null
}

export interface UpdateAgentRequest {
  name?: string
  description?: string | null
  model?: string | null
  reasoning_effort?: string | null
  permission_policy?: string | null
  prompt_template?: string | null
  capabilities?: string[]
  config_json?: Record<string, unknown>
  daemon_id?: string | null
  max_concurrent_tasks?: number
  is_default?: boolean
  paused?: boolean
  version: number
}

export interface AgentDiscoveredOptions {
  models: string[]
  permission_policies: string[]
  cli_specific: Record<string, unknown>
  available_daemons: Array<{ id: string; name: string; status: string }>
  warning: string | null
}

export interface AgentAvailability {
  available: boolean
  effective_status: string
  resolved_daemon_id: string | null
  active_assigned_task_count: number
  running_execution_count: number
  max_concurrent_tasks: number
  reason?: string | null
}

export type AgentsResponse = PaginatedResponse<Agent>

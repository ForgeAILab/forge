import type {
  AgentStatus,
  ContextManifestListQuery,
  ContextManifestListResponse,
  ContextManifestQuery,
  ContextManifestResponse,
  MainAgentBindingResponse,
  ProjectAgentBindingResponse,
  SetMainAgentBindingRequest,
  SetProjectAgentBindingRequest,
} from '@/types/generated'

export type JsonObject = Record<string, unknown>

export interface Page<T> {
  items: T[]
  next_cursor?: string | null
  has_more: boolean
  total_count?: number | null
}

export type ProjectAgentBinding = ProjectAgentBindingResponse
export type MainAgentBinding = MainAgentBindingResponse
export type MainAgentBindingInput = Omit<SetMainAgentBindingRequest, 'expected_version'> & {
  expected_version: number
}
export type ProjectAgentBindingInput = Omit<
  SetProjectAgentBindingRequest,
  'expected_version' | 'wake_budget'
> & {
  expected_version: number
  wake_budget: number
}

export interface FederatedAgent {
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
  config_json: JsonObject
  credential_handle_id: string | null
  daemon_id: string | null
  max_concurrent_tasks: number
  status: AgentStatus
  active_task_count: number | null
  effective_status: string | null
  total_runs: number
  avg_duration_ms: number | null
  success_rate: number | null
  total_input_tokens: number
  total_output_tokens: number
  total_cache_read_tokens: number
  total_cache_write_tokens: number
  total_tokens: number
  total_cost_usd: number | null
  is_default: boolean
  paused: boolean
  owner_id: string | null
  visibility: string
  version: number
  created_at: string
  updated_at: string
}

export interface AgentProfile {
  id: string
  identity_id: string
  backend_kind: string
  executor_type: string
  provider: string | null
  model: string | null
  reasoning_effort: string | null
  permission_policy: string | null
  system_prompt: string | null
  capabilities: JsonObject
  tool_policy: JsonObject
  config: JsonObject
  credential_handle_id: string | null
  version: number
  created_at: string
}

export interface AgentConnectionHealth {
  profile_id: string
  status: string
  capabilities: JsonObject
  checked_at: string | null
  error_code: string | null
  updated_at: string
}

export interface AgentSession {
  id: string
  identity_id: string
  profile_id: string
  context_scope_id: string
  backend_kind: string
  status: string
  capabilities: JsonObject
  connection_status: string
  predecessor_session_id: string | null
  replaced_by_session_id: string | null
  last_activity_at: string | null
  version: number
  created_at: string
  updated_at: string
}

export interface CredentialHandle {
  id: string
  provider: string
  label: string
  credential_method: string
  status: string
  version: number
  created_at: string
  updated_at: string
}

export interface ConnectedEmbeddedAgent {
  agent: FederatedAgent
  credential_handle: CredentialHandle
  profile: AgentProfile
  health: AgentConnectionHealth
  session: AgentSession
}

export interface ConnectedEmbeddedProfile {
  agent: FederatedAgent
  profile: AgentProfile
  credential_handle: CredentialHandle
  health: AgentConnectionHealth
}

export interface CreateEmbeddedAgentInput {
  name: string
  description?: string | null
  credential_id: string
  model: string
  system_prompt?: string | null
  account_permission_ceiling?: JsonObject | null
  tool_policy?: JsonObject | null
  context_tokens?: number | null
  max_input_tokens?: number | null
  max_output_tokens?: number | null
}

export interface ConnectEmbeddedProfileInput {
  version: number
  credential_id: string
  model: string
  system_prompt?: string | null
  permission_policy?: string | null
  tool_policy?: JsonObject | null
  context_tokens?: number | null
  max_input_tokens?: number | null
  max_output_tokens?: number | null
}

export interface CreateAgentSessionInput {
  profile_id?: string | null
  scope:
    | { type: 'account' }
    | { type: 'project'; project_id: string }
    | { type: 'agent_chat'; chat_id: string }
    | { type: 'task'; task_id: string; role: string }
}

export interface EffectivePermissions {
  allowed: string[]
  denied: string[]
  requires_approval: string[]
}

export interface AttentionItem {
  id: string
  category: string
  scope_type: string
  scope_id: string
  identity_id: string | null
  source_event_id: string
  priority: number
  lifecycle: string
  summary: string
  details: JsonObject
  dedupe_key: string
  occurred_at: string
  updated_at: string
  version: number
  acknowledged_at: string | null
  snoozed_until: string | null
  resolved_at: string | null
  recommended_action: string | null
}

export interface MissionControlWorkItem {
  task_id: string
  project_id: string
  title: string
  status: string
  priority: number
  updated_at: string
  primary_action: string
}

export type ReviewReadyItem = MissionControlWorkItem
export type ActiveWorkItem = MissionControlWorkItem

export interface AgentHealthItem {
  identity_id: string
  name: string
  backend_kind: string | null
  provider: string | null
  model: string | null
  identity_status: string
  paused: boolean
  connection_status: string | null
  last_activity_at: string | null
  active_session_count: number
  project_count: number
}

export interface OutcomeItem {
  task_id: string
  project_id: string
  title: string
  outcome: string
  occurred_at: string
}

export type CoordinationActivityKind = 'direct_command' | 'approval_action'

export interface CoordinationActivityItem {
  id: string
  activity_kind: CoordinationActivityKind
  actor_type: string
  actor_id: string
  scope_type: string
  scope_id: string
  operation: string
  input_digest: string
  policy_result: string
  status: string
  correlation_id: string
  outcome?: JsonObject | null
  occurred_at: string
}

export interface RuntimeCapacity {
  active_executions: number
  queued_tasks: number
  active_sessions: number
  healthy: boolean
}

export interface AttentionConsumerHealth {
  consumer_name: string
  last_sequence: number
  last_success_at: string | null
  last_error_code: string | null
  stale: boolean
  processed_events: number
  updated_at: string
}

export interface MissionControlResponse {
  needs_attention: AttentionItem[]
  review_ready: ReviewReadyItem[]
  active_work: ActiveWorkItem[]
  agent_health: AgentHealthItem[]
  recent_outcomes: OutcomeItem[]
  coordination_activity: CoordinationActivityItem[]
  capacity: RuntimeCapacity
  consumer_health: AttentionConsumerHealth | null
  computed_at: string
}

export type ContextManifest = ContextManifestResponse
export type ContextManifestSource = ContextManifestResponse['sources'][number]
export type ContextManifestLookup = ContextManifestQuery
export type ContextManifestDiscoveryQuery = ContextManifestListQuery
export type ContextManifestDiscoveryResponse = ContextManifestListResponse

/**
 * Provider usage projection (`GET /providers/{id}/usage`). Not yet part of
 * the generated API types, so it is modeled locally against the contract
 * the endpoint is being built against.
 */
export interface ProviderUsageWindow {
  id: 'primary' | 'secondary' | string
  used_percent: number
  window_minutes: number | null
  resets_at: string | null
}

export interface ProviderUsage {
  id: string
  provider: string
  source: 'probe' | 'unknown'
  plan_type?: string | null
  windows: ProviderUsageWindow[]
  fetched_at: string
  detail?: string | null
}

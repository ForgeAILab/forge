// Workflow definitions and Project execution settings.

// The generated binding is the one definition (it carries `replan`).
import type { TransitionBridgeKind } from '../bindings/TransitionBridgeKind'

export interface ReviewConfig {
  verify?: import('../bindings/ReviewVerifyConfig').ReviewVerifyConfig | null
  conformance_checks?: import('../bindings/ConformanceCheck').ConformanceCheck[]
  requirement_ids?: string[]
  requirement_allocations?: Record<string, string>
  setup_steps?: string[]
  ci_steps: string[]
  review_prompt?: string | null
  check_timeout_seconds?: number
}

export interface DefaultRoleAssignment {
  role_name: string
  assignee_type: string
  assignee_id: string | null
}

export interface RetryBudgets {
  review?: number | null
  review_progress?: number | null
  review_verify_rerun?: number | null
  merge_fix?: number | null
  execution?: number | null
}

export type LifecycleEvent =
  | 'before_work'
  | 'on_work_start'
  | 'on_work_stop'
  | 'on_task_done'
  | 'on_task_cancel'

export type LifecycleHookDef =
  | {
      type: 'script'
      command: string
      timeout_seconds: number
      blocking: boolean
    }
  | { type: 'plugin'; name: string; enabled: boolean; config: Record<string, unknown> | null }

export type LifecycleHooks = Partial<Record<LifecycleEvent, LifecycleHookDef[]>>

export interface TestLifecycleHookRequest {
  task_id: string
  event: LifecycleEvent
  hook_index: number
}

export interface LifecycleHookTestResponse {
  status: string
  stdout: string
  stderr: string
  exit_code: number | null
  duration_ms: number
  timeout: boolean
  working_dir: string
  environment_preview: Record<string, string>
  hook_log_path: string | null
}

export interface ProjectSettings {
  placement?: ProjectPlacementSettings
  max_active_tasks: number
  retry_budgets: RetryBudgets
  default_role_assignments: DefaultRoleAssignment[]
  lifecycle_hooks: LifecycleHooks
  automatic_recovery: AutomaticRecoverySettings
  environment: ProjectEnvironment
}

export interface ProjectEnvironment {
  recheck_interval_seconds: number
  env: Record<string, string>
  assets: EnvironmentAsset[]
  checks: EnvironmentCheck[]
}

export interface EnvironmentAsset {
  source: string
  target: string
}

export type EnvironmentCheckScope = 'workspace' | 'machine'

export type PlacementProvision = 'when_verified' | 'never'

export interface ProjectPlacementSettings {
  provision_timeout_seconds?: number
  provision: PlacementProvision
}

export interface EnvironmentCheck {
  scope?: EnvironmentCheckScope
  name: string
  command: string
  roles: string[]
  timeout_seconds: number
}

export interface AutomaticRecoverySettings {
  enabled: boolean
  agent_id: string | null
  max_attempts: number
}

// --- Workflow Definition ---

export type StateKind = 'backlog' | 'initial' | 'active' | 'gate' | 'terminal' | 'custom'

export interface GateConfig {
  reject_target: string | null
  max_rejections: number | null
  approve_label?: string | null
  reject_label?: string | null
  requires_user_approval?: boolean
  optional_when_unassigned?: boolean
}

export interface HookSpec {
  action: string
  params: Record<string, unknown>
  applies_to: 'All' | 'AgentOnly' | 'UserOnly'
  on_failure: string
}

export interface StateHooks {
  before_exit: HookSpec[]
  on_exit: HookSpec[]
  before_enter: HookSpec[]
  on_enter: HookSpec[]
  after_enter: HookSpec[]
}

export interface StateDefinition {
  name: string
  kind: StateKind
  column: string
  display_name: string
  role: string | null
  hooks: StateHooks
  gate_config: GateConfig | null
  config: Record<string, unknown>
}

export interface RoleDefinition {
  name: string
  display_name: string
  description: string
}

export type WorkflowConfigValueType = 'integer' | 'text'

export type WorkflowConfigBinding =
  | { type: 'gate_config'; state: string; field: string }
  | { type: 'state_config'; state: string; path: string[] }

export interface WorkflowConfigField {
  id: string
  label: string
  description: string | null
  value_type: WorkflowConfigValueType
  min: number | null
  default_value: unknown
  binding: WorkflowConfigBinding
}

export interface WorkflowDefinition {
  roles: RoleDefinition[]
  states: StateDefinition[]
  configuration: WorkflowConfigField[]
  cancellation_state: string | null
}

// --- Workflow Templates ---

export interface WorkflowTemplateSummary {
  name: string
  display_name: string
  description: string
  is_builtin: boolean
}

export interface WorkflowTemplateResponse {
  name: string
  display_name: string
  description: string
  is_builtin: boolean
  definition: WorkflowDefinition
}

export interface SaveWorkflowTemplateRequest {
  display_name?: string | null
  description?: string | null
  definition: WorkflowDefinition
}

export interface UpdateProjectWorkflowRequest {
  template_name?: string | null
  definition?: WorkflowDefinition | null
}

// --- Transition log ---

export interface HookResultEntry {
  action: string
  phase: string
  outcome: string
  duration_ms: number | null
  error: string | null
}

export interface TransitionLogEntry {
  bridge_kind: TransitionBridgeKind | null
  bridge_payload: unknown | null
  id: string
  task_id: string
  from_state: string
  to_state: string
  triggered_by: string
  trigger_reason: string
  hook_results_json: HookResultEntry[]
  rejection: boolean
  created_at: string
}

// Types aligned with the backend api-types crate responses.
// PaginatedResponse<T> = { data: T[], next_cursor?, has_more, total_count? }

import type { CanonicalPhase } from './bindings/CanonicalPhase'
import type { ProjectHookRule } from './bindings/ProjectHookRule'

export type TaskStatus = string

export type TaskType = 'task' | 'planning_task' | 'sub_task' | 'discovery'

export type ExecutionRole = string
export type ExecutionStatus = 'running' | 'completed' | 'failed' | 'cancelled'
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
export type ExecutionActionKind =
  | 'manual_launch'
  | 'session_follow_up'
  | 'workflow_resume'
  | 're_execute'
  | 'stop_execution'
  | 'cancel_task'
export type ExecutionAction = {
  action: ExecutionActionKind
  label: string
  enabled: boolean
  propagates: boolean
  requires_session: boolean
  disabled_reason: string | null
  target_execution_id: string | null
}
export type RecoveryAction =
  | 'resume_session'
  | 'reexecute'
  | 'reset_to_initial'
  | 'cancel_task'
  | 'mark_reviewed'
  | 'retry_hook'
  | 'resume_process'
  | 'update_workspace_and_retry_hook'
  | 'skip_hook_once'
  | 'reset_retry_window'
  | 'proceed_once'
  | 'open_interactive'
export type AgentStatus = 'idle' | 'busy' | 'error' | 'offline'
export type ReviewStatus = 'running' | 'awaiting_human' | 'passed' | 'failed' | 'cancelled'
export type DaemonStatus = 'online' | 'offline'
export type WorkMode = 'direct_merge' | 'pull_request'
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

export interface BlockingArtifact {
  kind: string
  id: string | null
  log_path: string | null
}

export type FailureKind =
  | 'merge_conflict'
  | 'target_repo_dirty'
  | 'dirty_worktree'
  | 'ci_failed'
  | 'review_gate_failed'
  | 'review_budget_exhausted'
  | 'retry_exhausted'
  | 'merge_fix_budget_exhausted'
  | 'workflow_guard_rejected'
  | 'internal_command_failed'
  | 'pr_closed_without_merge'
  | 'executor_failed'
  | 'workspace_failed'
  | 'workspace_reset_required'
  | 'workspace_error'
  | 'before_work_hook_timeout'
  | 'before_work_hook_failed'
  | 'max_turns_exceeded'
  | 'manual_stop'
  | 'recovery_required'
  | 'executor_unavailable'
  | 'unknown'

export interface TaskBlockingAnnotation {
  type: FailureKind
  blocking_reason: string
  blocked_by: string | null
  blocked_at: string | null
  blocked_execution_id: string | null
  artifact: BlockingArtifact | null
  message: string | null
  hook?: Record<string, unknown> | null
  recovery_actions: RecoveryAction[]
}

export type TaskAnnotation = TaskBlockingAnnotation | Record<string, unknown>

export interface ReviewConfig {
  ci_steps: string[]
  review_prompt?: string | null
}

export interface DefaultRoleAssignment {
  role_name: string
  assignee_type: string
  assignee_id: string | null
}

export interface TaskRoleAssignmentResponse {
  id: string
  task_id: string
  role_name: string
  assignee_type: string | null
  assignee_id: string | null
  created_at: string
  updated_at: string
}

export interface RetryBudgets {
  review?: number | null
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
  retry_budgets: RetryBudgets
  default_role_assignments: DefaultRoleAssignment[]
  lifecycle_hooks: LifecycleHooks
  automatic_recovery: AutomaticRecoverySettings
}

export interface AutomaticRecoverySettings {
  enabled: boolean
  agent_id: string | null
  max_attempts: number
}

export interface TaskMetadata {
  retry_budgets?: RetryBudgets | null
}

export interface InterruptionMetadata {
  reason: string
  created_at: string
  kind?: FailureKind | null
  source?: string | null
  execution_id?: string | null
  details?: Record<string, unknown> | null
}

export type WorkflowHealthKind =
  | 'idle'
  | 'waiting_for_agent'
  | 'running'
  | 'awaiting_human'
  | 'blocked'
  | 'failed'
  | 'stuck'

export type HealthSeverity = 'info' | 'warning' | 'error'

export interface WorkflowHealthSummary {
  kind: WorkflowHealthKind
  label: string
  severity: HealthSeverity
  message: string | null
  state: string | null
  role: string | null
  execution_id: string | null
  review_id: string | null
  since: string | null
  stale_reason: string | null
}

export interface FailingStepSummary {
  index: number
  command: string | null
  exit_code: number | null
  output_tail: string | null
  stderr_tail: string | null
}

export interface RelatedEvidence {
  kind: string
  id: string | null
  message: string | null
}

export interface WorkflowExceptionAction {
  kind: RecoveryAction
  label: string
  enabled: boolean
  disabled_reason: string | null
  requires_reason: boolean
  requires_guidance: boolean
  propagates: boolean
  target_state: string | null
  target_role: string | null
  target_execution_id: string | null
}

export interface WorkflowExceptionSummary {
  type: string
  message: string
  review_id: string | null
  execution_id: string | null
  state: string | null
  role: string | null
  target_state: string | null
  target_role: string | null
  failing_step: FailingStepSummary | null
  related_evidence: RelatedEvidence[]
  actions: WorkflowExceptionAction[]
}

export interface PrProviderStatus {
  provider_type: string
  has_token: boolean
  polling_interval_seconds: number
}

export interface PrSummary {
  pr_url?: string | null
  pr_state: string
  source_branch: string
  target_branch: string
  merge_status: string
}

export interface TaskExecutionObservability {
  execution_count: number
  active_execution_id?: string | null
  active_role?: string | null
  active_started_at?: string | null
  active_elapsed_seconds?: number | null
  latest_execution_id?: string | null
  latest_execution_status?: string | null
  latest_role?: string | null
  latest_started_at?: string | null
  latest_stopped_at?: string | null
  latest_runtime_seconds?: number | null
  total_runtime_seconds: number
  total_input_tokens: number
  total_output_tokens: number
  total_cache_read_tokens: number
  total_cache_write_tokens: number
  total_tokens: number
  total_cost_usd?: number | null
}

export interface PromptPreviewResponse {
  system: string
  user: string
  tools: string[] | null
}

export interface MemorySearchQuery {
  query: string
  layer?: number | null
  token_budget?: number | null
  limit?: number | null
  cursor?: string | null
}

export interface MemoryGetQuery {
  layer?: number | null
  project_id?: string | null
}

export interface MemorySearchResultDto {
  id: string
  layer: number
  content: string
  score: number
  source_type: string
  source_id: string
  project_id: string
  task_id: string | null
  created_at: string
  creator: string | null
}

export interface MemorySearchResponse {
  items: MemorySearchResultDto[]
  has_more: boolean
  next_cursor: string | null
}

export interface MemoryPublicationRequest {
  source_scope_type: string
  source_scope_id: string
  target_scope_type: string
  target_scope_id: string
  target_project_id: string | null
  target_task_id: string | null
  target_chat_id: string | null
  target_visibility: string
  target_authority: string
  actor_identity_id: string
  reason: string
  evidence_json: string
}

export interface MemoryLifecycleRequest {
  scope_type: string
  scope_id: string
  assertion_type: string
  related_memory_id: string | null
  reason: string | null
  evidence_json: string
  actor_identity_id: string
}

export interface MemoryLifecycleResponse {
  id: string
  memory_item_id: string
  assertion_type: string
  related_memory_id: string | null
  reason: string | null
  evidence_present: boolean
  asserted_by_type: string
  asserted_by_id: string | null
  source_event_id: string | null
  created_at: string
}

export interface MemoryProvenanceResponse {
  id: string
  scope_type: string
  scope_id: string
  visibility: string
  owner_identity_id: string | null
  authority: string
  sensitivity: string
  retention_priority: number
  source_type: string
  source_ref: string | null
  source_event_id: string | null
  source_scope_type: string | null
  source_scope_id: string | null
  source_revision: string | null
  source_chat_sequence: number | null
  publication_source_id: string | null
  supersedes_id: string | null
  valid_from: string | null
  valid_until: string | null
  created_by_type: string | null
  created_by_id: string | null
  created_at: string
  lifecycle: MemoryLifecycleResponse[]
}

export interface ContextManifestQuery {
  identity_id: string
  context_scope_id: string
}

export interface ContextManifestListQuery {
  context_scope_id: string | null
  limit: number | null
}

export interface MemoryProvenanceQuery {
  scope_type: string
  scope_id: string
  identity_id: string
}

export interface ContextManifestSourceResponse {
  ordinal: number
  source_id: string
  source_type: string
  source_revision: string
  selection_reason: string
  disposition: string
  is_stale: boolean
  current_revision: string | null
  retention_priority: number
  fragment_fingerprint: string
}

export interface ContextManifestResponse {
  id: string
  identity_id: string
  agent_session_id: string | null
  context_scope_id: string
  scope_type: string
  scope_id: string
  policy_revision: string
  domain_revision: string
  lcm_binding_revision: string | null
  runtime_manifest_id: string | null
  runtime_manifest_fingerprint: string | null
  combined_fingerprint: string
  request_fingerprint: string
  created_at: string
  sources: ContextManifestSourceResponse[]
}

export interface ContextManifestListResponse {
  items: ContextManifestResponse[]
  has_more: boolean
}

// --- Paginated wrapper (matches api_types::PaginatedResponse<T>) ---

export interface PaginatedResponse<T> {
  items: T[]
  next_cursor?: string | null
  has_more: boolean
  total_count?: number | null
}

// --- Task (matches api_types::TaskResponse) ---

export interface Task {
  id: string
  project_id: string
  repo_id: string | null
  parent_task_id?: string | null
  assignee_type?: string | null
  assignee_id?: string | null
  title: string
  description?: string | null
  task_type: TaskType
  status: TaskStatus
  canonical_phase?: CanonicalPhase
  priority: number
  board_position: number
  subtask_order?: number | null
  role_assignments: TaskRoleAssignmentResponse[]
  remaining_retries: Record<string, number>
  execution_actions?: ExecutionAction[]
  pr_summary?: PrSummary | null
  awaiting_human?: boolean
  error_annotation?: TaskAnnotation | null
  blocked?: InterruptionMetadata | null
  failed?: InterruptionMetadata | null
  workflow_health?: WorkflowHealthSummary | null
  workflow_exception?: WorkflowExceptionSummary | null
  external_issue_number?: number | null
  external_issue_url?: string | null
  review_passed_at?: string | null
  archived_at?: string | null
  workspace?: Workspace | null
  execution_observability?: TaskExecutionObservability
  plan_progress?: PlanProgressSummary | null
  plan_artifact?: PlanArtifactDetail | null
  version: number
  created_at: string
  updated_at: string
}

export type TaskResponse = Task

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
  workspace_id?: string | null
  plan_progress?: PlanProgressSummary | null
  plan_artifact?: PlanArtifactDetail | null
  usage?: ExecutionUsage[] | null
  created_at: string
  updated_at: string
}

export interface RecoverTaskRequest {
  action: RecoveryAction
  reason: string | null
  context: string | null
}

export type ExecutionResponse = Execution

// --- Agent (matches api_types::AgentResponse) ---

export interface Agent {
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
  active_task_count?: number | null
  effective_status?: string | null
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

// --- Project (matches api_types::ProjectResponse) ---

export interface Project {
  id: string
  name: string
  primary_repo_id: string | null
  settings: Record<string, unknown>
  project_hooks?: ProjectHookRule[]
  default_review_config?: ReviewConfig | null
  workflow_template_name?: string | null
  paused_at: string | null
  paused: boolean
  charter_status: string
  charter_setup_required: boolean
  current_charter_id: string | null
  current_charter_revision_id: string | null
  current_charter_version: number
  primary_milestone_id: string | null
  version: number
  created_at: string
  updated_at: string
}

// --- Repo (matches api_types::RepoResponse) ---

export interface Repo {
  id: string
  project_id: string
  name: string
  local_path: string | null
  remote_url: string
  default_branch: string
  work_mode: WorkMode
  pr_provider?: string | null
  pr_provider_status?: PrProviderStatus | null
  created_at: string
  updated_at: string
}

export interface RepoSyncResponse {
  pull_output: string
  push_output: string
}

// --- Filesystem (matches api_types::{FsEntry, FsListResponse, BranchListResponse}) ---

export interface FsEntry {
  name: string
  path: string
  is_dir: boolean
  is_git_repo: boolean
}

export interface FsListResponse {
  path: string
  entries: FsEntry[]
}

export interface BranchListResponse {
  branches: string[]
  default_branch: string | null
  origin_url: string | null
}

// --- Review (matches api_types::ReviewResponse) ---

export interface StepResult {
  index: number
  command: string
  exit_code: number
  stderr_tail: string
  output_tail: string
}

export interface StepResultEntry {
  index: number
  command: string
  exit_code: number
  stderr_tail: string
  output_tail: string
}

export interface AuditorVerdictEntry {
  verdict: string
  reason?: string | null
}

export interface ReviewDetails {
  ci_steps: StepResultEntry[]
  auditor?: AuditorVerdictEntry | null
}

export interface Review {
  id: string
  task_id: string
  execution_id: string
  attempt_number: number
  status: ReviewStatus
  step_results: StepResult[]
  details: ReviewDetails
  started_at: string
  finished_at: string | null
  created_at: string
  updated_at: string
}

export interface TransitionTaskResponse {
  task: Task
  review: Review | null
}

export interface NotificationResponse {
  id: string
  project_id: string
  task_id: string | null
  event_type: string
  title: string
  body: string | null
  read: boolean
  created_at: string
}

export interface UnreadCountResponse {
  count: number
}

export interface MoveTaskRequest {
  operation_id: string
  task_version: number
  board_revision: number
  target_status: string
  before_id: string | null
  after_id: string | null
}

export interface MoveTaskResponse {
  task: Task
  board_revision: number
  operation_id: string
}

export interface OperationConflictDetails {
  operation_id: string
}

export interface TaskVersionConflictDetails {
  expected_task_version: number
  actual_task_version: number
}

export interface BoardRevisionConflictDetails {
  expected_board_revision: number
  actual_board_revision: number
}

export interface TaskMovedEventPayload {
  project_id: string
  operation_id: string
  old_status: string
  new_status: string
  old_board_position: number
  new_board_position: number
  task_version: number
  board_revision: number
  before_id: string | null
  after_id: string | null
}

// --- Error (matches api_types::ErrorResponse) ---

export interface ErrorResponse {
  code: string
  message: string
  details?: Record<string, unknown> | null
  request_id: string
}

// --- Request types (matches api_types) ---

export interface InitialRoleAssignment {
  role_name: string
  assignee_type: AssigneeKind
  assignee_id?: string | null
}

export interface CreateTaskRequest {
  title: string
  description?: string | null
  parent_task_id?: string | null
  task_type?: TaskType
  priority?: number
  review_config?: ReviewConfig | null
  merge_config?: Record<string, unknown> | null
  role_assignments?: InitialRoleAssignment[] | null
}

export interface UpdateTaskRequest {
  title?: string
  description?: string | null
  priority?: number
  merge_config?: Record<string, unknown> | null
  plan?: string
  parent_task_id?: string | null
  version: number
}

export interface RejectReviewRequest {
  reason?: string | null
}

export interface ReviewDecisionResponse {
  task: Task
  review: Review
}

export type AssigneeKind = 'agent' | 'user'

export type AuthorType = 'user' | 'agent' | 'system'

export interface Comment {
  id: string
  task_id: string
  author_type: AuthorType
  author_id?: string | null
  author_name: string
  content: string
  created_at: string
  updated_at: string
}

export interface CreateCommentRequest {
  content: string
  author_name: string
}

export interface TransitionTaskRequest {
  status: string
  version: number
  reason?: string | null
  source?: 'board_drag' | null
}

export interface ClaimOverrides {
  model_id?: string
  reasoning_effort?: string
  permission_policy?: string
}

export interface ClaimTaskRequest {
  agent_id: string
  overrides?: ClaimOverrides | null
}

export interface LaunchExecutionRequest {
  agent_id: string
  summary?: string | null
  overrides?: ClaimOverrides | null
}

export interface FollowUpRequest {
  message: string
  agent_id?: string
  overrides?: {
    model_id?: string
    reasoning_effort?: string
    permission_policy?: string
  }
}

export interface LaunchExecutionResponse {
  data: {
    task: Task
    execution: Execution
    workspace: Workspace
    execution_behavior?: ExecutionBehavior
  }
}

export type DiffFileStatus = 'added' | 'modified' | 'deleted' | 'renamed'

export interface FileDiffSummary {
  path: string
  status: DiffFileStatus
  additions: number
  deletions: number
}

export interface DiffStats {
  files_changed: number
  total_additions: number
  total_deletions: number
}

export interface DiffResponse {
  base_ref: string
  head_ref: string
  base_sha: string
  head_sha: string
  files: FileDiffSummary[]
  stats: DiffStats
  diff: string
}

export interface DiffEnvelope {
  data: DiffResponse
}

export interface CreateProjectRequest {
  name: string
  settings?: Record<string, unknown> | null
  default_review_config?: ReviewConfig | null
  paused?: boolean | null
  project_agent_identity_id?: string | null
  project_agent_profile_id?: string | null
  product_genesis_session_id?: string | null
}

export interface UpdateProjectRequest {
  version: number
  name?: string
  primary_repo_id?: string | null
  settings?: Record<string, unknown> | null
  default_review_config?: ReviewConfig | null
  project_hooks?: ProjectHookRule[] | null
}

export interface CreateRepoRequest {
  remote_url: string
  local_path?: string | null
  name?: string | null
  default_branch?: string | null
  work_mode?: WorkMode
  pr_provider?: string | null
  pr_provider_config?: {
    base_url?: string | null
    polling_interval_seconds?: number | null
    token?: string | null
  } | null
}

export interface UpdateRepoRequest {
  name?: string | null
  local_path?: string | null
  remote_url?: string
  default_branch?: string | null
  work_mode?: WorkMode
  pr_provider?: string | null
  pr_provider_config?: {
    base_url?: string | null
    polling_interval_seconds?: number | null
    token?: string | null
  } | null
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
  active_task_count: number
  max_concurrent_tasks: number
  reason?: string | null
}

export interface Workspace {
  id: string
  task_id: string
  repo_id: string
  worktree_path: string
  branch: string
  status: string
  before_sha?: string | null
  error?: string | null
  created_at: string
  updated_at: string
}

export interface Daemon {
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

export interface AssignRoleRequest {
  assignee_type: string
  assignee_id: string | null
  reset_workspace?: boolean | null
  reset_worktree?: boolean | null
}

export interface ReorderSubtasksRequest {
  ordered_ids: string[]
}

export type ExecutionsResponse = PaginatedResponse<Execution>
export type AgentsResponse = PaginatedResponse<Agent>
export type ProjectsResponse = PaginatedResponse<Project>
export type TasksResponse = PaginatedResponse<Task> & { board_revision: number }

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

// --- Execution Usage (matches api_types::ExecutionUsageResponse) ---

export interface ExecutionUsage {
  id: string
  execution_id: string
  provider: string
  model: string
  input_tokens: number
  output_tokens: number
  cache_read_tokens: number
  cache_write_tokens: number
  cost_usd?: number | null
  created_at: string
}

export interface TaskUsageSummary {
  total_input_tokens: number
  total_output_tokens: number
  total_cache_read_tokens: number
  total_cache_write_tokens: number
  total_cost_usd?: number | null
  execution_count: number
}

export interface ProjectAnalyticsResponse {
  ci_steps: CiStepAnalytics[]
  token_usage: TokenUsageAnalytics
  review_summary: ReviewSummaryAnalytics
}

export interface CiStepAnalytics {
  command: string
  total_runs: number
  pass_count: number
  fail_count: number
  success_rate: number
  avg_duration_ms: number | null
  p50_duration_ms: number | null
  p95_duration_ms: number | null
  last_run_at: string | null
}

export interface TokenUsageAnalytics {
  total_input_tokens: number
  total_output_tokens: number
  total_cache_read_tokens: number
  total_cache_write_tokens: number
  total_cost_usd: number | null
  execution_count: number
  by_model: ModelTokenBreakdown[]
  by_agent: AgentTokenBreakdown[]
}

export interface ModelTokenBreakdown {
  provider: string
  model: string
  input_tokens: number
  output_tokens: number
  cache_read_tokens: number
  cache_write_tokens: number
  cost_usd: number | null
  execution_count: number
}

export interface AgentTokenBreakdown {
  agent_id: string
  agent_name: string
  executor_type: string
  model: string | null
  input_tokens: number
  output_tokens: number
  cache_read_tokens: number
  cache_write_tokens: number
  cost_usd: number | null
  execution_count: number
  success_rate: number | null
  avg_duration_ms: number | null
}

export interface ReviewSummaryAnalytics {
  total_reviews: number
  passed: number
  failed: number
  cancelled: number
  avg_duration_ms: number | null
  pass_rate: number
}

export type OperatorSeverity = 'healthy' | 'attention' | 'blocked' | 'error'

export interface OperatorStatusResponse {
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
  daemon_id: string
  hostname: string | null
  active_sessions: number
  max_sessions: number | null
  at_capacity: boolean
}

export interface AgentPressureSummary {
  agent_id: string
  agent_name: string
  daemon_id: string | null
  active_sessions: number
  max_sessions: number
  at_capacity: boolean
}

export interface TokenTotalsSummary {
  input_tokens: number
  output_tokens: number
  cache_read_tokens: number
  cache_write_tokens: number
  cost_usd: number | null
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
  available: boolean
  total_input_tokens: number | null
  total_output_tokens: number | null
  total_cost_usd: number | null
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

export interface SettingsResponse {
  config_path: string
  restart_required: boolean
  settings: ForgeSettingResponse[]
}

export interface ForgeSettingResponse {
  key: string
  value: unknown | null
  effective_value: unknown
  restart_required: boolean
}

export interface UpdateSettingsRequest {
  forge?: UpdateForgePathsRequest
  server?: UpdateServerSettingsRequest
  workspace?: UpdateWorkspaceSettingsRequest
  agent?: UpdateAgentSettingsRequest
  project?: Record<string, string>
}

export interface UpdateForgePathsRequest {
  data_dir?: string | null
}

export interface UpdateServerSettingsRequest {
  bind?: string | null
  mcp_enabled?: boolean | null
}

export interface UpdateWorkspaceSettingsRequest {
  root?: string | null
  cleanup_delay_seconds?: number | null
}

export interface UpdateAgentSettingsRequest {
  max_concurrent_tasks?: number | null
  heartbeat_interval_seconds?: number | null
  max_missed_heartbeats?: number | null
}

export interface RegisterRequest {
  email: string
  password: string
  display_name: string | null
}

export interface LoginRequest {
  email: string
  password: string
}

export interface RefreshRequest {
  refresh_token: string
}

export interface LogoutRequest {
  refresh_token: string
}

export interface AuthResponse {
  access_token: string
  refresh_token: string
  token_type: string
  expires_in: number
}

export interface UserResponse {
  id: string
  email: string
  display_name: string | null
  is_admin: boolean
  created_at: string
}

export interface UpdateProfileRequest {
  email?: string | null
  display_name?: string | null
}

export type MemberRole = 'owner' | 'admin' | 'member' | 'viewer'

export interface TokenResponse {
  id: string
  name: string
  token?: string
  prefix: string
  scopes: string
  expires_at: string | null
  last_used_at: string | null
  created_at: string
}

export interface ProjectMemberResponse {
  id: string
  user_id: string
  email: string
  display_name: string | null
  role: MemberRole
  created_at: string
}

export interface UserSearchResult {
  id: string
  email: string
  display_name: string | null
}

export interface ProviderUsageWindow {
  id: string
  used_percent: number
  window_minutes: number | null
  resets_at: string | null
}

export interface ProviderUsageResponse {
  id: string
  provider: string
  source: 'probe' | 'unknown'
  plan_type?: string | null
  windows: ProviderUsageWindow[]
  fetched_at: string
  detail: string | null
}

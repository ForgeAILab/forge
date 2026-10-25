// Task, review, comment, and Task action types.

import type { IntegrationHeadCheckEvidence } from '../bindings/IntegrationHeadCheckEvidence'
import type { TaskCondition } from '../bindings/TaskCondition'
import type { TaskIntegrationSummary } from '../bindings/TaskIntegrationSummary'
import type { ReviewConformance } from '../bindings/ReviewConformance'
// Types aligned with the backend api-types crate responses.
// PaginatedResponse<T> = { data: T[], next_cursor?, has_more, total_count? }
import type { CanonicalPhase } from '../bindings/CanonicalPhase'
import type { ExecutionBlockerProjection } from '../bindings/ExecutionBlockerProjection'
import type { ExecutionEvidenceSummary } from '../bindings/ExecutionEvidenceSummary'
import type { ActivityCounts } from '../bindings/ActivityCounts'
import type { CostSummary } from '../bindings/CostSummary'
import type { TokenCounters } from '../bindings/TokenCounters'
import type { WorkflowDefinition as GeneratedWorkflowDefinition } from '../bindings/WorkflowDefinition'
import type { TaskPlacementDiagnostic } from '../bindings/TaskPlacementDiagnostic'
import type { Offer } from '../bindings/Offer'
import type { RetryBudgets, ReviewConfig } from './workflow'
import type { WorkspacePlacementResponse, Workspace } from './runtime'
import type {
  PlanProgressSummary,
  PlanArtifactDetail,
  ExecutionSummary,
  ExecutionBehavior,
  Execution,
} from './executions'
import type { PaginatedResponse } from './common'

export type TaskStatus = string

export type TaskType = 'task' | 'planning_task' | 'sub_task' | 'discovery'

export type ReviewStatus = 'running' | 'awaiting_human' | 'passed' | 'failed' | 'cancelled'

export interface BlockingArtifact {
  kind: string
  id: string | null
  log_path: string | null
}

export type FailureKind = import('../bindings/FailureKind').FailureKind

export interface TaskBlockingAnnotation {
  type: FailureKind
  blocking_reason: string
  blocked_by: string | null
  blocked_at: string | null
  blocked_execution_id: string | null
  artifact: BlockingArtifact | null
  message: string | null
  hook?: Record<string, unknown> | null
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

export type EffectiveCoderSource = 'own' | 'inherited_from_root'

export interface EffectiveLeadResponse {
  assignment: TaskRoleAssignmentResponse
  source: EffectiveCoderSource
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
  actions: Offer[]
}

export interface TaskExecutionObservability {
  counts: ActivityCounts
  tokens: TokenCounters
  cost: CostSummary
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
}

// --- Task (matches api_types::TaskResponse) ---

export interface Task {
  placement_diagnostics?: TaskPlacementDiagnostic[]
  id: string
  project_id: string
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
  effective_coder: TaskRoleAssignmentResponse | null
  effective_coder_source: EffectiveCoderSource | null
  effective_lead?: EffectiveLeadResponse | null
  remaining_retries: Record<string, number>
  retry_limits?: Record<string, number>
  available_actions?: Offer[]
  awaiting_human?: boolean
  condition: TaskCondition
  /** The Task's place in the merge queue; null without an attempt in progress. */
  integration?: TaskIntegrationSummary | null
  workflow_health?: WorkflowHealthSummary | null
  workflow_exception?: WorkflowExceptionSummary | null
  external_issue_number?: number | null
  external_issue_url?: string | null
  review_passed_at?: string | null
  archived_at?: string | null
  workspace?: Workspace | null
  placement: WorkspacePlacementResponse | null
  execution_observability?: TaskExecutionObservability
  plan_progress?: PlanProgressSummary | null
  plan_artifact?: PlanArtifactDetail | null
  // Canonical attempt/execution/commit evidence for this Task (D17, F12).
  // Progress language everywhere else must derive from this value; it can
  // never be overridden to show "not started" once evidence exists. Optional
  // here (unlike the Rust response, which always sends it) only so existing
  // hand-built `Task` fixtures across the frontend do not all need updating;
  // every real response includes it.
  execution_evidence?: ExecutionEvidenceSummary
  // The one canonical execution blocker for this Task (D16/D17), or `null`
  // when nothing blocks this Task's execution right now. A Task-scoped
  // blocker here never implies the whole Project is blocked.
  execution_blocker?: ExecutionBlockerProjection | null
  version: number
  created_at: string
  updated_at: string
}

// --- Project task list item (matches api_types::TaskListItemResponse) ---

export interface TaskListItem {
  id: string
  project_id: string
  parent_task_id: string | null
  assignee_type: string | null
  assignee_id: string | null
  title: string
  task_type: TaskType
  status: TaskStatus
  canonical_phase: CanonicalPhase
  awaiting_human: boolean
  priority: number
  board_position: number
  subtask_order: number | null
  role_assignments: TaskRoleAssignmentResponse[]
  remaining_retries: Record<string, number>
  retry_limits: Record<string, number>
  condition: TaskCondition
  /** The Task's place in the merge queue; null without an attempt in progress. */
  integration: TaskIntegrationSummary | null
  workflow_health: WorkflowHealthSummary | null
  workflow_exception: WorkflowExceptionSummary | null
  review_passed_at: string | null
  archived_at: string | null
  external_issue_number: number | null
  external_issue_url: string | null
  execution_observability: { latest_execution_id: string | null }
  version: number
  created_at: string
  updated_at: string
}

export type TaskResponse = Task

/** The first execution page embedded in a Task detail bootstrap response. */
export interface TaskDetailExecutionsPage {
  items: ExecutionSummary[]
  next_cursor: string | null
  has_more: boolean
  total_count: number | null
}

export interface TaskDetailResponse {
  task: Task
  workflow: GeneratedWorkflowDefinition
  executions: TaskDetailExecutionsPage
}

export interface TaskRelationSummary {
  id: string
  title: string
  status: TaskStatus
  parent_task_id: string | null
  subtask_order: number | null
  created_at: string
}

export interface TaskRelationsResponse {
  parent: TaskRelationSummary | null
  subtasks: TaskRelationSummary[]
  dependencies: TaskRelationSummary[]
  missing_dependency_ids: string[]
  dependents: TaskRelationSummary[]
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
  verify?: import('../bindings/ReviewVerifyDetails').ReviewVerifyDetails | null
  execution?: { integration_checks: IntegrationHeadCheckEvidence[]; [key: string]: unknown } | null
  conformance: ReviewConformance
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

export type TransitionTaskResponse =
  import('../bindings/TransitionTaskResponse').TransitionTaskResponse

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
  task_state_config?: Record<string, unknown> | null
  review_requirement_ids?: string[]
  parent_task_id?: string | null
  version: number
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
  /** CommentResponse.kind in crates/api-types/src/repo_review.rs; absent on ordinary comments. */
  kind?: string | null
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
  /** Optional wall-clock limit for this execution; omit for no limit. */
  hard_deadline_seconds?: number
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

export interface AssignRoleRequest {
  assignee_type: string
  assignee_id: string | null
  reset_workspace?: boolean | null
  reset_worktree?: boolean | null
}

export interface ReorderSubtasksRequest {
  ordered_ids: string[]
}

// GET task lists support ETag/If-None-Match with private, no-cache; 304 preserves the cached page.
export type TasksResponse = PaginatedResponse<TaskListItem> & { board_revision: number }

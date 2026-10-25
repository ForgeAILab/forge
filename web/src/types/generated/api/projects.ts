// Project records and mutation types.

import type { ProjectHookRule } from '../bindings/ProjectHookRule'
import type { ProjectExecutionSetupResponse } from '../bindings/ProjectExecutionSetupResponse'
import type { EnvironmentPauseResponse } from '../bindings/EnvironmentPauseResponse'
import type { ProjectEnvironmentReadiness } from '../bindings/ProjectEnvironmentReadiness'
import type { ProjectSlots } from '../bindings/ProjectSlots'
import type { ReviewConfig } from './workflow'
import type { PaginatedResponse } from './common'

// --- Project (matches api_types::ProjectResponse) ---

export interface Project {
  id: string
  name: string
  primary_repo_id: string | null
  owner_id: string | null
  settings: Record<string, unknown>
  project_hooks?: ProjectHookRule[]
  default_review_config?: ReviewConfig | null
  workflow_template_name?: string | null
  paused_at: string | null
  system_pause_reason?: string | null
  environment_pause: EnvironmentPauseResponse | null
  environment_readiness?: ProjectEnvironmentReadiness[]
  slots: ProjectSlots
  paused: boolean
  charter_status: string
  charter_setup_required: boolean
  current_charter_id: string | null
  current_charter_revision_id: string | null
  current_charter_version: number
  primary_milestone_id: string | null
  version: number
  execution_setup: ProjectExecutionSetupResponse | null
  created_at: string
  updated_at: string
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

export type ProjectsResponse = PaginatedResponse<Project>

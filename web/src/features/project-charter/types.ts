import type { AuthorizationProvenance } from '@/types/generated/bindings/AuthorizationProvenance'
import type { MutationEnvelope } from '@/types/generated/bindings/MutationEnvelope'
import type { ProjectMode } from '@/types/generated/bindings/ProjectMode'

// Generated ts-rs bindings preserve Rust i64 as bigint; the wire carries plain
// JSON numbers, the same convention product-genesis uses for optimistic
// versions.
export type ProjectCharterMutationEnvelope = Omit<MutationEnvelope, 'expected_version'> & {
  expected_version: number
}

/**
 * A Project (adoption or amendment) Charter approval. Unlike the Genesis
 * approval this one targets an existing Project, so it must pin the Project
 * version the user reviewed against.
 */
export interface ApproveProjectCharterInput {
  mutation: ProjectCharterMutationEnvelope
  charter_id: string
  revision_id: string
  content_digest: string
  render_digest: string
  expected_charter_version: number
  expected_project_version: number
  approved_project_name: string
  approved_project_slug: string | null
  project_mode: ProjectMode
  selected_project_agent_identity_id: string
  selected_project_agent_profile_revision_id: string
  selected_project_agent_operating_skill_revision: string
  selected_project_agent_policy_digest: string
}

export type { AuthorizationProvenance }

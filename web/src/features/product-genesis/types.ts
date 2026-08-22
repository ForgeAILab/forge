import type { ProductGenesisActiveResponse } from '@/types/generated/bindings/ProductGenesisActiveResponse'
import type { ProductGenesisSession } from '@/types/generated/bindings/ProductGenesisSession'
import type { ProductGenesisStartResponse } from '@/types/generated/bindings/ProductGenesisStartResponse'
import type { ProductMaturity } from '@/types/generated/bindings/ProductMaturity'
import type { AuthorizationProvenance } from '@/types/generated/bindings/AuthorizationProvenance'
import type { CreateProjectFromCharterApprovalResponse } from '@/types/generated/bindings/CreateProjectFromCharterApprovalResponse'
import type { MutationEnvelope } from '@/types/generated/bindings/MutationEnvelope'
import type { ProductAgentSelection } from '@/types/generated/bindings/ProductAgentSelection'
import type { ProductGenesisCharterResponse } from '@/types/generated/bindings/ProductGenesisCharterResponse'
import type { ProjectCharterContent } from '@/types/generated/bindings/ProjectCharterContent'
import type { ProjectCharter } from '@/types/generated/bindings/ProjectCharter'
import type { ProjectCharterApproval } from '@/types/generated/bindings/ProjectCharterApproval'
import type { ProjectCharterReadiness } from '@/types/generated/bindings/ProjectCharterReadiness'
import type { ProjectCharterRevision } from '@/types/generated/bindings/ProjectCharterRevision'
import type { ProjectMode } from '@/types/generated/bindings/ProjectMode'
import type { RevisionProvenance } from '@/types/generated/bindings/RevisionProvenance'

export type { ProductGenesisSession, ProductMaturity }
export type ProductGenesisActive = ProductGenesisActiveResponse
export type ProductGenesisStart = ProductGenesisStartResponse
export type {
  AuthorizationProvenance,
  CreateProjectFromCharterApprovalResponse,
  MutationEnvelope,
  ProductAgentSelection,
  ProductGenesisCharterResponse,
  ProjectCharterContent,
  ProjectCharter,
  ProjectCharterApproval,
  ProjectCharterReadiness,
  ProjectCharterRevision,
  ProjectMode,
  RevisionProvenance,
}

// Generated ts-rs types preserve Rust i64 as bigint. Browser JSON payloads
// use the repository's existing number wire convention for optimistic versions.
export type ProductGenesisMutationEnvelope = Omit<MutationEnvelope, 'expected_version'> & {
  expected_version: number
}

export interface ProductGenesisStartInput {
  idempotency_key: string
  maturity: ProductMaturity
  initial_idea: string | null
  preferred_project_agent_identity_id: string | null
}

export interface ProductGenesisCancelInput {
  expected_version: number
  reason: string | null
}

/**
 * The server response is intentionally a small projection over immutable
 * Charter records. Keeping this shape here means the Main Chat can render a
 * Charter without treating chat prose as the source of truth.
 */
export interface SaveProductGenesisCharterRevisionInput {
  mutation: ProductGenesisMutationEnvelope
  charter_id: string
  base_revision_id: string | null
  project_mode: ProjectMode
  maturity: ProductMaturity
  content: ProjectCharterContent
  rendered_view: string
  render_version: string
  provenance: RevisionProvenance
}

export interface ApproveProductGenesisCharterRevisionInput {
  mutation: ProductGenesisMutationEnvelope
  charter_id: string
  revision_id: string
  content_digest: string
  render_digest: string
  expected_charter_version: number
  approved_project_name: string
  approved_project_slug: string | null
  project_mode: ProjectMode
  selected_project_agent_identity_id: string
  selected_project_agent_profile_revision_id: string
  selected_project_agent_operating_skill_revision: string
  selected_project_agent_policy_digest: string
}

export interface CreateProjectFromCharterApprovalInput {
  approval_id: string
  idempotency_key: string
  authorization: AuthorizationProvenance
}

export function productGenesisVersion(session: ProductGenesisSession): number {
  return typeof session.version === 'bigint' ? Number(session.version) : session.version
}

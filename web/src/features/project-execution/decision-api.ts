import { apiFetch } from '@/api/client'
import type {
  ApproveDecisionCandidateRequest,
  DecisionCandidate,
  DecisionRecord,
  MutationEnvelope,
  RejectDecisionCandidateRequest,
} from '@/types/generated'

/** JSON carries Rust i64 versions as numbers; generated bindings use bigint. */
export type MutationEnvelopeWire = Omit<MutationEnvelope, 'expected_version'> & {
  expected_version: number
}

export type ApproveDecisionCandidateWire = Omit<ApproveDecisionCandidateRequest, 'mutation'> & {
  mutation: MutationEnvelopeWire
}

export type RejectDecisionCandidateWire = Omit<RejectDecisionCandidateRequest, 'mutation'> & {
  mutation: MutationEnvelopeWire
}

/**
 * `PendingDecisionSummary.approve_target`/`reject_target` (design D19, F15)
 * name the exact route a pending candidate action posts to, as an absolute
 * `/api/v1/...` path -- the same shape the server's own route table uses.
 * `apiFetch` already prefixes every call with `/api/v1`, so strip it here
 * rather than hand-maintaining a second copy of the route template.
 */
const API_PREFIX = '/api/v1'

function relativePath(targetPath: string): string {
  return targetPath.startsWith(API_PREFIX) ? targetPath.slice(API_PREFIX.length) : targetPath
}

export function approveDecisionCandidate(
  approveTargetPath: string,
  input: ApproveDecisionCandidateWire,
): Promise<DecisionRecord> {
  return apiFetch<DecisionRecord>(relativePath(approveTargetPath), {
    method: 'POST',
    body: JSON.stringify(input),
  })
}

export function rejectDecisionCandidate(
  rejectTargetPath: string,
  input: RejectDecisionCandidateWire,
): Promise<DecisionCandidate> {
  return apiFetch<DecisionCandidate>(relativePath(rejectTargetPath), {
    method: 'POST',
    body: JSON.stringify(input),
  })
}

import { apiFetch } from '@/api/client'
import type {
  MutationEnvelope,
  ProjectReconciliation,
  ProjectReconciliationListResponse,
  ResolveProjectReconciliationRequest,
} from '@/types/generated'

/**
 * Shared, scoped Project reconciliation routes (design D15). Resolution is
 * user-only: the shared service rejects any authorization whose principal is
 * not `user`, so this module never exposes a generic agent self-resolve call.
 */
export const projectReconciliationApiPaths = {
  list: (projectId: string) => `/projects/${projectId}/reconciliations`,
  detail: (projectId: string, reconciliationId: string) =>
    `/projects/${projectId}/reconciliations/${reconciliationId}`,
  resolve: (projectId: string, reconciliationId: string) =>
    `/projects/${projectId}/reconciliations/${reconciliationId}/resolve`,
} as const

/** JSON carries Rust i64 versions as numbers; generated bindings use bigint. */
export type MutationEnvelopeWire = Omit<MutationEnvelope, 'expected_version'> & {
  expected_version: number
}

export type ResolveProjectReconciliationWire = Omit<
  ResolveProjectReconciliationRequest,
  'mutation'
> & {
  mutation: MutationEnvelopeWire
}

export function listProjectReconciliations(
  projectId: string,
  cursor?: string,
): Promise<ProjectReconciliationListResponse> {
  return apiFetch<ProjectReconciliationListResponse>(projectReconciliationApiPaths.list(projectId), {
    search: { cursor, limit: 50 },
  })
}

export function getProjectReconciliation(
  projectId: string,
  reconciliationId: string,
): Promise<ProjectReconciliation> {
  return apiFetch<ProjectReconciliation>(
    projectReconciliationApiPaths.detail(projectId, reconciliationId),
  )
}

export function resolveProjectReconciliation(
  projectId: string,
  reconciliationId: string,
  input: ResolveProjectReconciliationWire,
): Promise<ProjectReconciliation> {
  return apiFetch<{ reconciliation: ProjectReconciliation }>(
    projectReconciliationApiPaths.resolve(projectId, reconciliationId),
    {
      method: 'POST',
      body: JSON.stringify(input),
    },
  ).then((response) => response.reconciliation)
}

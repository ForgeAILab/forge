import { apiFetch } from '@/api/client'
import type { ProductGenesisCharterResponse } from '@/types/generated/bindings/ProductGenesisCharterResponse'
import type { ProjectCharterApproval } from '@/types/generated/bindings/ProjectCharterApproval'

import type { ApproveProjectCharterInput } from './types'

export const projectCharterApiPaths = {
  charter: (projectId: string) => `/projects/${projectId}/charter`,
  approveRevision: (projectId: string, revisionId: string) =>
    `/projects/${projectId}/charter/revisions/${revisionId}/approve`,
} as const

export function getProjectCharter(projectId: string): Promise<ProductGenesisCharterResponse> {
  return apiFetch<ProductGenesisCharterResponse>(projectCharterApiPaths.charter(projectId))
}

export function approveProjectCharterRevision(
  projectId: string,
  revisionId: string,
  input: ApproveProjectCharterInput,
): Promise<ProjectCharterApproval> {
  return apiFetch<ProjectCharterApproval>(
    projectCharterApiPaths.approveRevision(projectId, revisionId),
    { method: 'POST', body: JSON.stringify(input) },
  )
}

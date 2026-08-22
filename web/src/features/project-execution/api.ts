import { apiFetch } from '@/api/client'
import type {
  AttachPrimaryRepositoryRequest,
  ProjectExecutionSetupResponse,
  RetryProvisioningRequest,
  SelectExecutionPrincipalRequest,
} from '@/types/generated'

/**
 * Project execution setup is a durable, server-owned projection. Keep the
 * route names in one place so the Overview and Project Chat cannot drift into
 * subtly different setup contracts.
 *
 * The action routes are intentionally role-specific. A reviewer selection is
 * never sent through the Worker endpoint, which makes the independence rule
 * explicit at the UI/API boundary.
 */
export const projectExecutionSetupApiPaths = {
  projection: (projectId: string) => `/projects/${projectId}/execution-setup`,
  worker: (projectId: string) => `/projects/${projectId}/execution-setup/worker`,
  independentReviewer: (projectId: string) =>
    `/projects/${projectId}/execution-setup/independent-reviewer`,
  primaryRepository: (projectId: string) => `/projects/${projectId}/execution-setup/repository`,
  retryProvisioning: (projectId: string) =>
    `/projects/${projectId}/execution-setup/provisioning/retry`,
} as const

export type SelectExecutionPrincipalWire = Omit<
  SelectExecutionPrincipalRequest,
  'expected_project_version'
> & {
  /** JSON carries Rust i64 versions as numbers; generated bindings use bigint. */
  expected_project_version: number
}

export type AttachPrimaryRepositoryWire = Omit<
  AttachPrimaryRepositoryRequest,
  'expected_project_version'
> & {
  expected_project_version: number
}

export type RetryProvisioningWire = Omit<RetryProvisioningRequest, 'expected_operation_version'> & {
  expected_operation_version: number
}

export function getProjectExecutionSetup(
  projectId: string,
): Promise<ProjectExecutionSetupResponse> {
  return apiFetch<ProjectExecutionSetupResponse>(
    projectExecutionSetupApiPaths.projection(projectId),
  )
}

export function selectWorker(
  projectId: string,
  input: SelectExecutionPrincipalWire,
): Promise<ProjectExecutionSetupResponse> {
  return apiFetch<ProjectExecutionSetupResponse>(projectExecutionSetupApiPaths.worker(projectId), {
    method: 'POST',
    body: JSON.stringify(input),
  })
}

export function selectIndependentReviewer(
  projectId: string,
  input: SelectExecutionPrincipalWire,
): Promise<ProjectExecutionSetupResponse> {
  return apiFetch<ProjectExecutionSetupResponse>(
    projectExecutionSetupApiPaths.independentReviewer(projectId),
    {
      method: 'POST',
      body: JSON.stringify(input),
    },
  )
}

export function attachPrimaryRepository(
  projectId: string,
  input: AttachPrimaryRepositoryWire,
): Promise<ProjectExecutionSetupResponse> {
  return apiFetch<ProjectExecutionSetupResponse>(
    projectExecutionSetupApiPaths.primaryRepository(projectId),
    {
      method: 'POST',
      body: JSON.stringify(input),
    },
  )
}

export function retryProvisioning(
  projectId: string,
  input: RetryProvisioningWire,
): Promise<ProjectExecutionSetupResponse> {
  return apiFetch<ProjectExecutionSetupResponse>(
    projectExecutionSetupApiPaths.retryProvisioning(projectId),
    {
      method: 'POST',
      body: JSON.stringify(input),
    },
  )
}

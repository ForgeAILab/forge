// Workflow definition, prompt builder, and template hooks.

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  apiFetch,
  deleteWorkflowTemplate,
  getWorkflowTemplate,
  listWorkflowTemplates,
  saveWorkflowTemplate,
} from '@/api/client'
import { qk } from '@/api/query-keys'
import { invalidateProjectTaskDetails } from '@/api/task-detail-invalidation'
import type {
  SaveWorkflowTemplateRequest,
  UpdateProjectWorkflowRequest,
  WorkflowDefinition,
} from '@/types/generated'

export type PromptBuilderRegistryEntry = {
  id: string
  label: string
  compatible_role_hints: string[]
  description: string
}

/**
 * Execution logs are returned as parsed JSONL entries from the backend.
 */
// --- Workflow ---

export function useWorkflowQuery(projectId: string, options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: qk.workflow(projectId),
    queryFn: ({ signal }) =>
      apiFetch<WorkflowDefinition>(`/projects/${projectId}/workflow`, { signal }),
    enabled: Boolean(projectId) && (options.enabled ?? true),
  })
}

export function useUpdateWorkflow() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ projectId, body }: { projectId: string; body: UpdateProjectWorkflowRequest }) =>
      apiFetch<WorkflowDefinition>(`/projects/${projectId}/workflow`, {
        method: 'PUT',
        body: JSON.stringify(body),
      }),
    onSuccess: (_, vars) => {
      void queryClient.invalidateQueries({ queryKey: qk.workflow(vars.projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.project(vars.projectId) })
      invalidateProjectTaskDetails(queryClient, vars.projectId)
    },
  })
}

export function useWorkflowPromptBuildersQuery() {
  return useQuery({
    queryKey: qk.workflowPromptBuilders,
    queryFn: () => apiFetch<PromptBuilderRegistryEntry[]>('/workflow/prompt-builders'),
  })
}

// --- Workflow Templates ---

export function useWorkflowTemplatesQuery() {
  return useQuery({
    queryKey: qk.workflowTemplates,
    queryFn: () => listWorkflowTemplates(),
  })
}

export function useWorkflowTemplateQuery(name: string) {
  return useQuery({
    queryKey: qk.workflowTemplate(name),
    queryFn: () => getWorkflowTemplate(name),
    enabled: Boolean(name),
  })
}

export function useSaveWorkflowTemplate() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ name, body }: { name: string; body: SaveWorkflowTemplateRequest }) =>
      saveWorkflowTemplate(name, body),
    onSuccess: (saved) => {
      void queryClient.invalidateQueries({ queryKey: qk.workflowTemplates })
      void queryClient.invalidateQueries({ queryKey: qk.workflowTemplate(saved.name) })
    },
  })
}

export function useDeleteWorkflowTemplate() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (name: string) => deleteWorkflowTemplate(name),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.workflowTemplates })
    },
  })
}

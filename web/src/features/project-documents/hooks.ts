import { useQuery } from '@tanstack/react-query'

import { qk } from '@/api/query-keys'

import {
  getProjectDocument,
  getProjectDocumentRevision,
  getProjectDocumentRevisionDiff,
  listProjectDocumentRevisions,
} from './api'

export function useProjectDocumentQuery(projectId: string, documentId: string, enabled = true) {
  return useQuery({
    queryKey: qk.projectDocument(projectId, documentId),
    queryFn: () => getProjectDocument(projectId, documentId),
    enabled: enabled && Boolean(projectId && documentId),
  })
}

export function useProjectDocumentRevisionsQuery(
  projectId: string,
  documentId: string,
  enabled = true,
) {
  return useQuery({
    queryKey: qk.projectDocumentRevisions(projectId, documentId),
    queryFn: () => listProjectDocumentRevisions(projectId, documentId),
    enabled: enabled && Boolean(projectId && documentId),
  })
}

export function useProjectDocumentRevisionQuery(
  projectId: string,
  documentId: string,
  revisionId: string | null,
  enabled = true,
) {
  return useQuery({
    queryKey: qk.projectDocumentRevision(projectId, documentId, revisionId ?? ''),
    queryFn: () => getProjectDocumentRevision(projectId, documentId, revisionId ?? ''),
    enabled: enabled && Boolean(projectId && documentId && revisionId),
  })
}

export function useProjectDocumentRevisionDiffQuery(
  projectId: string,
  documentId: string,
  revisionId: string | null,
  baseRevisionId: string | null,
  enabled = true,
) {
  return useQuery({
    queryKey: qk.projectDocumentRevisionDiff(projectId, documentId, revisionId ?? '', baseRevisionId),
    queryFn: () => getProjectDocumentRevisionDiff(projectId, documentId, revisionId ?? '', baseRevisionId),
    enabled: enabled && Boolean(projectId && documentId && revisionId),
  })
}

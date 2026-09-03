import { apiFetch } from '@/api/client'
import type {
  ProjectDocument,
  ProjectDocumentRevision,
  ProjectDocumentRevisionDiffResponse,
  ProjectDocumentRevisionListResponse,
} from '@/types/generated'

/** The server clamps revision pages to 100 rows; ask for the whole bound. */
const REVISION_PAGE_LIMIT = 100

export function getProjectDocument(projectId: string, documentId: string): Promise<ProjectDocument> {
  return apiFetch<ProjectDocument>(`/projects/${projectId}/documents/${documentId}`)
}

export function listProjectDocumentRevisions(
  projectId: string,
  documentId: string,
): Promise<ProjectDocumentRevisionListResponse> {
  return apiFetch<ProjectDocumentRevisionListResponse>(
    `/projects/${projectId}/documents/${documentId}/revisions`,
    { search: { limit: REVISION_PAGE_LIMIT } },
  )
}

export function getProjectDocumentRevision(
  projectId: string,
  documentId: string,
  revisionId: string,
): Promise<ProjectDocumentRevision> {
  return apiFetch<ProjectDocumentRevision>(
    `/projects/${projectId}/documents/${documentId}/revisions/${revisionId}`,
  )
}

/**
 * The deterministic line diff of one revision's rendered view. Without a
 * base the server diffs against the revision's own base revision, or shows
 * the whole view as additions when it has none.
 */
export function getProjectDocumentRevisionDiff(
  projectId: string,
  documentId: string,
  revisionId: string,
  baseRevisionId?: string | null,
): Promise<ProjectDocumentRevisionDiffResponse> {
  return apiFetch<ProjectDocumentRevisionDiffResponse>(
    `/projects/${projectId}/documents/${documentId}/revisions/${revisionId}/diff`,
    { search: { base_revision_id: baseRevisionId ?? undefined } },
  )
}

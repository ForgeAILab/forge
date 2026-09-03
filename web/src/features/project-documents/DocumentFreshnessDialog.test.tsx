import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { DocumentFreshness, ProjectDocumentRevision } from '@/types/generated'

import { DocumentFreshnessDialog } from './DocumentFreshnessDialog'

const mocks = vi.hoisted(() => ({
  getProjectDocument: vi.fn(),
  listProjectDocumentRevisions: vi.fn(),
  getProjectDocumentRevision: vi.fn(),
  getProjectDocumentRevisionDiff: vi.fn(),
}))

vi.mock('./api', () => ({
  getProjectDocument: mocks.getProjectDocument,
  listProjectDocumentRevisions: mocks.listProjectDocumentRevisions,
  getProjectDocumentRevision: mocks.getProjectDocumentRevision,
  getProjectDocumentRevisionDiff: mocks.getProjectDocumentRevisionDiff,
}))

const document: DocumentFreshness = {
  document_id: 'doc-1',
  kind: 'product_spec',
  approved_revision_id: 'rev-approved',
  approved_digest: 'digest-approved-0123456789',
  working_revision_id: 'rev-working',
  working_digest: 'digest-working-0123456789',
  working_lifecycle: 'proposed',
  status: 'changes_pending',
  reason: 'A working revision is newer than the approved Project truth and awaits approval.',
}

function revision(
  id: string,
  revisionNumber: number,
  lifecycle: ProjectDocumentRevision['lifecycle'],
  renderedView: string,
): ProjectDocumentRevision {
  return {
    id,
    document_id: 'doc-1',
    project_id: 'project-1',
    revision_number: BigInt(revisionNumber),
    base_revision_id: null,
    lifecycle,
    schema_version: '1',
    content: { kind: 'ProductSpec', content: {} as never },
    rendered_view: renderedView,
    render_version: '1',
    content_digest: `digest-${id}-0123456789`,
    render_digest: `render-${id}`,
    provenance: {
      author: { kind: 'agent', id: 'agent-1', display_name: 'Sol' },
      profile_revision: null,
      operating_skill_revision: null,
      source_refs: [],
      change_summary: `Change summary for ${id}`,
      material_diff: null,
    },
    approved_at: lifecycle === 'approved' ? '2026-09-01T10:00:00Z' : null,
    superseded_by_revision_id: null,
    created_at: '2026-09-01T10:00:00Z',
  }
}

const approvedRevision = revision('rev-approved', 1, 'approved', '# Spec\n\nApproved body text.')
const workingRevision = revision('rev-working', 2, 'proposed', '# Spec\n\nWorking body text.')

function renderDialog(target: DocumentFreshness = document) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  })
  return render(
    <QueryClientProvider client={queryClient}>
      <DocumentFreshnessDialog
        projectId="project-1"
        document={target}
        open
        onOpenChange={() => undefined}
      />
    </QueryClientProvider>,
  )
}

describe('DocumentFreshnessDialog', () => {
  beforeEach(() => {
    mocks.getProjectDocumentRevisionDiff.mockResolvedValue({
      document_id: 'doc-1',
      base_revision_id: 'rev-approved',
      revision_id: 'rev-working',
      diff: ' # Spec\n \n-Approved body text.\n+Working body text.',
    })
    mocks.getProjectDocumentRevision.mockImplementation(
      async (_projectId: string, _documentId: string, revisionId: string) =>
        revisionId === 'rev-approved' ? approvedRevision : workingRevision,
    )
    mocks.listProjectDocumentRevisions.mockResolvedValue({
      items: [workingRevision, approvedRevision],
      next_cursor: null,
      has_more: false,
    })
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  it('opens on the diff between the working and approved revisions', async () => {
    renderDialog()

    expect(screen.getByRole('heading', { name: 'Product Spec' })).toBeTruthy()
    expect(screen.getByText(document.reason ?? '')).toBeTruthy()

    const diff = await screen.findByLabelText('Revision diff')
    const added = diff.querySelectorAll('[data-diff-line="add"]')
    const removed = diff.querySelectorAll('[data-diff-line="remove"]')
    expect(added).toHaveLength(1)
    expect(added[0].textContent).toBe('+Working body text.')
    expect(removed).toHaveLength(1)
    expect(removed[0].textContent).toBe('-Approved body text.')
    expect(mocks.getProjectDocumentRevisionDiff).toHaveBeenCalledWith(
      'project-1',
      'doc-1',
      'rev-working',
      'rev-approved',
    )
  })

  it('renders the approved revision view when its tab is chosen', async () => {
    renderDialog()
    await screen.findByLabelText('Revision diff')

    fireEvent.click(screen.getByRole('button', { name: 'Approved revision' }))

    await waitFor(() => {
      expect(screen.getByText('Approved body text.')).toBeTruthy()
    })
    expect(screen.queryByText('Working body text.')).toBeNull()
    expect(mocks.getProjectDocumentRevision).toHaveBeenCalledWith(
      'project-1',
      'doc-1',
      'rev-approved',
    )
  })

  it('lists the revision history with lifecycle, author, and summary', async () => {
    renderDialog()
    await screen.findByLabelText('Revision diff')

    fireEvent.click(screen.getByRole('button', { name: 'History' }))

    const history = await screen.findByLabelText('Revision history')
    const rows = history.querySelectorAll('li')
    expect(rows).toHaveLength(2)
    expect(rows[0].textContent).toContain('Revision 2')
    expect(rows[0].textContent).toContain('Proposed')
    expect(rows[0].textContent).toContain('current working')
    expect(rows[0].textContent).toContain('Change summary for rev-working')
    expect(rows[0].textContent).toContain('Sol')
    expect(rows[1].textContent).toContain('Revision 1')
    expect(rows[1].textContent).toContain('current approved')
  })

  it('starts on the approved view when nothing newer is waiting', async () => {
    renderDialog({
      ...document,
      working_revision_id: null,
      working_digest: null,
      working_lifecycle: null,
      status: 'current',
      reason: null,
    })

    await waitFor(() => {
      expect(screen.getByText('Approved body text.')).toBeTruthy()
    })
    expect(mocks.getProjectDocumentRevisionDiff).not.toHaveBeenCalled()
  })
})

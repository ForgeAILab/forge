import { useState } from 'react'
import { CircleNotch, WarningCircle } from '@phosphor-icons/react'

import { ChatMarkdown } from '@/components/chat/chat-markdown'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { humanize, shortId } from '@/features/federation/format'
import { cn } from '@/lib/cn'
import { getApiErrorMessage } from '@/lib/api-error'
import type { DocumentFreshness, ProjectDocumentRevision } from '@/types/generated'

import {
  useProjectDocumentRevisionDiffQuery,
  useProjectDocumentRevisionQuery,
  useProjectDocumentRevisionsQuery,
} from './hooks'

type DialogTab = 'changes' | 'working' | 'approved' | 'history'

function statusClass(status: DocumentFreshness['status']): string {
  switch (status) {
    case 'current':
      return 'border-success/40 bg-success/10 text-success'
    case 'changes_pending':
      return 'border-warning/40 bg-warning/10 text-warning'
    default:
      return 'border-destructive/40 bg-destructive/10 text-destructive'
  }
}

function formatDate(value: string | null | undefined): string {
  if (!value) return 'No date'
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

function numberValue(value: number | bigint): number {
  return typeof value === 'bigint' ? Number(value) : value
}

function authorLabel(revision: ProjectDocumentRevision): string {
  const author = revision.provenance.author
  return author.display_name?.trim() || `${humanize(author.kind)} ${shortId(author.id)}`
}

function InlineStatus({
  loading,
  error,
  children,
}: {
  loading: boolean
  error: unknown
  children: React.ReactNode
}) {
  if (loading) {
    return (
      <p
        className="flex items-center gap-2 py-4 text-xs text-muted-foreground"
        role="status"
        aria-live="polite"
      >
        <CircleNotch size={14} className="animate-spin text-primary" aria-hidden />
        Loading…
      </p>
    )
  }
  if (error) {
    return (
      <p className="flex items-start gap-2 py-4 text-xs text-destructive" role="alert">
        <WarningCircle size={14} className="mt-0.5 shrink-0" aria-hidden />
        {getApiErrorMessage(error, 'This document view could not be loaded.')}
      </p>
    )
  }
  return <>{children}</>
}

/**
 * The server's deterministic line diff: `+` additions, `-` removals, and a
 * leading space for unchanged lines. A revision with no approved base comes
 * back as pure additions, which is exactly what "first draft" means here.
 */
export function DiffView({ diff }: { diff: string }) {
  const lines = diff.length === 0 ? [] : diff.split('\n')
  const changed = lines.some((line) => line.startsWith('+') || line.startsWith('-'))
  if (!changed) {
    return (
      <p className="py-4 text-xs text-muted-foreground">
        The working revision renders identically to the approved revision.
      </p>
    )
  }
  return (
    <pre
      aria-label="Revision diff"
      className="max-h-[60vh] overflow-auto rounded-md border border-border-subtle bg-muted/30 p-3 font-mono text-xs leading-5"
    >
      {lines.map((line, index) => {
        const kind = line.startsWith('+') ? 'add' : line.startsWith('-') ? 'remove' : 'context'
        return (
          <div
            // The diff is positional; a line's index is its identity.
            // eslint-disable-next-line react/no-array-index-key
            key={index}
            data-diff-line={kind}
            className={cn(
              'whitespace-pre-wrap break-words px-1',
              kind === 'add' && 'bg-success/10 text-success',
              kind === 'remove' && 'bg-destructive/10 text-destructive',
              kind === 'context' && 'text-muted-foreground',
            )}
          >
            {line.length === 0 ? ' ' : line}
          </div>
        )
      })}
    </pre>
  )
}

function RenderedRevision({
  projectId,
  documentId,
  revisionId,
  emptyLabel,
}: {
  projectId: string
  documentId: string
  revisionId: string | null
  emptyLabel: string
}) {
  const query = useProjectDocumentRevisionQuery(projectId, documentId, revisionId)
  if (!revisionId) {
    return <p className="py-4 text-xs text-muted-foreground">{emptyLabel}</p>
  }
  return (
    <InlineStatus loading={query.isLoading} error={query.error}>
      {query.data ? (
        <div className="max-h-[60vh] overflow-y-auto rounded-md border border-border-subtle bg-muted/20 px-4 py-3">
          <p className="mb-3 break-all font-mono text-micro text-muted-foreground">
            revision {numberValue(query.data.revision_number)} · {humanize(query.data.lifecycle)} ·
            digest {shortId(query.data.content_digest)}
          </p>
          <div className="prose prose-sm max-w-none break-words dark:prose-invert">
            <ChatMarkdown text={query.data.rendered_view} />
          </div>
        </div>
      ) : null}
    </InlineStatus>
  )
}

function ChangesTab({
  projectId,
  document,
}: {
  projectId: string
  document: DocumentFreshness
}) {
  const query = useProjectDocumentRevisionDiffQuery(
    projectId,
    document.document_id,
    document.working_revision_id,
    document.approved_revision_id,
  )
  if (!document.working_revision_id) {
    return (
      <p className="py-4 text-xs text-muted-foreground">
        There is no working revision; the approved revision is the current Project truth.
      </p>
    )
  }
  return (
    <InlineStatus loading={query.isLoading} error={query.error}>
      {query.data ? (
        <>
          <p className="mb-2 text-xs text-muted-foreground">
            {document.approved_revision_id
              ? `Working revision ${shortId(document.working_revision_id)} compared with approved revision ${shortId(document.approved_revision_id)}.`
              : `Working revision ${shortId(document.working_revision_id)} has no approved base; every line is new.`}
          </p>
          <DiffView diff={query.data.diff} />
        </>
      ) : null}
    </InlineStatus>
  )
}

function HistoryTab({ projectId, document }: { projectId: string; document: DocumentFreshness }) {
  const query = useProjectDocumentRevisionsQuery(projectId, document.document_id)
  return (
    <InlineStatus loading={query.isLoading} error={query.error}>
      {query.data ? (
        query.data.items.length === 0 ? (
          <p className="py-4 text-xs text-muted-foreground">No revisions are recorded yet.</p>
        ) : (
          <ul className="max-h-[60vh] divide-y divide-border-subtle overflow-y-auto" aria-label="Revision history">
            {query.data.items.map((revision) => {
              const isApproved = revision.id === document.approved_revision_id
              const isWorking = revision.id === document.working_revision_id
              return (
                <li key={revision.id} className="min-w-0 py-2.5 first:pt-0 last:pb-0">
                  <div className="flex min-w-0 flex-wrap items-center gap-2">
                    <span className="text-sm font-medium text-foreground">
                      Revision {numberValue(revision.revision_number)}
                    </span>
                    <span
                      className={cn(
                        'inline-flex items-center rounded-full border px-2 py-0.5 font-mono text-micro font-semibold uppercase tracking-[0.08em]',
                        revision.lifecycle === 'approved'
                          ? 'border-success/40 bg-success/10 text-success'
                          : revision.lifecycle === 'rejected' || revision.lifecycle === 'withdrawn'
                            ? 'border-destructive/40 bg-destructive/10 text-destructive'
                            : 'border-border-subtle bg-muted/40 text-muted-foreground',
                      )}
                    >
                      {humanize(revision.lifecycle)}
                    </span>
                    {isApproved ? (
                      <span className="text-micro text-muted-foreground">current approved</span>
                    ) : null}
                    {isWorking ? (
                      <span className="text-micro text-warning">current working</span>
                    ) : null}
                  </div>
                  {revision.provenance.change_summary ? (
                    <p className="mt-1 break-words text-xs leading-5 text-foreground">
                      {revision.provenance.change_summary}
                    </p>
                  ) : null}
                  <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                    {authorLabel(revision)} · {formatDate(revision.created_at)} · digest{' '}
                    {shortId(revision.content_digest)}
                  </p>
                </li>
              )
            })}
          </ul>
        )
      ) : null}
    </InlineStatus>
  )
}

function DialogBody({ projectId, document }: { projectId: string; document: DocumentFreshness }) {
  const hasWorking =
    Boolean(document.working_revision_id) &&
    document.working_revision_id !== document.approved_revision_id
  const initialTab: DialogTab = hasWorking
    ? 'changes'
    : document.approved_revision_id
      ? 'approved'
      : 'history'
  const [tab, setTab] = useState<DialogTab>(initialTab)

  return (
    <>
      <DialogHeader>
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <DialogTitle>{humanize(document.kind)}</DialogTitle>
          <span
            className={cn(
              'inline-flex max-w-full items-center rounded-full border px-2 py-0.5 font-mono text-micro font-semibold uppercase tracking-[0.08em]',
              statusClass(document.status),
            )}
          >
            {humanize(document.status)}
          </span>
        </div>
        <DialogDescription>
          {document.reason ??
            'The approved revision is the current Project truth and nothing newer is waiting.'}
        </DialogDescription>
        <p className="break-all font-mono text-micro text-muted-foreground">
          approved {shortId(document.approved_revision_id)}
          {document.working_revision_id ? ` · working ${shortId(document.working_revision_id)}` : ''}
        </p>
      </DialogHeader>
      <Tabs value={tab} onValueChange={(value) => setTab(value as DialogTab)} className="mt-4">
        <TabsList className="h-auto flex-wrap">
          <TabsTrigger value="changes">Changes</TabsTrigger>
          <TabsTrigger value="working">Working revision</TabsTrigger>
          <TabsTrigger value="approved">Approved revision</TabsTrigger>
          <TabsTrigger value="history">History</TabsTrigger>
        </TabsList>
        <TabsContent value="changes">
          <ChangesTab projectId={projectId} document={document} />
        </TabsContent>
        <TabsContent value="working">
          <RenderedRevision
            projectId={projectId}
            documentId={document.document_id}
            revisionId={document.working_revision_id}
            emptyLabel="There is no working revision for this document."
          />
        </TabsContent>
        <TabsContent value="approved">
          <RenderedRevision
            projectId={projectId}
            documentId={document.document_id}
            revisionId={document.approved_revision_id}
            emptyLabel="No revision of this document has been approved yet."
          />
        </TabsContent>
        <TabsContent value="history">
          <HistoryTab projectId={projectId} document={document} />
        </TabsContent>
      </Tabs>
    </>
  )
}

/**
 * Read-only inspection of one canonical Project Document behind a freshness
 * row: what changed between the working and approved revisions, either
 * revision's rendered view, and the immutable revision history. Approval
 * stays with the typed next-action surface; this only shows the record.
 */
export function DocumentFreshnessDialog({
  projectId,
  document,
  open,
  onOpenChange,
}: {
  projectId: string
  document: DocumentFreshness | null
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  return (
    <Dialog open={open && document !== null} onOpenChange={onOpenChange} ariaLabel="Project Document">
      <DialogContent className="w-[min(100vw-2rem,52rem)] max-w-3xl">
        {document ? (
          <DialogBody key={document.document_id} projectId={projectId} document={document} />
        ) : null}
      </DialogContent>
    </Dialog>
  )
}

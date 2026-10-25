// Project document freshness, decisions, and risks.

import { useState } from 'react'
import { ArrowUpRight, FileText } from '@phosphor-icons/react'
import { DocumentFreshnessDialog } from '@/features/project-documents/DocumentFreshnessDialog'
import { DecisionCandidateCard } from '@/features/project-execution/DecisionCandidateCard'
import type { CharterRisk, DocumentFreshness, ProjectOverview } from '@/types/generated'
import { humanize, shortId } from './utils'
import { StatusLabel, SectionCard, EmptyInline } from './shared'

export function DocumentFreshnessPanel({
  projectId,
  documents,
}: {
  projectId: string
  documents: DocumentFreshness[]
}) {
  // A freshness row is a summary; the record behind it (approved text,
  // working text, the diff between them, and the revision history) opens in
  // place so "changes pending" is something the user can actually read.
  const [selected, setSelected] = useState<DocumentFreshness | null>(null)
  return (
    <SectionCard title="Document freshness" eyebrow="Canonical Project Documents">
      {documents.length === 0 ? (
        <EmptyInline text="No Project Documents are recorded yet. The Project Agent can propose a bounded document when the outcome needs one." />
      ) : (
        <ul className="divide-y divide-border-subtle">
          {documents.map((document) => (
            <li key={document.document_id} className="min-w-0 py-3 first:pt-0 last:pb-0">
              <button
                type="button"
                onClick={() => setSelected(document)}
                aria-label={`Inspect ${humanize(document.kind)} document`}
                className="-mx-1.5 flex w-[calc(100%+0.75rem)] min-w-0 items-start gap-2 rounded-md px-1.5 py-1 text-left transition-colors hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <FileText size={16} className="mt-0.5 shrink-0 text-muted-foreground" aria-hidden />
                <div className="min-w-0 flex-1">
                  <div className="flex min-w-0 flex-wrap items-center gap-2">
                    <p className="break-words text-sm font-medium text-foreground">
                      {humanize(document.kind)}
                    </p>
                    <StatusLabel status={document.status} />
                  </div>
                  {document.approved_revision_id ? (
                    <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                      approved revision {shortId(document.approved_revision_id)} · digest{' '}
                      {shortId(document.approved_digest)}
                    </p>
                  ) : (
                    <p className="mt-1 text-xs text-muted-foreground">No approved revision yet.</p>
                  )}
                  {document.working_revision_id ? (
                    <p className="mt-1 break-all font-mono text-micro text-warning">
                      working {document.working_lifecycle ?? 'revision'}{' '}
                      {shortId(document.working_revision_id)}
                      {document.working_digest
                        ? ` · digest ${shortId(document.working_digest)}`
                        : ''}
                    </p>
                  ) : null}
                  {document.reason ? (
                    <p className="mt-1 break-words text-xs text-warning">{document.reason}</p>
                  ) : null}
                </div>
                <span className="inline-flex shrink-0 items-center gap-1 text-micro font-semibold text-primary">
                  Inspect <ArrowUpRight size={12} aria-hidden />
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
      <DocumentFreshnessDialog
        projectId={projectId}
        document={selected}
        open={selected !== null}
        onOpenChange={(open) => {
          if (!open) setSelected(null)
        }}
      />
    </SectionCard>
  )
}

export function DecisionsAndRisks({
  projectId,
  overview,
}: {
  projectId: string
  overview: ProjectOverview
}) {
  const decisions = overview.decisions
  return (
    <SectionCard title="Decisions & risks" eyebrow="Authority Ledger">
      <div className="space-y-4">
        <div>
          <p className="text-xs font-medium text-muted-foreground">Pending proposals</p>
          <div className="mt-2">
            <DecisionCandidateCard projectId={projectId} candidates={overview.pending_decisions} />
          </div>
        </div>
        <div className="border-t border-border-subtle pt-4">
          <p className="text-xs font-medium text-muted-foreground">Decision log</p>
          {decisions.length === 0 ? (
            <EmptyInline text="No effective decisions are recorded in the current authority ledger." />
          ) : (
            <ul className="mt-2 space-y-2">
              {decisions.map((decision) => {
                // Keep the affected authority references bounded and typed; the full records remain
                // available from their canonical views.
                const affected = [
                  ...decision.affected_artifact_refs.map(
                    (ref) => `artifact ${shortId(ref.artifact_id)}`,
                  ),
                  ...decision.affected_task_ids.map((id) => `task ${shortId(id)}`),
                  ...decision.affected_milestone_ids.map((id) => `milestone ${shortId(id)}`),
                ]
                return (
                  <li
                    key={decision.id}
                    className="min-w-0 rounded-md border border-border-subtle bg-muted/30 px-3 py-2"
                  >
                    <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
                      <p className="break-words text-sm text-foreground">{decision.question}</p>
                      <StatusLabel status={decision.state} />
                    </div>
                    {decision.context ? (
                      <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
                        Context: <span className="text-foreground">{decision.context}</span>
                      </p>
                    ) : null}
                    {decision.options.length > 0 ? (
                      <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
                        Alternatives: {decision.options.join(' · ')}
                      </p>
                    ) : null}
                    <p className="mt-1 break-words text-xs text-muted-foreground">
                      Outcome: <span className="text-foreground">{decision.selected_outcome}</span>
                    </p>
                    <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
                      Rationale: <span className="text-foreground">{decision.rationale}</span>
                    </p>
                    <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                      Principal:{' '}
                      <span className="text-foreground">
                        {decision.decision_maker.display_name ?? decision.decision_maker.id}
                      </span>{' '}
                      · Class: {humanize(decision.decision_class)}
                    </p>
                    <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                      Decision ID {shortId(decision.id)}
                    </p>
                    {affected.length > 0 ? (
                      <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                        Affected: {affected.join(' · ')}
                      </p>
                    ) : null}
                  </li>
                )
              })}
            </ul>
          )}
        </div>
        <div className="border-t border-border-subtle pt-4">
          <p className="text-xs font-medium text-muted-foreground">Charter risks</p>
          {overview.risks.length === 0 ? (
            <EmptyInline text="No active risk is recorded in the current Charter projection." />
          ) : (
            <ul className="mt-2 space-y-3">
              {overview.risks.map((risk) => (
                <RiskRow key={risk.id} risk={risk} />
              ))}
            </ul>
          )}
        </div>
      </div>
    </SectionCard>
  )
}

function RiskRow({ risk }: { risk: CharterRisk }) {
  return (
    <li className="min-w-0 border-l-2 border-warning/50 pl-3">
      <p className="break-words text-sm text-foreground">{risk.description}</p>
      {risk.impact ? (
        <p className="mt-1 break-words text-xs text-muted-foreground">Impact: {risk.impact}</p>
      ) : null}
      {risk.treatment ? (
        <p className="mt-1 break-words text-xs text-muted-foreground">
          Treatment: {risk.treatment}
        </p>
      ) : null}
      <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
        {risk.owner?.display_name ?? risk.owner?.id ?? 'Unassigned'} · {shortId(risk.id)}
      </p>
    </li>
  )
}

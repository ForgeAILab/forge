// Project Overview formatting helpers and review candidates.

import type { EvidenceAvailability } from '@/types/generated'
import type { MilestoneAcceptanceCheckState } from '@/types/generated/bindings/MilestoneAcceptanceCheckState'

type CountValue = number | bigint

export const EVIDENCE_AVAILABILITY_COPY: Record<EvidenceAvailability, string> = {
  available: 'Available proof',
  quarantined: 'Pending review',
  redacted: 'Redacted derivative',
  purged: 'Evidence unavailable',
}

export function count(value: CountValue | undefined): number {
  return typeof value === 'bigint' ? Number(value) : (value ?? 0)
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return 'No date'
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

export function humanize(value: string | null | undefined): string {
  if (!value) return 'Unknown'
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

export function shortId(value: string | null | undefined): string {
  if (!value) return '—'
  return value.length > 16 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value
}

export function numberValue(value: number | bigint | null | undefined): number | null {
  if (value === null || value === undefined) return null
  const number = typeof value === 'bigint' ? Number(value) : value
  return Number.isFinite(number) ? number : null
}

export async function sha256Hex(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value)
  const digest = await crypto.subtle.digest('SHA-256', bytes)
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('')
}

export type ReleaseCandidate = {
  milestoneId: string
  canonicalId: string
  label: string
  snapshotId: string
  digest: string
  expectedMilestoneVersion: number
  readinessExpectedMilestoneVersion: number
}

export type ManualAttestationCandidate = {
  milestoneId: string
  milestoneLabel: string
  definitionRevisionId: string
  charterRevisionId: string
  check: MilestoneAcceptanceCheckState
}

export function formatDuration(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return 'Duration pending'
  const seconds = Math.max(0, Math.round(value))
  const minutes = Math.floor(seconds / 60)
  return `${minutes}:${String(seconds % 60).padStart(2, '0')}`
}

export function statusClass(status: string): string {
  if (['released', 'pass', 'current', 'approved', 'available'].includes(status)) {
    return 'border-success/30 bg-success/10 text-success'
  }
  if (
    [
      'stale',
      'changes_pending',
      'reconciliation_required',
      'superseded',
      'waived',
      'quarantined',
      'ready_for_release',
    ].includes(status)
  ) {
    return 'border-warning/40 bg-warning/10 text-foreground'
  }
  if (
    ['failed', 'fail', 'blocked', 'invalidated', 'purged', 'redacted', 'error'].includes(status)
  ) {
    return 'border-destructive/30 bg-destructive/10 text-destructive'
  }
  return 'border-border-subtle bg-muted text-muted-foreground'
}

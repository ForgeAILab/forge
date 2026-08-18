import { useEffect, useId, useState, type FormEvent } from 'react'
import { ArrowClockwise, ClipboardText, Fingerprint, ShieldCheck } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useContextManifestDiscoveryQuery, useContextManifestQuery } from './hooks'
import type { ContextManifest, ContextManifestLookup, ContextManifestSource } from './types'
import { EmptyPanel, ErrorPanel, LoadingPanel, SectionKicker, StateBadge } from './components'

export type ContextManifestLookupWithId = ContextManifestLookup & { manifest_id: string }

function shortValue(value: string | null | undefined): string {
  if (!value) return 'Not recorded'
  if (value.length <= 18) return value
  return `${value.slice(0, 9)}…${value.slice(-7)}`
}

function MetadataValue({
  label,
  value,
  copyable = false,
}: {
  label: string
  value: string | null | undefined
  copyable?: boolean
}) {
  const [copied, setCopied] = useState(false)
  const normalized = value ?? 'Not recorded'

  async function copyValue() {
    if (!value) return
    try {
      if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(value)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1400)
    } catch {
      setCopied(false)
    }
  }

  return (
    <div className="min-w-0">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="mt-1 flex min-w-0 items-center gap-2">
        <code
          className="min-w-0 flex-1 break-all font-mono text-xs text-foreground"
          title={normalized}
        >
          {shortValue(normalized)}
        </code>
        {copyable && value ? (
          <Button
            type="button"
            variant="ghost"
            size="icon-xs"
            aria-label={`Copy ${label}`}
            onClick={() => void copyValue()}
          >
            <ClipboardText size={13} aria-hidden />
          </Button>
        ) : null}
      </dd>
      {copied ? (
        <span className="text-micro text-success" role="status">
          Copied
        </span>
      ) : null}
    </div>
  )
}

function ManifestSummary({ manifest }: { manifest: ContextManifest }) {
  return (
    <div className="grid gap-4 rounded-lg border border-border-subtle bg-muted/20 p-4 sm:grid-cols-2 lg:grid-cols-3">
      <MetadataValue label="Manifest ID" value={manifest.id} copyable />
      <MetadataValue label="Identity" value={manifest.identity_id} copyable />
      <MetadataValue label="Agent session" value={manifest.agent_session_id} copyable />
      <MetadataValue label="Context scope" value={manifest.context_scope_id} copyable />
      <MetadataValue
        label="Authorized scope"
        value={`${manifest.scope_type}:${manifest.scope_id}`}
      />
      <MetadataValue label="Created" value={manifest.created_at} />
      <MetadataValue label="Policy revision" value={manifest.policy_revision} copyable />
      <MetadataValue label="Domain revision" value={manifest.domain_revision} copyable />
      <MetadataValue label="LCM binding revision" value={manifest.lcm_binding_revision} copyable />
    </div>
  )
}

function FingerprintCard({
  label,
  value,
  emphasis = false,
}: {
  label: string
  value: string | null | undefined
  emphasis?: boolean
}) {
  return (
    <div
      className={`rounded-lg border p-3 ${emphasis ? 'border-ember-border bg-ember-surface' : 'border-border-subtle bg-card'}`}
    >
      <div className="flex items-center gap-2">
        <Fingerprint size={15} className="text-primary" aria-hidden />
        <SectionKicker>{label}</SectionKicker>
      </div>
      <code
        className="mt-2 block break-all font-mono text-xs text-foreground"
        title={value ?? 'Not recorded'}
      >
        {value ?? 'Not recorded'}
      </code>
    </div>
  )
}

function SourceDecision({ source }: { source: ContextManifestSource }) {
  return (
    <li className="rounded-lg border border-border-subtle bg-card p-4">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <span className="rounded bg-muted px-2 py-0.5 font-mono text-micro text-muted-foreground">
            #{String(source.ordinal)}
          </span>
          <span className="font-mono text-micro uppercase tracking-[0.8px] text-muted-foreground">
            {source.source_type}
          </span>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2">
          <StateBadge status={source.disposition} />
          {source.is_stale ? <StateBadge status="stale" label="Stale pointer" /> : null}
        </div>
      </header>
      <dl className="mt-4 grid gap-3 text-xs sm:grid-cols-2">
        <MetadataValue label="Source ID" value={source.source_id} copyable />
        <MetadataValue label="Source revision" value={source.source_revision} copyable />
        {source.current_revision || source.is_stale ? (
          <MetadataValue
            label="Current canonical revision"
            value={source.current_revision ?? "No current pointer"}
            copyable={Boolean(source.current_revision)}
          />
        ) : null}
        <div>
          <dt className="text-muted-foreground">Selection reason</dt>
          <dd className="mt-1 text-foreground">{source.selection_reason}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">Retention priority</dt>
          <dd className="mt-1 font-mono text-foreground">{String(source.retention_priority)}</dd>
        </div>
        <MetadataValue label="Fragment fingerprint" value={source.fragment_fingerprint} copyable />
      </dl>
    </li>
  )
}

export function ContextManifestInspector({
  lookup,
}: {
  lookup: ContextManifestLookupWithId | undefined
}) {
  const query = useContextManifestQuery(lookup)

  if (!lookup)
    return (
      <EmptyPanel
        title="Manifest lookup required"
        description="Provide the manifest, identity, and context-scope identifiers to inspect an authorized projection."
        icon={<Fingerprint size={19} />}
      />
    )
  if (query.isLoading) return <LoadingPanel label="Loading context manifest metadata" />
  if (query.isError)
    return (
      <ErrorPanel
        title="Context manifest unavailable"
        description="The server could not authorize or load this manifest. No source body is requested by this inspector."
        onRetry={() => void query.refetch()}
      />
    )
  if (!query.data)
    return (
      <EmptyPanel
        title="No context manifest returned"
        description="The authorized manifest projection is empty or no longer available."
        icon={<Fingerprint size={19} />}
      />
    )

  const manifest = query.data
  return (
    <section aria-label="Context manifest metadata" className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <SectionKicker>Authorized context manifest</SectionKicker>
          <h3 className="mt-1 text-lg font-semibold text-foreground">Selection and provenance</h3>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            Immutable metadata only. Source bodies and submitted evidence are never rendered.
          </p>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => void query.refetch()}
          disabled={query.isFetching}
        >
          <ArrowClockwise
            size={14}
            className={query.isFetching ? 'animate-spin' : ''}
            aria-hidden
          />
          Refresh
        </Button>
      </div>
      <ManifestSummary manifest={manifest} />
      <div className="grid gap-3 md:grid-cols-2">
        <FingerprintCard
          label="Combined fingerprint"
          value={manifest.combined_fingerprint}
          emphasis
        />
        <FingerprintCard label="Request fingerprint" value={manifest.request_fingerprint} />
        <FingerprintCard
          label="Runtime manifest fingerprint"
          value={manifest.runtime_manifest_fingerprint}
        />
        <FingerprintCard label="Runtime manifest ID" value={manifest.runtime_manifest_id} />
        <FingerprintCard label="Context policy revision" value={manifest.policy_revision} />
        <FingerprintCard label="Domain revision" value={manifest.domain_revision} />
      </div>
      <div className="rounded-lg border border-ember-border bg-ember-surface px-4 py-3">
        <div className="flex items-start gap-3">
          <ShieldCheck size={17} className="mt-0.5 shrink-0 text-primary" aria-hidden />
          <p className="text-xs leading-5 text-foreground">
            <span className="font-semibold">Redaction boundary.</span> This view exposes source IDs,
            revisions, selection reasons, dispositions, and fingerprints only. It does not fetch or
            display source fragments.
          </p>
        </div>
      </div>
      <div>
        <div className="flex items-center justify-between gap-3">
          <div>
            <SectionKicker>Source decisions</SectionKicker>
            <p className="mt-1 text-xs text-muted-foreground">
              {manifest.sources.length} immutable{' '}
              {manifest.sources.length === 1 ? 'source' : 'sources'}
            </p>
          </div>
        </div>
        {manifest.sources.length === 0 ? (
          <div className="mt-3">
            <EmptyPanel
              title="No source decisions"
              description="This manifest has no recorded source selections."
              icon={<Fingerprint size={19} />}
            />
          </div>
        ) : (
          <ol aria-label="Context manifest source decisions" className="mt-3 space-y-3">
            {manifest.sources.map((source) => (
              <SourceDecision key={`${source.ordinal}:${source.source_id}`} source={source} />
            ))}
          </ol>
        )}
      </div>
    </section>
  )
}

export function ContextManifestDialog({
  initialManifestId,
  initialIdentityId,
  initialContextScopeId,
  label = 'Inspect context manifest',
  contextHint,
  compact = false,
}: {
  initialManifestId?: string
  initialIdentityId?: string
  initialContextScopeId?: string
  label?: string
  contextHint?: string
  /** Icon-only trigger for dense surfaces like chat message footers. */
  compact?: boolean
}) {
  const dialogId = useId().replaceAll(':', '')
  const fieldIds = {
    manifest: `${dialogId}-manifest-id`,
    identity: `${dialogId}-manifest-identity-id`,
    contextScope: `${dialogId}-manifest-context-scope-id`,
  }
  const [open, setOpen] = useState(false)
  const [manifestId, setManifestId] = useState('')
  const [identityId, setIdentityId] = useState(initialIdentityId ?? '')
  const [contextScopeId, setContextScopeId] = useState(initialContextScopeId ?? '')
  const [discoveryLookup, setDiscoveryLookup] = useState<ContextManifestLookup>()
  const [selectedManifestId, setSelectedManifestId] = useState<string>()
  const [error, setError] = useState<string | null>(null)
  const discoveryQuery = useContextManifestDiscoveryQuery(discoveryLookup)
  const discoveredManifests = discoveryQuery.data ?? []

  useEffect(() => {
    if (!open) return
    setManifestId(initialManifestId ?? '')
    setIdentityId(initialIdentityId ?? '')
    setContextScopeId(initialContextScopeId ?? '')
    setDiscoveryLookup(undefined)
    setSelectedManifestId(undefined)
    setError(null)
  }, [initialContextScopeId, initialIdentityId, initialManifestId, open])

  useEffect(() => {
    if (!selectedManifestId && discoveredManifests.length === 1) {
      setSelectedManifestId(discoveredManifests[0]?.id)
    }
  }, [discoveredManifests, selectedManifestId])

  function submit(event: FormEvent) {
    event.preventDefault()
    const nextIdentityId = identityId.trim()
    const nextContextScopeId = contextScopeId.trim()
    const nextManifestId = manifestId.trim()
    if (!nextIdentityId || !nextContextScopeId) {
      setError('Identity ID and context-scope ID are required for authorized discovery.')
      return
    }
    setError(null)
    setSelectedManifestId(nextManifestId || undefined)
    setDiscoveryLookup(
      nextManifestId
        ? undefined
        : { identity_id: nextIdentityId, context_scope_id: nextContextScopeId },
    )
  }

  const selectedLookup = selectedManifestId
    ? {
        manifest_id: selectedManifestId,
        identity_id: identityId.trim(),
        context_scope_id: contextScopeId.trim(),
      }
    : undefined

  return (
    <>
      {compact ? (
        <button
          type="button"
          onClick={() => setOpen(true)}
          aria-haspopup="dialog"
          aria-label={label}
          title={label}
          className="inline-flex h-6 w-6 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted/60 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <Fingerprint size={13} aria-hidden />
        </button>
      ) : (
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => setOpen(true)}
          aria-haspopup="dialog"
        >
          <Fingerprint size={14} aria-hidden />
          {label}
        </Button>
      )}
      <Dialog open={open} onOpenChange={setOpen} ariaLabel="Inspect context manifest">
        <DialogContent className="max-w-4xl">
          <DialogHeader>
            <SectionKicker>Protected provenance</SectionKicker>
            <DialogTitle className="mt-1">Inspect context manifest</DialogTitle>
            <DialogDescription>
              {contextHint ? `Open from ${contextHint}. ` : ''}Find an authorized manifest by
              identity and context scope, then inspect selection metadata without exposing source
              bodies.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={submit} className="mt-5 space-y-4">
            <div className="grid gap-3 md:grid-cols-3">
              <div className="space-y-2 md:col-span-2">
                <Label htmlFor={fieldIds.manifest}>Manifest ID (optional)</Label>
                <Input
                  id={fieldIds.manifest}
                  value={manifestId}
                  onChange={(event) => setManifestId(event.target.value)}
                  placeholder="Use only when a manifest ID is already known"
                  autoComplete="off"
                />
                <p className="text-micro text-muted-foreground">
                  Leave blank to discover authorized manifests from the server.
                </p>
              </div>
              <div className="space-y-2">
                <Label htmlFor={fieldIds.identity}>Identity ID</Label>
                <Input
                  id={fieldIds.identity}
                  value={identityId}
                  onChange={(event) => setIdentityId(event.target.value)}
                  placeholder="Owned identity UUID"
                  autoComplete="off"
                />
              </div>
              <div className="space-y-2 md:col-span-3">
                <Label htmlFor={fieldIds.contextScope}>Context-scope ID</Label>
                <Input
                  id={fieldIds.contextScope}
                  value={contextScopeId}
                  onChange={(event) => setContextScopeId(event.target.value)}
                  placeholder="Context scope UUID"
                  autoComplete="off"
                />
              </div>
            </div>
            {error ? (
              <p className="text-xs text-destructive" role="alert">
                {error}
              </p>
            ) : null}
            <DialogFooter className="gap-2">
              <Button type="button" variant="ghost" onClick={() => setOpen(false)}>
                Close
              </Button>
              <Button type="submit">
                <Fingerprint size={14} aria-hidden />
                {manifestId.trim() ? 'Inspect metadata' : 'Find authorized manifests'}
              </Button>
            </DialogFooter>
          </form>
          <div className="mt-6 border-t border-border-subtle pt-6">
            {discoveryLookup ? (
              <div className="space-y-5">
                <div>
                  <SectionKicker>Authorized manifests</SectionKicker>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Only server-returned manifest IDs can be selected.
                  </p>
                </div>
                {discoveryQuery.isLoading ? (
                  <LoadingPanel label="Discovering authorized manifests" />
                ) : null}
                {discoveryQuery.isError ? (
                  <ErrorPanel
                    title="Manifest discovery unavailable"
                    description="The server could not list authorized manifests for this identity and context scope."
                    onRetry={() => void discoveryQuery.refetch()}
                  />
                ) : null}
                {!discoveryQuery.isLoading &&
                !discoveryQuery.isError &&
                discoveredManifests.length === 0 ? (
                  <EmptyPanel
                    title="No authorized manifests"
                    description="No immutable context manifest is recorded for this identity and context scope."
                    icon={<Fingerprint size={19} />}
                  />
                ) : null}
                {discoveredManifests.length > 0 ? (
                  <div className="grid gap-2" role="list" aria-label="Authorized context manifests">
                    {discoveredManifests.map((manifest) => (
                      <button
                        key={manifest.id}
                        type="button"
                        aria-pressed={manifest.id === selectedManifestId}
                        className={`flex items-start justify-between gap-3 rounded-lg border px-3 py-3 text-left transition-colors hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${manifest.id === selectedManifestId ? 'border-ember-border bg-ember-surface' : 'border-border-subtle bg-card'}`}
                        onClick={() => setSelectedManifestId(manifest.id)}
                      >
                        <span className="min-w-0">
                          <span className="block truncate font-mono text-xs text-foreground">
                            {manifest.id}
                          </span>
                          <span className="mt-1 block text-xs text-muted-foreground">
                            {manifest.sources.length} sources · {manifest.created_at}
                          </span>
                        </span>
                        <span className="font-mono text-micro uppercase text-primary">
                          {manifest.id === selectedManifestId ? 'Selected' : 'Inspect'}
                        </span>
                      </button>
                    ))}
                  </div>
                ) : null}
                {selectedLookup ? <ContextManifestInspector lookup={selectedLookup} /> : null}
              </div>
            ) : selectedLookup ? (
              <ContextManifestInspector lookup={selectedLookup} />
            ) : (
              <EmptyPanel
                title="Awaiting scope identifiers"
                description="The inspector will discover server-authorized manifests after identity and context-scope IDs are provided."
                icon={<Fingerprint size={19} />}
              />
            )}
          </div>
        </DialogContent>
      </Dialog>
    </>
  )
}

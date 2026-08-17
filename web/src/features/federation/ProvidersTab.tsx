import { useCallback, useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  ArrowClockwise,
  ArrowUpRight,
  CaretRight,
  CheckCircle,
  CircleNotch,
  Copy,
  Key,
  ShieldCheck,
  TerminalWindow,
  WarningCircle,
} from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
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
import {
  federationQueryKeys,
  useAgentProviderCapabilitiesQuery,
  useCancelProviderAuthorizationMutation,
  useCreateProviderEntryMutation,
  useProviderAuthorizationQuery,
  useProviderUsageQuery,
  useRemoveProviderEntryMutation,
  useRenameProviderEntryMutation,
  useStartProviderAuthorizationMutation,
  isVersionConflict,
} from '@/features/federation/hooks'
import { testProviderEntry } from '@/features/federation/api'
import type { ProviderUsage } from '@/features/federation/types'
import type {
  AgentProviderCapability,
  CliRuntimeEntryResponse,
  ProviderCredentialMethod,
  ProviderEntryResponse,
  ProviderEntryTestResponse,
} from '@/types/generated'
import {
  EmptyPanel,
  ErrorPanel,
  LoadingPanel,
  SectionKicker,
  StateBadge,
} from '@/features/federation/components'
import { formatResetRelative, humanize, runtimeDisplayNames, shortId, windowLabel } from './format'

/**
 * One-shot live connectivity check for a stored provider entry, rendered as a
 * single quiet status line. The check runs itself when `autoRun` is set; there
 * is deliberately no manual "test again" clutter here — the provider card's
 * refresh button covers manual re-checks.
 */
function ProviderConnectionTest({
  entryId,
  autoRun = false,
}: {
  entryId: string
  autoRun?: boolean
}) {
  const [pending, setPending] = useState(false)
  const [result, setResult] = useState<ProviderEntryTestResponse | null>(null)
  const [failure, setFailure] = useState<string | null>(null)
  const runSeq = useRef(0)
  const autoRanFor = useRef<string | null>(null)

  const runTest = useCallback((id: string) => {
    const seq = (runSeq.current += 1)
    setPending(true)
    setFailure(null)
    testProviderEntry(id)
      .then((response) => {
        if (runSeq.current !== seq) return
        setResult(response)
      })
      .catch((cause: unknown) => {
        if (runSeq.current !== seq) return
        setResult(null)
        setFailure(cause instanceof Error ? cause.message : 'The connection test could not run.')
      })
      .finally(() => {
        if (runSeq.current === seq) setPending(false)
      })
  }, [])

  useEffect(() => {
    if (!autoRun || autoRanFor.current === entryId) return
    autoRanFor.current = entryId
    runTest(entryId)
  }, [autoRun, entryId, runTest])

  return (
    <p className="flex flex-wrap items-center gap-1.5 text-xs" role="status" aria-live="polite">
      {pending ? (
        <>
          <CircleNotch size={14} className="animate-spin text-primary" aria-hidden />
          <span className="text-muted-foreground">Checking the provider connection…</span>
        </>
      ) : result?.status === 'ok' ? (
        <>
          <CheckCircle size={14} className="text-success" aria-hidden />
          <span className="font-medium text-success">
            Provider responding · {result.latency_ms} ms
          </span>
          {result.message ? (
            <span className="text-muted-foreground">· {result.message}</span>
          ) : null}
        </>
      ) : result != null || failure != null ? (
        <>
          <WarningCircle size={14} className="text-destructive" aria-hidden />
          <span className="font-medium text-destructive">
            {result?.message ?? failure ?? 'The connection test failed.'}
          </span>
        </>
      ) : (
        <span className="text-muted-foreground">Forge verifies the stored credential once.</span>
      )}
    </p>
  )
}

function UsageSummary({ usage }: { usage: ProviderUsage }) {
  if (usage.source === 'unknown' || usage.windows.length === 0) {
    return <span className="text-muted-foreground">Usage unknown</span>
  }
  const mostConsumed = usage.windows.reduce(
    (max, window) => (window.used_percent > max.used_percent ? window : max),
    usage.windows[0],
  )
  return (
    <div>
      <p className="font-medium text-foreground">
        {Math.round(mostConsumed.used_percent)}% used · resets{' '}
        {formatResetRelative(mostConsumed.resets_at)}
      </p>
      {usage.windows.length > 1 ? (
        <p className="mt-0.5 text-micro text-muted-foreground">
          {usage.windows
            .map((window) => `${windowLabel(window.window_minutes)} ${Math.round(window.used_percent)}%`)
            .join(' · ')}
        </p>
      ) : null}
    </div>
  )
}

/**
 * OAuth operation runner for the wizard's Connect step; the server owns the
 * PKCE/device state and this panel renders only the public view.
 */
function ProviderAuthorizationPanel({
  capability,
  method,
  onConnected,
  onBack,
  onClose,
}: {
  capability: AgentProviderCapability
  method: ProviderCredentialMethod
  onConnected: (entryId: string | null) => void
  onBack: () => void
  onClose: () => void
}) {
  const [label, setLabel] = useState(`${capability.display_name} login`)
  const [operationId, setOperationId] = useState<string>()
  const [error, setError] = useState<string>()
  const start = useStartProviderAuthorizationMutation()
  const cancel = useCancelProviderAuthorizationMutation()
  const operation = useProviderAuthorizationQuery(operationId)
  const startInFlight = useRef(false)

  const operationState = operation.data?.state
  const operationEntryId = operation.data?.credential_handle_id
  useEffect(() => {
    if (operationState !== 'succeeded') return
    const timeoutId = window.setTimeout(() => onConnected(operationEntryId ?? null), 600)
    return () => window.clearTimeout(timeoutId)
  }, [onConnected, operationState, operationEntryId])

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (startInFlight.current) return
    startInFlight.current = true
    setError(undefined)
    try {
      const started = await start.mutateAsync({
        provider: capability.provider,
        method,
        redirect_origin: window.location.origin,
        credential_label: label.trim(),
        // The browser is on the server's machine whenever Forge is served over
        // loopback, so Forge itself binds the provider's localhost callback.
        // Anywhere else the server rejects browser OAuth and points at the
        // device-code method or `forge-ctl embedded provider login`.
        loopback_owner: 'server',
        loopback_port: null,
      })
      setOperationId(started.id)
      if (method === 'browser_oauth' && started.authorization_url) {
        window.location.assign(started.authorization_url)
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Provider authorization could not start.')
    } finally {
      startInFlight.current = false
    }
  }

  const current = operation.data
  const terminal = current
    ? ['succeeded', 'denied', 'expired', 'cancelled', 'failed'].includes(current.state)
    : false

  return (
    <>
      {!current ? (
        <form onSubmit={submit} className="mt-5 space-y-4">
          <div className="space-y-2">
            <Label htmlFor="oauth-label">Provider entry name</Label>
            <Input
              id="oauth-label"
              value={label}
              onChange={(event) => setLabel(event.target.value)}
              required
            />
          </div>
          {error ? (
            <p role="alert" className="text-xs text-destructive">
              {error}
            </p>
          ) : null}
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={onBack}>
              Back
            </Button>
            <Button type="submit" disabled={start.isPending}>
              {start.isPending ? 'Starting…' : 'Start authorization'}
            </Button>
          </DialogFooter>
        </form>
      ) : (
        <div className="mt-5 space-y-4" aria-live="polite">
          <div className="rounded-lg border border-border-subtle bg-muted/20 p-4">
            <div className="flex items-center justify-between gap-3">
              <SectionKicker>Authorization state</SectionKicker>
              <StateBadge status={current.state} label={humanize(current.state)} />
            </div>
            {current.user_code ? (
              <div className="mt-4">
                <p className="text-xs text-muted-foreground">Enter this code at the provider:</p>
                <button
                  type="button"
                  className="mt-2 flex w-full items-center justify-between rounded-md border border-input bg-card px-3 py-2 font-mono text-lg tracking-[0.16em] text-foreground"
                  onClick={() => void navigator.clipboard.writeText(current.user_code ?? '')}
                >
                  {current.user_code}
                  <Copy size={16} aria-hidden />
                </button>
              </div>
            ) : null}
            {current.authorization_url ? (
              <a
                className="mt-4 inline-flex items-center gap-1.5 text-sm font-medium text-primary hover:underline"
                href={current.authorization_url}
                target="_blank"
                rel="noreferrer"
              >
                Open provider authorization <ArrowUpRight size={14} aria-hidden />
              </a>
            ) : null}
            {current.error_message ? (
              <p className="mt-3 text-xs text-destructive" role="alert">
                {current.error_message}
              </p>
            ) : null}
          </div>
          <DialogFooter>
            {!terminal ? (
              <Button
                variant="outline"
                disabled={cancel.isPending}
                onClick={() =>
                  void cancel.mutateAsync({
                    id: current.id,
                    input: { expected_version: current.version },
                  })
                }
              >
                Cancel authorization
              </Button>
            ) : (
              <Button onClick={onClose}>Done</Button>
            )}
          </DialogFooter>
        </div>
      )}
    </>
  )
}

/** API-key entry form for the wizard's Connect step. */
function ApiKeyEntryForm({
  capability,
  onCreated,
  onBack,
}: {
  capability: AgentProviderCapability
  onCreated: (entry: ProviderEntryResponse) => void
  onBack: () => void
}) {
  const create = useCreateProviderEntryMutation()
  const [label, setLabel] = useState(`${capability.display_name} API key`)
  const [credential, setCredential] = useState('')
  const [baseUrl, setBaseUrl] = useState(capability.default_base_url ?? '')
  const [error, setError] = useState<string>()
  const inFlight = useRef(false)

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (inFlight.current) return
    if (!credential.trim() || !label.trim()) {
      setError('A name and API key are required.')
      return
    }
    inFlight.current = true
    setError(undefined)
    try {
      const entry = await create.mutateAsync({
        provider: capability.provider,
        label: label.trim(),
        credential: credential.trim(),
        base_url: baseUrl.trim() ? baseUrl.trim() : null,
      })
      setCredential('')
      onCreated(entry)
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The provider entry could not be created.')
    } finally {
      inFlight.current = false
    }
  }

  return (
    <form onSubmit={submit} className="mt-5 space-y-4">
      <div className="space-y-2">
        <Label htmlFor="entry-label">Provider entry name</Label>
        <Input
          id="entry-label"
          value={label}
          onChange={(event) => setLabel(event.target.value)}
          required
        />
      </div>
      <div className="space-y-2">
        <Label htmlFor="entry-credential">API key</Label>
        <Input
          id="entry-credential"
          type="password"
          autoComplete="new-password"
          value={credential}
          onChange={(event) => setCredential(event.target.value)}
          required
        />
      </div>
      <div className="space-y-2">
        <Label htmlFor="entry-base-url">API endpoint</Label>
        <Input
          id="entry-base-url"
          type="url"
          value={baseUrl}
          onChange={(event) => setBaseUrl(event.target.value)}
          placeholder={
            capability.provider === 'openai_compatible'
              ? 'https://your-endpoint.example/v1'
              : (capability.default_base_url ?? '')
          }
          required={capability.provider === 'openai_compatible'}
        />
      </div>
      {error ? (
        <p role="alert" className="text-xs text-destructive">
          {error}
        </p>
      ) : null}
      <DialogFooter className="mt-6 gap-2">
        <Button type="button" variant="ghost" onClick={onBack}>
          Back
        </Button>
        <Button type="submit" disabled={create.isPending}>
          <ShieldCheck size={15} aria-hidden />
          {create.isPending ? 'Verifying…' : 'Add provider'}
        </Button>
      </DialogFooter>
    </form>
  )
}

/**
 * Four-step provider setup: choose a provider, choose how to authenticate,
 * connect, then verify the stored entry with a live connection test.
 */
export function AddProviderWizard({
  open,
  onClose,
  onCreateAgent,
}: {
  open: boolean
  onClose: () => void
  onCreateAgent: (entryId: string | null) => void
}) {
  const providers = useAgentProviderCapabilitiesQuery()
  const [capability, setCapability] = useState<AgentProviderCapability | null>(null)
  const [method, setMethod] = useState<ProviderCredentialMethod | null>(null)
  const [connected, setConnected] = useState<{ id: string | null; label: string } | null>(null)

  useEffect(() => {
    if (!open) return
    setCapability(null)
    setMethod(null)
    setConnected(null)
  }, [open])

  const step: 1 | 2 | 3 | 4 = connected ? 4 : method ? 3 : capability ? 2 : 1

  return (
    <Dialog open={open} onOpenChange={(next) => !next && onClose()}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <SectionKicker>
            {capability ? `${capability.display_name} · ` : ''}New provider · step {step} of 4
          </SectionKicker>
          <DialogTitle className="mt-1">
            {step === 1
              ? 'Choose a provider'
              : step === 2
                ? 'Choose how to authenticate'
                : step === 3
                  ? method === 'api_key'
                    ? 'Add an API-key entry'
                    : method === 'device_oauth'
                      ? 'Sign in with a device code'
                      : 'Continue in your browser'
                  : 'Provider connected'}
          </DialogTitle>
          <DialogDescription>
            {step === 1
              ? 'You can add the same provider more than once — for example two OpenAI accounts. Availability comes from the server capability catalog.'
              : step === 2
                ? 'Only the methods the server declares are offered. A guided login never replaces the API-key alternative.'
                : step === 3
                  ? 'A successful connection stores a protected credential and creates a provider entry — it does not create an agent. Secrets never return to this screen.'
                  : 'The credential is stored. Create an agent on this entry whenever you are ready.'}
          </DialogDescription>
        </DialogHeader>

        {step === 1 ? (
          <div className="mt-5 space-y-3">
            {providers.isLoading ? <LoadingPanel label="Loading provider catalog" /> : null}
            {providers.isError ? (
              <ErrorPanel
                title="Provider catalog unavailable"
                description="Forge could not load the authoritative credential-method catalog."
                onRetry={() => void providers.refetch()}
              />
            ) : null}
            <div className="max-h-[55vh] space-y-3 overflow-y-auto">
              {providers.data?.items.map((provider) => (
                <button
                  key={provider.provider}
                  type="button"
                  className="flex w-full items-center justify-between gap-3 rounded-md border border-border-subtle bg-card px-3 py-3 text-left transition-colors hover:border-ember-border"
                  onClick={() => setCapability(provider)}
                >
                  <div className="flex min-w-0 items-center gap-3">
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-ember-surface text-primary">
                      <Key size={17} aria-hidden />
                    </div>
                    <div className="min-w-0">
                      <p className="truncate text-sm font-medium text-foreground">
                        {provider.display_name}
                      </p>
                      <p className="mt-0.5 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
                        {provider.model_discovery ? 'Model discovery' : 'Manual model selection'} ·{' '}
                        {provider.credential_methods.length} login method
                        {provider.credential_methods.length === 1 ? '' : 's'}
                      </p>
                    </div>
                  </div>
                  <CaretRight size={15} className="shrink-0 text-muted-foreground" aria-hidden />
                </button>
              ))}
            </div>
          </div>
        ) : null}

        {step === 2 && capability ? (
          <div className="mt-5 space-y-3">
            {capability.credential_methods.map((credential) => (
              <button
                key={credential.method}
                type="button"
                disabled={!credential.configured}
                className={`w-full rounded-md border px-3 py-3 text-left ${
                  credential.configured
                    ? 'border-border-subtle bg-card transition-colors hover:border-ember-border'
                    : 'cursor-not-allowed border-border-subtle bg-muted/40 opacity-70'
                }`}
                onClick={() => setMethod(credential.method)}
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="flex min-w-0 flex-wrap items-center gap-2">
                    <p className="text-sm font-medium text-foreground">
                      {credential.action_label}
                    </p>
                    <StateBadge
                      status={credential.support_level}
                      label={humanize(credential.support_level)}
                    />
                  </div>
                  {credential.configured ? (
                    <CaretRight size={15} className="shrink-0 text-muted-foreground" aria-hidden />
                  ) : null}
                </div>
                {credential.boundary_note ? (
                  <p className="mt-1.5 text-micro leading-5 text-muted-foreground">
                    {credential.boundary_note}
                  </p>
                ) : null}
                {credential.setup_guidance ? (
                  <p className="mt-1.5 text-micro leading-5 text-warning">
                    {credential.setup_guidance}
                  </p>
                ) : null}
              </button>
            ))}
            <DialogFooter>
              <Button type="button" variant="ghost" onClick={() => setCapability(null)}>
                Back
              </Button>
            </DialogFooter>
          </div>
        ) : null}

        {step === 3 && capability && method ? (
          method === 'api_key' ? (
            <ApiKeyEntryForm
              capability={capability}
              onBack={() => setMethod(null)}
              onCreated={(entry) => setConnected({ id: entry.id, label: entry.label })}
            />
          ) : (
            <ProviderAuthorizationPanel
              key={`${capability.provider}:${method}`}
              capability={capability}
              method={method}
              onBack={() => setMethod(null)}
              onClose={onClose}
              onConnected={(entryId) =>
                setConnected({ id: entryId, label: capability.display_name })
              }
            />
          )
        ) : null}

        {step === 4 && connected ? (
          <div className="mt-5 space-y-4">
            <p
              className="flex items-start gap-2 rounded-md border border-success/30 bg-success/10 px-3 py-2.5 text-sm text-foreground"
              role="status"
            >
              <CheckCircle size={16} className="mt-0.5 shrink-0 text-success" aria-hidden />
              <span>
                <strong>{connected.label}</strong> is connected. No agent was created.
              </span>
            </p>
            {connected.id ? (
              <ProviderConnectionTest entryId={connected.id} autoRun />
            ) : (
              <p className="text-xs text-muted-foreground">
                The entry is stored on the server and appears on its card in the Providers tab.
              </p>
            )}
            <DialogFooter className="gap-2">
              <Button type="button" variant="ghost" onClick={onClose}>
                Done
              </Button>
              <Button type="button" onClick={() => onCreateAgent(connected.id)}>
                Create an agent with this provider
              </Button>
            </DialogFooter>
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  )
}

function ProviderEntryCard({
  entry,
  onShowAgents,
}: {
  entry: ProviderEntryResponse
  onShowAgents: () => void
}) {
  const rename = useRenameProviderEntryMutation()
  const remove = useRemoveProviderEntryMutation()
  const queryClient = useQueryClient()
  const usageQuery = useProviderUsageQuery(entry.status === 'configured' ? entry.id : undefined)
  const [renaming, setRenaming] = useState(false)
  const [confirmingRemoval, setConfirmingRemoval] = useState(false)
  const [label, setLabel] = useState(entry.label)
  const [error, setError] = useState<string>()
  const [notice, setNotice] = useState<string>()

  const statusBadge =
    entry.status === 'configured'
      ? { status: 'active', label: 'Connected' }
      : entry.status === 'revoked'
        ? { status: 'error', label: 'Revoked' }
        : entry.status === 'invalid'
          ? { status: 'error', label: 'Error' }
          : { status: entry.status, label: humanize(entry.status) }

  /** Refresh the entry's status and usage in place from the server. */
  function refreshStatus() {
    void queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
    if (entry.status === 'configured') void usageQuery.refetch()
  }

  async function submitRename(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(undefined)
    try {
      await rename.mutateAsync({
        id: entry.id,
        input: { label: label.trim(), version: entry.version },
      })
      setRenaming(false)
    } catch (cause) {
      setError(
        isVersionConflict(cause)
          ? 'This entry changed in another session. Refresh before renaming.'
          : cause instanceof Error
            ? cause.message
            : 'Rename failed.',
      )
    }
  }

  async function disconnect() {
    setError(undefined)
    setNotice(undefined)
    try {
      const result = await remove.mutateAsync({ handleId: entry.id, version: entry.version })
      setConfirmingRemoval(false)
      setNotice(
        result.provider_revocation === 'failed'
          ? 'Disconnected locally. Provider-side revocation could not be confirmed; revoke Forge in the provider account as a follow-up.'
          : 'Provider entry disconnected. Referencing agents are now marked unhealthy.',
      )
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The entry could not be disconnected.')
    }
  }

  return (
    <Card className="flex flex-col p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="truncate text-sm font-semibold text-foreground">{entry.label}</h3>
            <StateBadge status={statusBadge.status} label={statusBadge.label} />
          </div>
          <p className="mt-1 truncate text-xs text-muted-foreground">{humanize(entry.provider)}</p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label="Refresh provider status"
            title="Refresh provider status"
            onClick={refreshStatus}
          >
            <ArrowClockwise
              size={14}
              className={usageQuery.isFetching ? 'animate-spin' : ''}
              aria-hidden
            />
          </Button>
          <Key size={17} className="text-primary" aria-hidden />
        </div>
      </div>
      <dl className="mt-3 space-y-1.5 text-xs">
        <div className="flex justify-between gap-3">
          <dt className="text-muted-foreground">Method</dt>
          <dd className="text-foreground">
            {entry.credential_method === 'oauth_bundle' ? 'OAuth login' : 'API key'}
          </dd>
        </div>
        {entry.provider_account_id ? (
          <div className="flex justify-between gap-3">
            <dt className="text-muted-foreground">Account</dt>
            <dd className="truncate font-mono text-foreground">
              {shortId(entry.provider_account_id)}
            </dd>
          </div>
        ) : null}
        <div className="flex justify-between gap-3">
          <dt className="text-muted-foreground">Last used</dt>
          <dd className="text-foreground">
            {entry.last_used_at ? new Date(entry.last_used_at).toLocaleString() : 'Never'}
          </dd>
        </div>
      </dl>
      {entry.status === 'configured' ? (
        <div className="mt-3 rounded-md border border-border-subtle bg-muted/20 px-3 py-2 text-xs">
          {usageQuery.isLoading ? (
            <span className="text-muted-foreground">Checking usage…</span>
          ) : usageQuery.isError ? (
            <span className="text-muted-foreground">Usage unavailable</span>
          ) : usageQuery.data ? (
            <UsageSummary usage={usageQuery.data} />
          ) : null}
        </div>
      ) : null}
      <button
        type="button"
        className="mt-3 inline-flex items-center gap-1.5 text-left text-xs font-medium text-primary hover:underline"
        onClick={onShowAgents}
      >
        Used by {entry.used_by.length} agent{entry.used_by.length === 1 ? '' : 's'}
        <ArrowUpRight size={13} aria-hidden />
      </button>
      {renaming ? (
        <form onSubmit={submitRename} className="mt-3 flex items-center gap-2">
          <Input
            aria-label="New entry name"
            value={label}
            onChange={(event) => setLabel(event.target.value)}
          />
          <Button type="submit" size="sm" disabled={rename.isPending}>
            Save
          </Button>
          <Button type="button" size="sm" variant="ghost" onClick={() => setRenaming(false)}>
            Cancel
          </Button>
        </form>
      ) : null}
      {confirmingRemoval ? (
        <div
          className="mt-3 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning"
          role="alertdialog"
          aria-label={`Confirm disconnecting ${entry.label}`}
        >
          {entry.used_by.length > 0 ? (
            <p>
              {entry.used_by.length} agent{entry.used_by.length === 1 ? '' : 's'} reference this
              entry ({entry.used_by.map((agent) => agent.agent_name).join(', ')}). They will become
              unhealthy and are never silently rebound.
            </p>
          ) : (
            <p>No agents reference this entry.</p>
          )}
          <div className="mt-2 flex gap-2">
            <Button size="sm" variant="destructive" disabled={remove.isPending} onClick={() => void disconnect()}>
              Disconnect
            </Button>
            <Button size="sm" variant="ghost" onClick={() => setConfirmingRemoval(false)}>
              Keep
            </Button>
          </div>
        </div>
      ) : null}
      {error ? (
        <p role="alert" className="mt-2 text-xs text-destructive">
          {error}
        </p>
      ) : null}
      {notice ? (
        <p role="status" className="mt-2 text-xs text-muted-foreground">
          {notice}
        </p>
      ) : null}
      {entry.status !== 'revoked' && !confirmingRemoval ? (
        <div className="mt-4 flex gap-2 border-t border-border-subtle pt-3">
          {!renaming ? (
            <Button size="sm" variant="outline" onClick={() => setRenaming(true)}>
              Rename
            </Button>
          ) : null}
          <Button
            size="sm"
            variant="outline"
            className="text-destructive hover:bg-destructive/10 hover:border-destructive/30"
            onClick={() => setConfirmingRemoval(true)}
          >
            Disconnect
          </Button>
        </div>
      ) : null}
    </Card>
  )
}

function CliRuntimeCard({ runtime }: { runtime: CliRuntimeEntryResponse }) {
  const authenticated = runtime.availability === 'authenticated'
  const badgeLabel = authenticated
    ? 'Authenticated'
    : runtime.availability === 'unauthenticated'
      ? 'Not Logged In'
      : 'Unavailable'
  return (
    <Card className="flex flex-col p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="truncate text-sm font-semibold text-foreground">
              {runtimeDisplayNames[runtime.kind] ?? humanize(runtime.kind)}
            </h3>
            {runtime.version ? (
              <span className="font-mono text-xs text-muted-foreground">v{runtime.version}</span>
            ) : null}
            <StateBadge
              status={authenticated ? 'healthy' : 'unavailable'}
              label={badgeLabel}
            />
          </div>
          <p className="mt-1 truncate text-xs text-muted-foreground">
            {runtime.daemon_hostname ?? runtime.daemon_id}
          </p>
        </div>
        <TerminalWindow size={17} className="shrink-0 text-primary" aria-hidden />
      </div>
      <p className="mt-3 text-xs text-muted-foreground">
        Used by {runtime.used_by.length} agent{runtime.used_by.length === 1 ? '' : 's'}
        {runtime.used_by.length > 0
          ? `: ${runtime.used_by.map((agent) => agent.agent_name).join(', ')}`
          : ''}
      </p>
      {!authenticated && runtime.login_hint ? (
        <p className="mt-2 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning">
          {runtime.login_hint}. Forge never reads the CLI&apos;s credential files.
        </p>
      ) : null}
    </Card>
  )
}

/** Providers tab panel: connected provider entries + CLI-managed runtimes. */
export function ProvidersTab({
  entries,
  cliRuntimes,
  isLoading,
  isError,
  onRetry,
  routeSearch,
  onShowAgents,
  onCreateAgentWithProvider,
}: {
  entries: ProviderEntryResponse[]
  cliRuntimes: CliRuntimeEntryResponse[]
  isLoading: boolean
  isError: boolean
  onRetry: () => void
  routeSearch: { status?: string; provider?: string }
  onShowAgents: (provider: string) => void
  onCreateAgentWithProvider: () => void
}) {
  return (
    <div
      role="tabpanel"
      id="agent-settings-panel-providers"
      aria-labelledby="agent-settings-tab-providers"
      className="space-y-6"
    >
      {routeSearch.status ? (
        <div
          className="rounded-lg border border-ember-border bg-ember-surface px-4 py-3 text-sm text-foreground"
          role="status"
        >
          {routeSearch.provider ? humanize(routeSearch.provider) : 'Provider'} authorization{' '}
          <strong>{humanize(routeSearch.status)}</strong>.
          {routeSearch.status === 'succeeded' ? (
            <Button size="sm" variant="outline" className="ml-3" onClick={onCreateAgentWithProvider}>
              Create an agent with this provider
            </Button>
          ) : null}
        </div>
      ) : null}
      {isLoading ? <LoadingPanel label="Loading provider entries" /> : null}
      {isError ? (
        <ErrorPanel
          title="Provider entries unavailable"
          description="Forge could not load the provider entry projection."
          onRetry={onRetry}
        />
      ) : null}
      {!isLoading && !isError && entries.length === 0 ? (
        <EmptyPanel
          title="No providers connected"
          description="Add a provider to store its credential once, then create as many agents on it as you need."
          icon={<Key size={19} />}
        />
      ) : null}
      {entries.length > 0 ? (
        <section aria-labelledby="provider-entries-heading" className="space-y-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <SectionKicker>Connected providers</SectionKicker>
              <h2 id="provider-entries-heading" className="mt-1 text-lg font-semibold text-foreground">
                Provider entries
              </h2>
              <p className="mt-1 max-w-2xl text-sm leading-6 text-muted-foreground">
                Each entry is one credentialed connection. Add the same provider again for another
                account or key.
              </p>
            </div>
            <span className="inline-flex shrink-0 items-center gap-1.5 rounded-full border border-border-subtle bg-muted px-3 py-1 font-mono text-micro uppercase tracking-[0.8px] text-muted-foreground">
              <Key size={13} aria-hidden />
              Protected credentials only
            </span>
          </div>
          <div className="grid gap-3 lg:grid-cols-2 xl:grid-cols-3">
            {entries.map((entry) => (
              <ProviderEntryCard
                key={entry.id}
                entry={entry}
                onShowAgents={() => onShowAgents(entry.provider)}
              />
            ))}
          </div>
        </section>
      ) : null}
      {cliRuntimes.length > 0 ? (
        <section aria-labelledby="cli-runtimes-heading" className="space-y-3">
          <div>
            <SectionKicker>CLI runtimes</SectionKicker>
            <h2 id="cli-runtimes-heading" className="mt-1 text-lg font-semibold text-foreground">
              CLI-managed logins
            </h2>
            <p className="mt-1 max-w-2xl text-sm leading-6 text-muted-foreground">
              Harnesses discovered on connected runtimes that manage their own authentication. Forge
              reads availability only.
            </p>
          </div>
          <div className="grid gap-3 lg:grid-cols-2 xl:grid-cols-3">
            {cliRuntimes.map((runtime) => (
              <CliRuntimeCard key={`${runtime.daemon_id}:${runtime.kind}`} runtime={runtime} />
            ))}
          </div>
        </section>
      ) : null}
    </div>
  )
}

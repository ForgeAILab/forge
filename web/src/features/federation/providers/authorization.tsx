// Provider authorization and API key entry controls.

import { useEffect, useRef, useState } from 'react'
import { ArrowUpRight, Copy, ShieldCheck } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { DialogFooter } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  useCancelProviderAuthorizationMutation,
  useCreateProviderEntryMutation,
  useProviderAuthorizationQuery,
  useStartProviderAuthorizationMutation,
} from '@/features/federation/hooks'
import type {
  AgentProviderCapability,
  ProviderCredentialMethod,
  ProviderEntryResponse,
} from '@/types/generated'
import { SectionKicker, StateBadge } from '@/features/federation/components'
import { humanize } from '../format'

/**
 * OAuth operation runner for the wizard's Connect step; the server owns the
 * PKCE/device state and this panel renders only the public view.
 */
export function ProviderAuthorizationPanel({
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
export function ApiKeyEntryForm({
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

// Provider entry creation wizard.

import { useEffect, useState } from 'react'
import { CaretRight, CheckCircle, Key } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { useAgentProviderCapabilitiesQuery } from '@/features/federation/hooks'
import type { AgentProviderCapability, ProviderCredentialMethod } from '@/types/generated'
import {
  ErrorPanel,
  LoadingPanel,
  SectionKicker,
  StateBadge,
} from '@/features/federation/components'
import { humanize } from '../format'
import { ApiKeyEntryForm, ProviderAuthorizationPanel } from './authorization'
import { ProviderConnectionTest } from './connection'

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
                    <p className="text-sm font-medium text-foreground">{credential.action_label}</p>
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

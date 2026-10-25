import { Key } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import type { CliRuntimeEntryResponse, ProviderEntryResponse } from '@/types/generated'
import {
  EmptyPanel,
  ErrorPanel,
  LoadingPanel,
  SectionKicker,
} from '@/features/federation/components'
import { humanize } from './format'
import { PricingCatalogStatusPanel } from './providers/pricing-catalog'
import { ProviderEntryCard } from './providers/provider-entry-card'
import { CliRuntimeCard } from './providers/cli-runtime-card'

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
            <Button
              size="sm"
              variant="outline"
              className="ml-3"
              onClick={onCreateAgentWithProvider}
            >
              Create an agent with this provider
            </Button>
          ) : null}
        </div>
      ) : null}
      <PricingCatalogStatusPanel />
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
              <h2
                id="provider-entries-heading"
                className="mt-1 text-lg font-semibold text-foreground"
              >
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

export { PricingCatalogStatusPanel } from './providers/pricing-catalog'
export { AddProviderWizard } from './providers/add-provider-wizard'

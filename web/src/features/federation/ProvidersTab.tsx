import { useCallback, useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  ArrowClockwise,
  ArrowUpRight,
  CaretRight,
  CheckCircle,
  CircleNotch,
  Copy,
  FloppyDisk,
  Key,
  MagnifyingGlass,
  PencilSimple,
  Plus,
  ShieldCheck,
  TerminalWindow,
  Trash,
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
  useCliRuntimePricingQuery,
  useCreateProviderEntryMutation,
  useProviderAuthorizationQuery,
  usePricingCatalogModelsQuery,
  usePricingCatalogStatusQuery,
  useProviderPricingQuery,
  useProviderUsageQuery,
  useRefreshPricingCatalogMutation,
  useRemoveProviderEntryMutation,
  useReplaceCliRuntimePricingMutation,
  useReplaceProviderPricingMutation,
  useRenameProviderEntryMutation,
  useSetCliRuntimeAvailabilityMutation,
  useSetProviderEntryAvailabilityMutation,
  useStartProviderAuthorizationMutation,
  isVersionConflict,
} from '@/features/federation/hooks'
import { testProviderEntry } from '@/features/federation/api'
import type { ProviderUsage } from '@/features/federation/types'
import type {
  AgentProviderCapability,
  CatalogModelRate,
  CliRuntimeEntryResponse,
  PricingBinding,
  PricingCatalogStatus,
  ProviderCredentialMethod,
  ProviderEntryResponse,
  ProviderEntryTestResponse,
  ProviderPricing,
  RateBuckets,
  ReplaceProviderPricingBinding,
} from '@/types/generated'
import {
  EmptyPanel,
  ErrorPanel,
  LoadingPanel,
  SectionKicker,
  StateBadge,
} from '@/features/federation/components'
import { formatRateAmount } from '@/lib/money-format'
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
            .map(
              (window) =>
                `${windowLabel(window.window_minutes)} ${Math.round(window.used_percent)}%`,
            )
            .join(' · ')}
        </p>
      ) : null}
    </div>
  )
}

type PricingSourceKind = 'models_dev_catalog' | 'manual_override'

type PricingSubject = {
  id: string
  label: string
  kind: 'provider' | 'cli_runtime'
  daemonId?: string
  executorType?: string
  caveat: boolean
}

type RateField = 'input' | 'output' | 'cache_read' | 'cache_write'

type DraftRates = Record<RateField, string>

type DraftBinding = {
  key: string
  runtime_model: string
  source_kind: PricingSourceKind
  effective_at: string | null
  catalog_provider_id: string | null
  catalog_model_id: string | null
  catalog_rate_revision_id: string | null
  rates: DraftRates
}

const RATE_FIELDS: Array<{ key: RateField; label: string }> = [
  { key: 'input', label: 'Input' },
  { key: 'output', label: 'Output' },
  { key: 'cache_read', label: 'Cache read' },
  { key: 'cache_write', label: 'Cache write' },
]

const EMPTY_DRAFT_RATES: DraftRates = {
  input: '',
  output: '',
  cache_read: '',
  cache_write: '',
}

function newPricingIdempotencyKey(prefix: string): string {
  const randomUuid =
    typeof crypto !== 'undefined' && 'randomUUID' in crypto ? crypto.randomUUID() : null
  return `${prefix}:${randomUuid ?? `${Date.now()}`}`
}

function boundedErrorMessage(cause: unknown, fallback: string): string {
  const message = cause instanceof Error ? cause.message : fallback
  const compact = message.replace(/\s+/g, ' ').trim()
  return compact.length > 160 ? `${compact.slice(0, 157)}…` : compact
}

function timestampLabel(value: string | null): string {
  return value ?? 'Not available'
}

function rateFieldError(value: string): string | null {
  if (value.length === 0) return null
  const match = /^(\d+)(?:\.(\d+))?$/.exec(value)
  if (!match) {
    return 'Enter a non-negative decimal without an exponent.'
  }
  const integer = match[1].replace(/^0+(?=\d)/, '')
  const fraction = match[2] ?? ''
  if (fraction && fraction.length > 9) return 'Use at most 9 fractional digits.'
  if (
    integer.length > 7 ||
    (integer.length === 7 && integer > '1000000') ||
    (integer === '1000000' && /[1-9]/.test(fraction))
  ) {
    return 'Use no more than 1,000,000 USD per 1M tokens.'
  }
  return null
}

function draftRatesFromBinding(binding: PricingBinding): DraftRates {
  return {
    input: binding.manual_rates?.input?.decimal_per_million ?? '',
    output: binding.manual_rates?.output?.decimal_per_million ?? '',
    cache_read: binding.manual_rates?.cache_read?.decimal_per_million ?? '',
    cache_write: binding.manual_rates?.cache_write?.decimal_per_million ?? '',
  }
}

function draftFromBinding(binding: PricingBinding): DraftBinding {
  return {
    key: binding.id,
    runtime_model: binding.runtime_model,
    source_kind: binding.source_kind,
    effective_at: binding.effective_at,
    catalog_provider_id: binding.catalog_provider_id,
    catalog_model_id: binding.catalog_model_id,
    catalog_rate_revision_id: binding.catalog_rate_revision_id,
    rates: draftRatesFromBinding(binding),
  }
}

function manualRatesFromDraft(rates: DraftRates): RateBuckets {
  const toRate = (value: string) =>
    value.length > 0 ? { currency: 'USD' as const, decimal_per_million: value } : null
  return {
    input: toRate(rates.input),
    output: toRate(rates.output),
    cache_read: toRate(rates.cache_read),
    cache_write: toRate(rates.cache_write),
  }
}

function requestBindingFromDraft(draft: DraftBinding): ReplaceProviderPricingBinding {
  return {
    runtime_model: draft.runtime_model.trim(),
    source_kind: draft.source_kind,
    catalog_provider_id:
      draft.source_kind === 'models_dev_catalog' ? draft.catalog_provider_id : null,
    catalog_model_id: draft.source_kind === 'models_dev_catalog' ? draft.catalog_model_id : null,
    catalog_rate_revision_id:
      draft.source_kind === 'models_dev_catalog' ? draft.catalog_rate_revision_id : null,
    manual_rates:
      draft.source_kind === 'manual_override' ? manualRatesFromDraft(draft.rates) : null,
  }
}

function safeRateLabel(rate: { currency: 'USD'; decimal_per_million: string } | null): string {
  if (!rate) return 'Unknown'
  try {
    return formatRateAmount(rate)
  } catch {
    return 'Unknown'
  }
}

function bindingRates(binding: PricingBinding): string {
  const rates = binding.manual_rates
  if (!rates) {
    if (binding.catalog_provider_id && binding.catalog_model_id) {
      return `Catalog ${binding.catalog_provider_id}/${binding.catalog_model_id} · rate revision ${binding.catalog_rate_revision_id ?? 'unknown'}`
    }
    return 'Catalog rates shown when selected'
  }
  return RATE_FIELDS.map(({ key, label }) => `${label}: ${safeRateLabel(rates[key])}`).join(' · ')
}

function bindingSourceLabel(source: PricingSourceKind): string {
  return source === 'manual_override' ? 'Manual override' : 'models.dev catalog'
}

function bindingCatalogLabel(binding: {
  source_kind: PricingSourceKind
  catalog_provider_id: string | null
  catalog_model_id: string | null
  catalog_rate_revision_id: string | null
}): string {
  if (binding.source_kind === 'manual_override') return 'Manual rates'
  if (!binding.catalog_provider_id || !binding.catalog_model_id) return 'Catalog row pending'
  return `${binding.catalog_provider_id}/${binding.catalog_model_id} · revision ${binding.catalog_rate_revision_id ?? 'unknown'}`
}

type BindingStateShape = {
  runtime_model: string
  source_kind: PricingSourceKind
  effective_at: string | null
  retired_at?: string | null
}

function hasActiveManualOverride(bindings: BindingStateShape[], runtimeModel: string): boolean {
  return bindings.some(
    (binding) =>
      binding.runtime_model === runtimeModel &&
      binding.source_kind === 'manual_override' &&
      !binding.retired_at,
  )
}

function bindingStateLabel(binding: BindingStateShape, bindings: BindingStateShape[]): string {
  if (binding.retired_at) return 'Retired'
  if (binding.source_kind === 'manual_override') return 'Manual override · wins'
  if (hasActiveManualOverride(bindings, binding.runtime_model)) return 'Catalog fallback'
  return binding.effective_at ? 'Active' : 'Draft'
}

function catalogOptionValue(model: CatalogModelRate): string {
  return JSON.stringify({
    provider_id: model.provider_id,
    model_id: model.model_id,
    catalog_rate_revision_id: model.rate_revision_id,
  })
}

function parseCatalogOption(value: string): {
  provider_id: string
  model_id: string
  catalog_rate_revision_id: string
} | null {
  if (!value) return null
  try {
    const parsed: unknown = JSON.parse(value)
    if (
      typeof parsed === 'object' &&
      parsed !== null &&
      typeof (parsed as { provider_id?: unknown }).provider_id === 'string' &&
      typeof (parsed as { model_id?: unknown }).model_id === 'string' &&
      typeof (parsed as { catalog_rate_revision_id?: unknown }).catalog_rate_revision_id ===
        'string'
    ) {
      return parsed as {
        provider_id: string
        model_id: string
        catalog_rate_revision_id: string
      }
    }
  } catch {
    // Treat a malformed select value as an unselected model.
  }
  return null
}

function PricingBindingSummary({ pricing }: { pricing: ProviderPricing | undefined }) {
  if (!pricing) {
    return <p className="text-xs text-muted-foreground">No exact model pricing configured yet.</p>
  }
  if (pricing.bindings.length === 0) {
    return <p className="text-xs text-muted-foreground">No exact model pricing configured yet.</p>
  }
  const visible = pricing.bindings.slice(0, 2)
  const activeBindings = pricing.bindings.filter((binding) => !binding.retired_at)
  return (
    <div className="space-y-2">
      {visible.map((binding) => (
        <div
          key={binding.id}
          className="min-w-0 rounded-md border border-border-subtle bg-background/40 px-3 py-2"
        >
          <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
            <code className="min-w-0 break-all text-xs font-medium text-foreground">
              {binding.runtime_model}
            </code>
            <span className="shrink-0 font-mono text-micro uppercase tracking-[0.7px] text-muted-foreground">
              {binding.retired_at
                ? 'Retired'
                : `${bindingSourceLabel(binding.source_kind)} · ${bindingStateLabel(binding, activeBindings)}`}
            </span>
          </div>
          <p className="mt-1 break-words text-micro leading-5 text-muted-foreground">
            {binding.retired_at ? `Retired ${binding.retired_at}` : bindingRates(binding)} ·
            Effective {binding.effective_at}
          </p>
        </div>
      ))}
      {pricing.bindings.length > visible.length ? (
        <p className="text-micro text-muted-foreground">
          +{pricing.bindings.length - visible.length} more exact model binding
          {pricing.bindings.length - visible.length === 1 ? '' : 's'}
        </p>
      ) : null}
      {activeBindings.some((binding) => binding.source_kind === 'manual_override') &&
      activeBindings.some((binding) => binding.source_kind === 'models_dev_catalog') ? (
        <p className="text-micro leading-5 text-muted-foreground">
          Manual overrides win for an exact runtime model; the catalog binding remains available as
          its fallback when the override is retired.
        </p>
      ) : null}
    </div>
  )
}

function PricingSummary({
  subject,
  pricing,
  isLoading,
  isError,
  onConfigure,
}: {
  subject: PricingSubject
  pricing: ProviderPricing | undefined
  isLoading: boolean
  isError: boolean
  onConfigure: () => void
}) {
  return (
    <section
      className="mt-4 rounded-md border border-ember-border bg-ember-surface/40 px-3 py-3"
      aria-labelledby={`pricing-heading-${subject.id}`}
    >
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <SectionKicker>Pricing</SectionKicker>
          <h4
            id={`pricing-heading-${subject.id}`}
            className="mt-1 text-sm font-semibold text-foreground"
          >
            Monetary model rates
          </h4>
        </div>
        <Button size="sm" variant="outline" onClick={onConfigure}>
          <PencilSimple size={14} aria-hidden />
          Configure pricing
        </Button>
      </div>
      <p className="mt-1.5 text-xs leading-5 text-muted-foreground">
        Exact per-model rates are separate from quota Usage and Rate limits.
      </p>
      {isLoading ? <p className="mt-2 text-xs text-muted-foreground">Loading pricing…</p> : null}
      {isError ? (
        <p className="mt-2 text-xs text-destructive" role="alert">
          Pricing configuration unavailable. Refresh to try again.
        </p>
      ) : null}
      {!isLoading && !isError ? (
        <div className="mt-2">
          <PricingBindingSummary pricing={pricing} />
        </div>
      ) : null}
      {subject.caveat ? (
        <p className="mt-2 text-micro leading-5 text-warning">
          Public API list prices may not describe this subscription, private contract, or custom
          runtime. Configure an exact rate only when it matches your billing terms.
        </p>
      ) : null}
    </section>
  )
}

export function PricingCatalogStatusPanel() {
  const statusQuery = usePricingCatalogStatusQuery()
  const refresh = useRefreshPricingCatalogMutation()
  const [notice, setNotice] = useState<string>()
  const [error, setError] = useState<string>()

  async function refreshCatalog() {
    setNotice(undefined)
    setError(undefined)
    try {
      await refresh.mutateAsync({
        idempotency_key: newPricingIdempotencyKey('pricing-catalog-refresh'),
      })
      setNotice(
        'Pricing catalog refresh completed. Last-known-good rates remain available if the source was unchanged or failed validation.',
      )
    } catch (cause) {
      setError(boundedErrorMessage(cause, 'Pricing catalog refresh failed.'))
      void statusQuery.refetch()
    }
  }

  if (statusQuery.isLoading) return <LoadingPanel label="Loading pricing catalog status" />

  const status = statusQuery.data
  if (statusQuery.isError || !status) {
    return (
      <Card className="border-destructive/30 bg-destructive/5 p-4" role="alert">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <SectionKicker>Pricing catalog</SectionKicker>
            <h2 className="mt-1 text-sm font-semibold text-foreground">
              Catalog status unavailable
            </h2>
            <p className="mt-1 text-xs leading-5 text-muted-foreground">
              Forge could not read the server-owned catalog status. Existing provider execution
              remains available.
            </p>
          </div>
          <Button size="sm" variant="outline" onClick={() => void statusQuery.refetch()}>
            <ArrowClockwise size={14} aria-hidden />
            Refresh
          </Button>
        </div>
      </Card>
    )
  }

  const stateCopy: Record<PricingCatalogStatus['state'], string> = {
    absent:
      'No pricing catalog has been loaded. Rates remain unknown until an explicit refresh succeeds.',
    fresh: 'Pricing catalog is fresh. Exact catalog rows are available for binding.',
    stale:
      'Pricing catalog is stale. The last-known-good rates remain active and eligible for estimates.',
    refresh_failed: 'Pricing catalog refresh failed. The last-known-good rates remain active.',
  }
  const stateLabel: Record<PricingCatalogStatus['state'], string> = {
    absent: 'Absent',
    fresh: 'Fresh',
    stale: 'Stale',
    refresh_failed: 'Refresh failed',
  }
  const stateRole = status.state === 'refresh_failed' ? 'alert' : 'status'

  return (
    <Card className="border-border-subtle bg-card/80 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <SectionKicker>Pricing catalog</SectionKicker>
          <div className="mt-1 flex flex-wrap items-center gap-2">
            <h2 className="text-sm font-semibold text-foreground">models.dev estimate source</h2>
            <StateBadge status={status.state} label={stateLabel[status.state]} />
          </div>
          <p
            className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground"
            role={stateRole}
            aria-live="polite"
          >
            {stateCopy[status.state]}
          </p>
        </div>
        <Button
          size="sm"
          variant="outline"
          disabled={refresh.isPending}
          onClick={() => void refreshCatalog()}
        >
          <ArrowClockwise
            size={14}
            className={refresh.isPending ? 'animate-spin' : ''}
            aria-hidden
          />
          {refresh.isPending ? 'Refreshing…' : 'Refresh catalog'}
        </Button>
      </div>
      <div className="mt-3 grid min-w-0 gap-2 text-xs sm:grid-cols-2 lg:grid-cols-4">
        <div className="min-w-0">
          <p className="text-muted-foreground">Effective snapshot</p>
          <p className="mt-0.5 break-all font-mono text-foreground">
            {status.active_snapshot_id ?? 'None'}
          </p>
        </div>
        <div className="min-w-0">
          <p className="text-muted-foreground">Revision</p>
          <p className="mt-0.5 break-all font-mono text-foreground">{status.revision ?? 'None'}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Fetched at</p>
          <time className="mt-0.5 block text-foreground" dateTime={status.fetched_at ?? undefined}>
            {timestampLabel(status.fetched_at)}
          </time>
        </div>
        <div>
          <p className="text-muted-foreground">Last checked</p>
          <time
            className="mt-0.5 block text-foreground"
            dateTime={status.last_checked_at ?? undefined}
          >
            {timestampLabel(status.last_checked_at)}
          </time>
        </div>
      </div>
      {status.stale_after ? (
        <p className="mt-2 text-micro text-muted-foreground">Fresh through {status.stale_after}.</p>
      ) : null}
      {status.last_error_code ? (
        <p className="mt-2 break-words text-xs text-warning">
          Last refresh issue: {status.last_error_code.slice(0, 120)}
        </p>
      ) : null}
      {notice ? (
        <p className="mt-2 text-xs text-success" role="status">
          {notice}
        </p>
      ) : null}
      {error ? (
        <p className="mt-2 break-words text-xs text-destructive" role="alert">
          {error}
        </p>
      ) : null}
    </Card>
  )
}

export function PricingConfigurationDialog({
  open,
  subject,
  onClose,
}: {
  open: boolean
  subject: PricingSubject | null
  onClose: () => void
}) {
  const providerPricingQuery = useProviderPricingQuery(
    subject?.kind === 'provider' && open ? subject.id : undefined,
  )
  const cliPricingQuery = useCliRuntimePricingQuery(
    subject?.kind === 'cli_runtime' && open ? subject.daemonId : undefined,
    subject?.kind === 'cli_runtime' && open ? subject.executorType : undefined,
  )
  const catalogStatusQuery = usePricingCatalogStatusQuery({ enabled: open })
  const [drafts, setDrafts] = useState<DraftBinding[]>([])
  const [retiredBindings, setRetiredBindings] = useState<PricingBinding[]>([])
  const [subjectRevisionDigest, setSubjectRevisionDigest] = useState('')
  const [version, setVersion] = useState(0)
  const [runtimeModel, setRuntimeModel] = useState('')
  const [sourceKind, setSourceKind] = useState<PricingSourceKind>('models_dev_catalog')
  const [catalogSearch, setCatalogSearch] = useState('')
  const [catalogCursor, setCatalogCursor] = useState<string>()
  const [catalogRows, setCatalogRows] = useState<CatalogModelRate[]>([])
  const [catalogSelection, setCatalogSelection] = useState('')
  const [manualRates, setManualRates] = useState<DraftRates>(EMPTY_DRAFT_RATES)
  const [editingKey, setEditingKey] = useState<string | null>(null)
  const [formError, setFormError] = useState<string>()
  const [rateErrors, setRateErrors] = useState<Partial<Record<RateField, string>>>({})
  const [error, setError] = useState<string>()
  const [notice, setNotice] = useState<string>()
  const hydratedFor = useRef<string | null>(null)
  const subjectKey = subject ? `${subject.kind}:${subject.id}` : 'none'

  const pricingQuery = subject?.kind === 'provider' ? providerPricingQuery : cliPricingQuery
  const replaceProvider = useReplaceProviderPricingMutation()
  const replaceCliRuntime = useReplaceCliRuntimePricingMutation()
  const catalogModelsQuery = usePricingCatalogModelsQuery(
    { limit: 30, cursor: catalogCursor, query: catalogSearch.trim() || undefined },
    {
      enabled:
        open && sourceKind === 'models_dev_catalog' && catalogStatusQuery.data?.state !== 'absent',
    },
  )

  useEffect(() => {
    if (!open) return
    const page = catalogModelsQuery.data?.items
    if (!page) return
    const modelKey = (model: CatalogModelRate) =>
      `${model.provider_id}:${model.model_id}:${model.rate_revision_id}`
    setCatalogRows((current) => {
      if (!catalogCursor) {
        const unchanged =
          current.length === page.length &&
          current.every((model, index) => modelKey(model) === modelKey(page[index]))
        return unchanged ? current : page
      }
      const existing = new Set(current.map(modelKey))
      const next = [...current, ...page.filter((model) => !existing.has(modelKey(model)))]
      return next.length === current.length ? current : next
    })
  }, [catalogCursor, catalogModelsQuery.data, open])

  useEffect(() => {
    if (!open) {
      hydratedFor.current = null
      return
    }
    if (!pricingQuery.data || hydratedFor.current === subjectKey) return
    hydratedFor.current = subjectKey
    setSubjectRevisionDigest(pricingQuery.data.subject_revision_digest)
    setVersion(pricingQuery.data.version)
    setDrafts(
      pricingQuery.data.bindings.filter((binding) => !binding.retired_at).map(draftFromBinding),
    )
    setRetiredBindings(pricingQuery.data.bindings.filter((binding) => Boolean(binding.retired_at)))
    setEditingKey(null)
    setFormError(undefined)
    setError(undefined)
    setNotice(undefined)
  }, [open, pricingQuery.data, subjectKey])

  useEffect(() => {
    if (!open) return
    setRuntimeModel('')
    setSourceKind('models_dev_catalog')
    setCatalogSearch('')
    setCatalogCursor(undefined)
    setCatalogRows([])
    setCatalogSelection('')
    setManualRates(EMPTY_DRAFT_RATES)
    setEditingKey(null)
    setFormError(undefined)
    setRateErrors({})
  }, [open, subjectKey])

  if (!open || !subject) return null

  const currentSubject = subject
  const selectedCatalog = parseCatalogOption(catalogSelection)
  const catalogModels = catalogRows
  const catalogState = catalogStatusQuery.data?.state

  function resetBindingForm() {
    setRuntimeModel('')
    setSourceKind('models_dev_catalog')
    setCatalogSearch('')
    setCatalogCursor(undefined)
    setCatalogRows([])
    setCatalogSelection('')
    setManualRates(EMPTY_DRAFT_RATES)
    setEditingKey(null)
    setFormError(undefined)
    setRateErrors({})
  }

  function startEditing(binding: DraftBinding) {
    setEditingKey(binding.key)
    setRuntimeModel(binding.runtime_model)
    setSourceKind(binding.source_kind)
    setCatalogSearch('')
    setCatalogCursor(undefined)
    setCatalogRows([])
    setCatalogSelection(
      binding.catalog_provider_id && binding.catalog_model_id && binding.catalog_rate_revision_id
        ? JSON.stringify({
            provider_id: binding.catalog_provider_id,
            model_id: binding.catalog_model_id,
            catalog_rate_revision_id: binding.catalog_rate_revision_id,
          })
        : '',
    )
    setManualRates(binding.rates)
    setFormError(undefined)
    setRateErrors({})
  }

  function updateManualRate(field: RateField, value: string) {
    setManualRates((current) => ({ ...current, [field]: value }))
    setRateErrors((current) => ({ ...current, [field]: rateFieldError(value) ?? undefined }))
  }

  function addOrUpdateDraft() {
    const trimmedModel = runtimeModel.trim()
    if (!trimmedModel) {
      setFormError('Enter the exact runtime model ID.')
      return
    }

    const nextRateErrors: Partial<Record<RateField, string>> = {}
    if (sourceKind === 'manual_override') {
      for (const { key } of RATE_FIELDS) {
        const invalid = rateFieldError(manualRates[key])
        if (invalid) nextRateErrors[key] = invalid
      }
    }
    setRateErrors(nextRateErrors)
    if (Object.keys(nextRateErrors).length > 0) {
      setFormError('Fix the highlighted rate fields before adding this model.')
      return
    }
    if (
      sourceKind === 'manual_override' &&
      Object.values(manualRates).every((value) => value === '')
    ) {
      setFormError('Enter at least one manual USD rate, or choose a catalog binding.')
      return
    }
    if (sourceKind === 'models_dev_catalog' && !selectedCatalog) {
      setFormError('Choose one exact catalog provider/model pair.')
      return
    }

    const next: DraftBinding = {
      key: editingKey ?? `draft:${sourceKind}:${trimmedModel}`,
      runtime_model: trimmedModel,
      source_kind: sourceKind,
      effective_at: null,
      catalog_provider_id: selectedCatalog?.provider_id ?? null,
      catalog_model_id: selectedCatalog?.model_id ?? null,
      catalog_rate_revision_id: selectedCatalog?.catalog_rate_revision_id ?? null,
      rates: manualRates,
    }
    setDrafts((current) => {
      const duplicate = current.some(
        (binding) =>
          binding.runtime_model === trimmedModel &&
          binding.source_kind === sourceKind &&
          binding.key !== editingKey,
      )
      if (duplicate) return current
      return editingKey
        ? current.map((binding) => (binding.key === editingKey ? next : binding))
        : [...current, next]
    })
    const duplicate = drafts.some(
      (binding) =>
        binding.runtime_model === trimmedModel &&
        binding.source_kind === sourceKind &&
        binding.key !== editingKey,
    )
    if (duplicate) {
      setFormError('That exact runtime model already has a binding.')
      return
    }
    setNotice(editingKey ? 'Model pricing draft updated.' : 'Model pricing draft added.')
    resetBindingForm()
  }

  function retireDraft(key: string) {
    const binding = drafts.find((candidate) => candidate.key === key)
    if (!binding) return
    setDrafts((current) => current.filter((candidate) => candidate.key !== key))
    if (editingKey === key) resetBindingForm()
    setNotice(`${binding.runtime_model} will be retired when you save pricing.`)
  }

  function catalogRatesFor(binding: DraftBinding): RateBuckets | null {
    if (binding.source_kind !== 'models_dev_catalog') return manualRatesFromDraft(binding.rates)
    const match = catalogModels.find(
      (model) =>
        model.provider_id === binding.catalog_provider_id &&
        model.model_id === binding.catalog_model_id &&
        model.rate_revision_id === binding.catalog_rate_revision_id,
    )
    return match?.rates ?? null
  }

  function validateDrafts(): boolean {
    for (const binding of drafts) {
      if (binding.source_kind === 'manual_override') {
        for (const { key } of RATE_FIELDS) {
          const invalid = rateFieldError(binding.rates[key])
          if (invalid) {
            setError(`${binding.runtime_model}: ${invalid}`)
            return false
          }
        }
        if (Object.values(binding.rates).every((value) => value === '')) {
          setError(`${binding.runtime_model}: enter at least one manual USD rate.`)
          return false
        }
      }
      if (
        binding.source_kind === 'models_dev_catalog' &&
        (!binding.catalog_provider_id ||
          !binding.catalog_model_id ||
          !binding.catalog_rate_revision_id)
      ) {
        setError(`${binding.runtime_model}: choose one exact catalog provider/model pair.`)
        return false
      }
    }
    return true
  }

  async function savePricing() {
    setError(undefined)
    setNotice(undefined)
    if (!validateDrafts()) return
    if (!subjectRevisionDigest) {
      setError('The pricing subject revision is not available yet. Refresh before saving.')
      return
    }

    const input = {
      expected_version: version,
      idempotency_key: newPricingIdempotencyKey(`pricing-save:${currentSubject.id}`),
      subject_revision_digest: subjectRevisionDigest,
      bindings: drafts.map(requestBindingFromDraft),
    }

    try {
      const saved =
        currentSubject.kind === 'provider'
          ? await replaceProvider.mutateAsync({ subjectId: currentSubject.id, input })
          : await replaceCliRuntime.mutateAsync({
              daemonId: currentSubject.daemonId!,
              executorType: currentSubject.executorType!,
              input,
            })
      setVersion(saved.version)
      setSubjectRevisionDigest(saved.subject_revision_digest)
      setDrafts(saved.bindings.filter((binding) => !binding.retired_at).map(draftFromBinding))
      setRetiredBindings(saved.bindings.filter((binding) => Boolean(binding.retired_at)))
      setNotice('Pricing saved. Future work uses these exact bindings.')
    } catch (cause) {
      if (isVersionConflict(cause)) {
        setError(
          'Pricing changed in another session. Your draft is preserved; refresh current pricing before saving again.',
        )
      } else {
        setError(boundedErrorMessage(cause, 'Pricing could not be saved.'))
      }
    }
  }

  async function refreshPricing() {
    setNotice(undefined)
    try {
      const refreshed = await pricingQuery.refetch()
      if (refreshed.isError) throw refreshed.error ?? new Error('Pricing refresh failed.')
      setNotice('Current pricing refreshed. Your draft remains in this dialog for review.')
    } catch (cause) {
      setError(boundedErrorMessage(cause, 'Current pricing could not be refreshed.'))
    }
  }

  const mutationPending = replaceProvider.isPending || replaceCliRuntime.isPending

  return (
    <Dialog open={open} onOpenChange={(next) => !next && onClose()}>
      <DialogContent className="max-w-4xl">
        <DialogHeader>
          <SectionKicker>
            {subject.kind === 'provider' ? 'Provider entry' : 'CLI runtime'} · Pricing
          </SectionKicker>
          <DialogTitle className="mt-1">Configure exact model pricing</DialogTitle>
          <DialogDescription>
            Bind each runtime model to one exact catalog row or a manual USD rate. Forge never
            infers a family price.
          </DialogDescription>
        </DialogHeader>

        <div className="mt-5 space-y-5">
          <section
            className="rounded-md border border-border-subtle bg-muted/20 px-3 py-3"
            aria-labelledby="pricing-subject-heading"
          >
            <SectionKicker>Pricing subject</SectionKicker>
            <h3
              id="pricing-subject-heading"
              className="mt-1 break-all font-mono text-sm font-semibold text-foreground"
            >
              {subject.label}
            </h3>
            <p className="mt-1 break-all text-micro text-muted-foreground">
              Subject revision: {subjectRevisionDigest || 'Loading…'} · version {version}
            </p>
            {subject.caveat ? (
              <p className="mt-2 text-xs leading-5 text-warning">
                Subscription, private-contract, and custom runtimes require an explicit rate that
                matches your contract; public API list prices are not assumed.
              </p>
            ) : null}
          </section>

          {pricingQuery.isLoading ? <LoadingPanel label="Loading pricing bindings" /> : null}
          {pricingQuery.isError ? (
            <div
              className="rounded-md border border-destructive/30 bg-destructive/5 px-3 py-3"
              role="alert"
            >
              <p className="text-sm font-medium text-foreground">Pricing bindings unavailable.</p>
              <p className="mt-1 text-xs leading-5 text-muted-foreground">
                Refresh to load the authoritative subject revision before editing.
              </p>
              <Button
                size="sm"
                variant="outline"
                className="mt-3"
                onClick={() => void refreshPricing()}
              >
                <ArrowClockwise size={14} aria-hidden />
                Refresh pricing
              </Button>
            </div>
          ) : null}

          {!pricingQuery.isLoading && !pricingQuery.isError ? (
            <>
              <section
                className="min-w-0 overflow-hidden rounded-md border border-border-subtle"
                aria-labelledby="configured-pricing-heading"
              >
                <div className="flex flex-wrap items-start justify-between gap-3 border-b border-border-subtle bg-muted/20 px-3 py-3">
                  <div>
                    <SectionKicker>Configured bindings</SectionKicker>
                    <h3
                      id="configured-pricing-heading"
                      className="mt-1 text-sm font-semibold text-foreground"
                    >
                      Exact model pricing
                    </h3>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    {drafts.length} active draft{drafts.length === 1 ? '' : 's'}
                  </p>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full min-w-[760px] border-collapse text-left text-xs">
                    <caption className="sr-only">
                      Configured exact model pricing bindings and per-million-token rates
                    </caption>
                    <thead className="bg-muted/20 text-muted-foreground">
                      <tr>
                        <th scope="col" className="px-3 py-2 font-medium">
                          Runtime model
                        </th>
                        <th scope="col" className="px-3 py-2 font-medium">
                          Bound catalog model
                        </th>
                        <th scope="col" className="px-3 py-2 font-medium">
                          Source
                        </th>
                        <th scope="col" className="px-3 py-2 font-medium">
                          Input
                        </th>
                        <th scope="col" className="px-3 py-2 font-medium">
                          Output
                        </th>
                        <th scope="col" className="px-3 py-2 font-medium">
                          Cache read
                        </th>
                        <th scope="col" className="px-3 py-2 font-medium">
                          Cache write
                        </th>
                        <th scope="col" className="px-3 py-2 font-medium">
                          State
                        </th>
                        <th scope="col" className="px-3 py-2 font-medium">
                          Effective
                        </th>
                        <th scope="col" className="px-3 py-2 font-medium">
                          <span className="sr-only">Actions</span>
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border-subtle">
                      {drafts.map((binding) => {
                        const rates = catalogRatesFor(binding)
                        return (
                          <tr key={binding.key}>
                            <th
                              scope="row"
                              className="max-w-[190px] break-all px-3 py-3 font-mono font-medium text-foreground"
                            >
                              {binding.runtime_model}
                            </th>
                            <td className="max-w-[240px] break-all px-3 py-3 text-muted-foreground">
                              {bindingCatalogLabel(binding)}
                            </td>
                            <td className="whitespace-nowrap px-3 py-3 text-muted-foreground">
                              {bindingSourceLabel(binding.source_kind)}
                            </td>
                            {RATE_FIELDS.map(({ key }) => (
                              <td key={key} className="whitespace-nowrap px-3 py-3 text-foreground">
                                {safeRateLabel(rates?.[key] ?? null)}
                              </td>
                            ))}
                            <td className="whitespace-nowrap px-3 py-3 text-muted-foreground">
                              {bindingStateLabel(binding, drafts)}
                            </td>
                            <td className="whitespace-nowrap px-3 py-3 text-muted-foreground">
                              {binding.effective_at ?? 'On save'}
                            </td>
                            <td className="px-3 py-3">
                              <div className="flex items-center gap-1">
                                <Button
                                  size="icon-sm"
                                  variant="ghost"
                                  aria-label={`Edit ${binding.runtime_model}`}
                                  title={`Edit ${binding.runtime_model}`}
                                  onClick={() => startEditing(binding)}
                                >
                                  <PencilSimple size={14} aria-hidden />
                                </Button>
                                <Button
                                  size="icon-sm"
                                  variant="ghost"
                                  aria-label={`Retire ${binding.runtime_model}`}
                                  title={`Retire ${binding.runtime_model}`}
                                  onClick={() => retireDraft(binding.key)}
                                >
                                  <Trash size={14} aria-hidden />
                                </Button>
                              </div>
                            </td>
                          </tr>
                        )
                      })}
                      {retiredBindings.map((binding) => (
                        <tr key={`retired:${binding.id}`} className="text-muted-foreground">
                          <th
                            scope="row"
                            className="max-w-[190px] break-all px-3 py-3 font-mono font-medium"
                          >
                            {binding.runtime_model}
                          </th>
                          <td className="max-w-[240px] break-all px-3 py-3">
                            {bindingCatalogLabel(binding)}
                          </td>
                          <td className="whitespace-nowrap px-3 py-3">
                            {bindingSourceLabel(binding.source_kind)}
                          </td>
                          {RATE_FIELDS.map(({ key }) => (
                            <td key={key} className="whitespace-nowrap px-3 py-3">
                              {safeRateLabel(binding.manual_rates?.[key] ?? null)}
                            </td>
                          ))}
                          <td className="whitespace-nowrap px-3 py-3">Retired</td>
                          <td className="whitespace-nowrap px-3 py-3">{binding.effective_at}</td>
                          <td className="px-3 py-3">—</td>
                        </tr>
                      ))}
                      {drafts.length === 0 && retiredBindings.length === 0 ? (
                        <tr>
                          <td colSpan={10} className="px-3 py-6 text-center text-muted-foreground">
                            No exact model bindings yet. Add one below.
                          </td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>
              </section>

              <section
                className="rounded-md border border-border-subtle bg-card px-3 py-4"
                aria-labelledby="binding-editor-heading"
              >
                <SectionKicker>
                  {editingKey ? 'Edit exact binding' : 'Add exact binding'}
                </SectionKicker>
                <h3
                  id="binding-editor-heading"
                  className="mt-1 text-sm font-semibold text-foreground"
                >
                  {editingKey ? 'Update model pricing draft' : 'Add a runtime model'}
                </h3>
                <div className="mt-3 grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)]">
                  <div className="space-y-3">
                    <div className="space-y-2">
                      <Label htmlFor="pricing-runtime-model">Runtime model ID</Label>
                      <Input
                        id="pricing-runtime-model"
                        value={runtimeModel}
                        onChange={(event) => setRuntimeModel(event.target.value)}
                        placeholder="The exact model string used by this runtime"
                        aria-describedby="pricing-runtime-model-help"
                      />
                      <p
                        id="pricing-runtime-model-help"
                        className="text-micro leading-5 text-muted-foreground"
                      >
                        Keep namespaces and slashes exactly as reported.
                      </p>
                    </div>
                    <fieldset className="space-y-2">
                      <legend className="text-xs font-medium text-foreground">
                        Pricing source
                      </legend>
                      <label className="flex items-center gap-2 text-xs text-foreground">
                        <input
                          type="radio"
                          name="pricing-source"
                          value="models_dev_catalog"
                          checked={sourceKind === 'models_dev_catalog'}
                          onChange={() => {
                            setSourceKind('models_dev_catalog')
                            setRateErrors({})
                            setFormError(undefined)
                          }}
                        />
                        Exact models.dev catalog row
                      </label>
                      <label className="flex items-center gap-2 text-xs text-foreground">
                        <input
                          type="radio"
                          name="pricing-source"
                          value="manual_override"
                          checked={sourceKind === 'manual_override'}
                          onChange={() => {
                            setSourceKind('manual_override')
                            setRateErrors({})
                            setFormError(undefined)
                          }}
                        />
                        Manual override
                      </label>
                    </fieldset>
                  </div>

                  {sourceKind === 'models_dev_catalog' ? (
                    <div className="space-y-3">
                      <div className="space-y-2">
                        <Label htmlFor="pricing-catalog-search">Search catalog models</Label>
                        <div className="relative">
                          <MagnifyingGlass
                            size={15}
                            className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground"
                            aria-hidden
                          />
                          <Input
                            id="pricing-catalog-search"
                            className="pl-9"
                            value={catalogSearch}
                            onChange={(event) => {
                              setCatalogSearch(event.target.value)
                              setCatalogCursor(undefined)
                              setCatalogRows([])
                            }}
                            placeholder="Search exact provider/model IDs"
                          />
                        </div>
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="pricing-catalog-model">Catalog provider/model</Label>
                        <select
                          id="pricing-catalog-model"
                          value={catalogSelection}
                          onChange={(event) => setCatalogSelection(event.target.value)}
                          className="flex w-full rounded-md border border-input bg-background px-3 py-2 text-ui text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                        >
                          <option value="">Choose an exact catalog row</option>
                          {catalogModels.map((model) => (
                            <option
                              key={`${model.provider_id}:${model.model_id}:${model.snapshot_id}`}
                              value={catalogOptionValue(model)}
                            >
                              {model.provider_id} / {model.model_id}
                            </option>
                          ))}
                        </select>
                        {catalogState === 'absent' ? (
                          <p className="text-micro text-muted-foreground">
                            No catalog is loaded. Refresh the catalog before selecting a row.
                          </p>
                        ) : null}
                        {catalogModelsQuery.isLoading ? (
                          <p className="text-micro text-muted-foreground">Searching catalog…</p>
                        ) : null}
                        {catalogModelsQuery.isError ? (
                          <p className="text-micro text-destructive" role="alert">
                            Catalog model search unavailable.
                          </p>
                        ) : null}
                        {catalogModelsQuery.data?.has_more &&
                        catalogModelsQuery.data.next_cursor ? (
                          <Button
                            type="button"
                            size="sm"
                            variant="ghost"
                            onClick={() =>
                              setCatalogCursor(catalogModelsQuery.data?.next_cursor ?? undefined)
                            }
                          >
                            Load more catalog models
                          </Button>
                        ) : null}
                      </div>
                    </div>
                  ) : (
                    <div className="grid gap-3 sm:grid-cols-2">
                      {RATE_FIELDS.map(({ key, label }) => {
                        const inputId = `pricing-rate-${key}`
                        const errorId = `${inputId}-error`
                        return (
                          <div key={key} className="space-y-2">
                            <Label htmlFor={inputId}>{label} · USD per 1M tokens</Label>
                            <Input
                              id={inputId}
                              type="text"
                              inputMode="decimal"
                              value={manualRates[key]}
                              onChange={(event) => updateManualRate(key, event.target.value)}
                              placeholder="0 or 0.000001"
                              aria-invalid={Boolean(rateErrors[key])}
                              aria-describedby={rateErrors[key] ? errorId : undefined}
                            />
                            {rateErrors[key] ? (
                              <p id={errorId} className="text-micro leading-5 text-destructive">
                                {rateErrors[key]}
                              </p>
                            ) : null}
                          </div>
                        )
                      })}
                    </div>
                  )}
                </div>
                {formError ? (
                  <p className="mt-3 text-xs text-destructive" role="alert">
                    {formError}
                  </p>
                ) : null}
                <div className="mt-4 flex flex-wrap gap-2">
                  <Button type="button" variant="outline" onClick={addOrUpdateDraft}>
                    <Plus size={14} aria-hidden />
                    {editingKey ? 'Update model pricing' : 'Add model pricing'}
                  </Button>
                  {editingKey ? (
                    <Button type="button" variant="ghost" onClick={resetBindingForm}>
                      Cancel edit
                    </Button>
                  ) : null}
                </div>
              </section>

              {error ? (
                <div
                  className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-3"
                  role="alert"
                >
                  <p className="min-w-0 break-words text-xs text-destructive">{error}</p>
                  {error.includes('another session') ? (
                    <Button size="sm" variant="outline" onClick={() => void refreshPricing()}>
                      <ArrowClockwise size={14} aria-hidden />
                      Refresh
                    </Button>
                  ) : null}
                </div>
              ) : null}
              {notice ? (
                <p className="text-xs text-success" role="status">
                  {notice}
                </p>
              ) : null}

              <DialogFooter className="gap-2">
                <Button type="button" variant="ghost" onClick={onClose}>
                  Cancel
                </Button>
                <Button type="button" disabled={mutationPending} onClick={() => void savePricing()}>
                  <FloppyDisk size={14} aria-hidden />
                  {mutationPending ? 'Saving…' : 'Save pricing'}
                </Button>
              </DialogFooter>
            </>
          ) : null}
        </div>
      </DialogContent>
    </Dialog>
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

function ProviderEntryCard({
  entry,
  onShowAgents,
}: {
  entry: ProviderEntryResponse
  onShowAgents: () => void
}) {
  const rename = useRenameProviderEntryMutation()
  const remove = useRemoveProviderEntryMutation()
  const setAvailability = useSetProviderEntryAvailabilityMutation()
  const queryClient = useQueryClient()
  const usageQuery = useProviderUsageQuery(
    entry.status === 'configured' && entry.enabled ? entry.id : undefined,
  )
  const pricingQuery = useProviderPricingQuery(entry.id)
  const [pricingOpen, setPricingOpen] = useState(false)
  const [renaming, setRenaming] = useState(false)
  const [confirmingRemoval, setConfirmingRemoval] = useState(false)
  const [confirmingDisable, setConfirmingDisable] = useState(false)
  const [label, setLabel] = useState(entry.label)
  const [error, setError] = useState<string>()
  const [notice, setNotice] = useState<string>()

  const statusBadge = !entry.enabled
    ? { status: 'paused', label: 'Disabled' }
    : entry.status === 'configured'
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

  async function updateAvailability(enabled: boolean) {
    setError(undefined)
    setNotice(undefined)
    try {
      await setAvailability.mutateAsync({
        id: entry.id,
        input: { enabled, version: entry.version },
      })
      setConfirmingDisable(false)
      setNotice(
        enabled
          ? 'Provider enabled. Referencing agents can accept new work again.'
          : 'Provider disabled. Credentials, agents, and bindings were preserved.',
      )
    } catch (cause) {
      setError(
        isVersionConflict(cause)
          ? 'This provider changed in another session. Refresh and try again.'
          : cause instanceof Error
            ? cause.message
            : 'Provider availability could not be changed.',
      )
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
      {entry.status === 'configured' && entry.enabled ? (
        <section
          className="mt-3 rounded-md border border-border-subtle bg-muted/20 px-3 py-2 text-xs"
          aria-labelledby={`quota-heading-${entry.id}`}
        >
          <SectionKicker>Quota usage</SectionKicker>
          <h4 id={`quota-heading-${entry.id}`} className="sr-only">
            Quota Usage and Rate limits
          </h4>
          {usageQuery.isLoading ? (
            <span className="text-muted-foreground">Checking usage…</span>
          ) : usageQuery.isError ? (
            <span className="text-muted-foreground">Usage unavailable</span>
          ) : usageQuery.data ? (
            <UsageSummary usage={usageQuery.data} />
          ) : null}
        </section>
      ) : null}
      <PricingSummary
        subject={{
          id: entry.id,
          label: entry.label,
          kind: 'provider',
          caveat:
            entry.credential_method !== 'api_key' ||
            ['openai_compatible', 'custom', 'private_contract', 'subscription'].includes(
              entry.provider.toLowerCase(),
            ),
        }}
        pricing={pricingQuery.data}
        isLoading={pricingQuery.isLoading}
        isError={pricingQuery.isError}
        onConfigure={() => setPricingOpen(true)}
      />
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
            <Button
              size="sm"
              variant="destructive"
              disabled={remove.isPending}
              onClick={() => void disconnect()}
            >
              Disconnect
            </Button>
            <Button size="sm" variant="ghost" onClick={() => setConfirmingRemoval(false)}>
              Keep
            </Button>
          </div>
        </div>
      ) : null}
      {confirmingDisable ? (
        <div
          className="mt-3 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning"
          role="alertdialog"
          aria-label={`Confirm disabling ${entry.label}`}
        >
          <p>
            Disable this provider for {entry.used_by.length} referencing agent
            {entry.used_by.length === 1 ? '' : 's'}
            {entry.used_by.length > 0
              ? ` (${entry.used_by.map((agent) => agent.agent_name).join(', ')})`
              : ''}
            ? Their configuration and bindings stay in place, but new work stops until you enable it
            again.
          </p>
          <div className="mt-2 flex gap-2">
            <Button
              size="sm"
              variant="outline"
              disabled={setAvailability.isPending}
              onClick={() => void updateAvailability(false)}
            >
              Disable provider
            </Button>
            <Button size="sm" variant="ghost" onClick={() => setConfirmingDisable(false)}>
              Keep enabled
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
      {entry.status !== 'revoked' && !confirmingRemoval && !confirmingDisable ? (
        <div className="mt-4 flex flex-wrap gap-2 border-t border-border-subtle pt-3">
          <Button
            size="sm"
            variant="outline"
            disabled={setAvailability.isPending}
            onClick={() =>
              entry.enabled ? setConfirmingDisable(true) : void updateAvailability(true)
            }
          >
            {entry.enabled ? 'Disable' : 'Enable'}
          </Button>
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
      <PricingConfigurationDialog
        open={pricingOpen}
        subject={{
          id: entry.id,
          label: entry.label,
          kind: 'provider',
          caveat:
            entry.credential_method !== 'api_key' ||
            ['openai_compatible', 'custom', 'private_contract', 'subscription'].includes(
              entry.provider,
            ),
        }}
        onClose={() => setPricingOpen(false)}
      />
    </Card>
  )
}

function CliRuntimeCard({ runtime }: { runtime: CliRuntimeEntryResponse }) {
  const setAvailability = useSetCliRuntimeAvailabilityMutation()
  const pricingQuery = useCliRuntimePricingQuery(runtime.daemon_id, runtime.kind)
  const [pricingOpen, setPricingOpen] = useState(false)
  const [confirmingDisable, setConfirmingDisable] = useState(false)
  const [error, setError] = useState<string>()
  const authenticated = runtime.availability === 'authenticated'
  const badgeLabel = !runtime.enabled
    ? 'Disabled'
    : authenticated
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
              status={!runtime.enabled ? 'paused' : authenticated ? 'healthy' : 'unavailable'}
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
      <PricingSummary
        subject={{
          id: `${runtime.daemon_id}:${runtime.kind}`,
          label: `${runtimeDisplayNames[runtime.kind] ?? humanize(runtime.kind)} · ${runtime.daemon_hostname ?? runtime.daemon_id}`,
          kind: 'cli_runtime',
          daemonId: runtime.daemon_id,
          executorType: runtime.kind,
          caveat: true,
        }}
        pricing={pricingQuery.data}
        isLoading={pricingQuery.isLoading}
        isError={pricingQuery.isError}
        onConfigure={() => setPricingOpen(true)}
      />
      {confirmingDisable ? (
        <div className="mt-3 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning">
          <p>
            Disable this runtime on this host? {runtime.used_by.length} referencing agent
            {runtime.used_by.length === 1 ? '' : 's'} will stop accepting new work, but all
            configuration stays in place.
          </p>
          <div className="mt-2 flex gap-2">
            <Button
              size="sm"
              variant="outline"
              disabled={setAvailability.isPending}
              onClick={() =>
                void setAvailability
                  .mutateAsync({
                    daemonId: runtime.daemon_id,
                    executorType: runtime.kind,
                    input: { enabled: false, version: runtime.policy_version },
                  })
                  .then(() => setConfirmingDisable(false))
                  .catch((cause: unknown) =>
                    setError(
                      cause instanceof Error ? cause.message : 'Runtime could not be disabled.',
                    ),
                  )
              }
            >
              Disable runtime
            </Button>
            <Button size="sm" variant="ghost" onClick={() => setConfirmingDisable(false)}>
              Keep enabled
            </Button>
          </div>
        </div>
      ) : null}
      {error ? (
        <p className="mt-2 text-xs text-destructive" role="alert">
          {error}
        </p>
      ) : null}
      {!confirmingDisable ? (
        <div className="mt-4 border-t border-border-subtle pt-3">
          <Button
            size="sm"
            variant="outline"
            disabled={setAvailability.isPending}
            onClick={() => {
              setError(undefined)
              if (runtime.enabled) {
                setConfirmingDisable(true)
                return
              }
              void setAvailability
                .mutateAsync({
                  daemonId: runtime.daemon_id,
                  executorType: runtime.kind,
                  input: { enabled: true, version: runtime.policy_version },
                })
                .catch((cause: unknown) =>
                  setError(
                    cause instanceof Error ? cause.message : 'Runtime could not be enabled.',
                  ),
                )
            }}
          >
            {runtime.enabled ? 'Disable' : 'Enable'}
          </Button>
        </div>
      ) : null}
      <PricingConfigurationDialog
        open={pricingOpen}
        subject={{
          id: `${runtime.daemon_id}:${runtime.kind}`,
          label: `${runtimeDisplayNames[runtime.kind] ?? humanize(runtime.kind)} · ${runtime.daemon_hostname ?? runtime.daemon_id}`,
          kind: 'cli_runtime',
          daemonId: runtime.daemon_id,
          executorType: runtime.kind,
          caveat: true,
        }}
        onClose={() => setPricingOpen(false)}
      />
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

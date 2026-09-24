import { useId, useState } from 'react'
import { PencilSimple } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { formatRateAmount } from '@/lib/money-format'
import type {
  AgentPricing,
  PricingMode,
  PricingSettings,
  RateAmount,
  RateBuckets,
  UpdatePricingSettingsRequest,
} from '@/types/generated'
import { SectionKicker } from './components'
import {
  isVersionConflict,
  useAgentPricingQuery,
  useCliRuntimePricingQuery,
  usePricingCatalogModelsQuery,
  useProviderPricingQuery,
  useResetAgentPricingMutation,
  useResetCliRuntimePricingMutation,
  useResetProviderPricingMutation,
  useUpdateAgentPricingMutation,
  useUpdateCliRuntimePricingMutation,
  useUpdateProviderPricingMutation,
} from './hooks'

type PricingSubject =
  | { kind: 'provider'; id: string }
  | { kind: 'cli_runtime'; daemonId: string; executorType: string }
  | { kind: 'agent'; id: string; providerLabel?: string }

type RateField = keyof RateBuckets
type RateDraft = Record<RateField, string>
type Draft = {
  mode: PricingMode
  discount: string
  rates: RateDraft
  providerId: string
  modelId: string
}

const RATE_FIELDS: { key: RateField; label: string }[] = [
  { key: 'input', label: 'Input' },
  { key: 'output', label: 'Output' },
  { key: 'cache_read', label: 'Cache read' },
  { key: 'cache_write', label: 'Cache write' },
]

function draftFrom(settings: PricingSettings | null, runtimeModel: string | null): Draft {
  return {
    mode: settings?.mode ?? 'list',
    discount: settings?.discount_percent ?? '',
    rates: {
      input: settings?.fixed_rates?.input?.decimal_per_million ?? '',
      output: settings?.fixed_rates?.output?.decimal_per_million ?? '',
      cache_read: settings?.fixed_rates?.cache_read?.decimal_per_million ?? '',
      cache_write: settings?.fixed_rates?.cache_write?.decimal_per_million ?? '',
    },
    providerId: settings?.catalog_provider_id ?? '',
    modelId: settings?.catalog_model_id ?? runtimeModel ?? '',
  }
}

function rateLabel(rate: RateAmount | null): string {
  if (!rate) return '—'
  try {
    return formatRateAmount(rate).replace(' / 1M tokens', '')
  } catch {
    return '—'
  }
}

export function pricingSettingsSummary(settings: PricingSettings | null): string {
  if (!settings || settings.mode === 'list') return 'List price (models.dev)'
  if (settings.mode === 'discount') return `${settings.discount_percent ?? '0'}% off list price`
  return `Fixed rates: ${rateLabel(settings.fixed_rates?.input ?? null)} in / ${rateLabel(settings.fixed_rates?.output ?? null)} out per 1M`
}

function ratePair(rates: RateBuckets | null): string {
  if (!rates) return 'Unavailable'
  return `${rateLabel(rates.input)} in / ${rateLabel(rates.output)} out per 1M`
}

function rateError(value: string): string | null {
  if (!value) return null
  const match = /^(\d+)(?:\.(\d+))?$/.exec(value)
  if (!match) return 'Enter a non-negative decimal without an exponent.'
  const whole = match[1].replace(/^0+(?=\d)/, '')
  const fraction = match[2] ?? ''
  if (fraction.length > 9) return 'Use at most 9 decimal places.'
  if (
    whole.length > 7 ||
    Number(whole) > 1_000_000 ||
    (whole === '1000000' && /[1-9]/.test(fraction))
  ) {
    return 'Use no more than 1,000,000 USD per 1M tokens.'
  }
  return null
}

function validationError(
  draft: Draft,
  isAgent: boolean,
  runtimeModel: string | null,
): string | null {
  if (
    draft.mode === 'discount' &&
    !/^(?:100(?:\.0{1,2})?|\d{1,2}(?:\.\d{1,2})?)$/.test(draft.discount)
  ) {
    return 'Enter a discount from 0 to 100 with at most two decimal places.'
  }
  if (draft.mode === 'fixed') {
    if (RATE_FIELDS.every(({ key }) => !draft.rates[key])) return 'Enter at least one fixed rate.'
    for (const { key, label } of RATE_FIELDS) {
      const error = rateError(draft.rates[key])
      if (error) return `${label}: ${error}`
    }
  }
  if (
    isAgent &&
    draft.modelId.trim() &&
    draft.modelId.trim() !== runtimeModel &&
    !draft.providerId.trim()
  ) {
    return 'Choose a models.dev provider before pinning a model.'
  }
  return null
}

function requestFrom(
  draft: Draft,
  version: number,
  isAgent: boolean,
): UpdatePricingSettingsRequest {
  const rate = (value: string): RateAmount | null =>
    value ? { currency: 'USD', decimal_per_million: value } : null
  return {
    mode: draft.mode,
    discount_percent: draft.mode === 'discount' ? draft.discount : null,
    fixed_rates:
      draft.mode === 'fixed'
        ? {
            input: rate(draft.rates.input),
            output: rate(draft.rates.output),
            cache_read: rate(draft.rates.cache_read),
            cache_write: rate(draft.rates.cache_write),
          }
        : null,
    catalog_provider_id: draft.providerId.trim() || null,
    catalog_model_id: isAgent && draft.providerId.trim() ? draft.modelId.trim() || null : null,
    expected_version: version,
  }
}

function AgentResolution({
  pricing,
  providerLabel,
  onChooseProvider,
  pending,
}: {
  pricing: AgentPricing
  providerLabel?: string
  onChooseProvider: (providerId: string) => void
  pending: boolean
}) {
  const statusCopy: Record<AgentPricing['status'], string> = {
    priced: '',
    no_model: 'Set a model to price this agent',
    catalog_absent: 'Refresh the models.dev catalog in Providers',
    not_in_catalog: 'No models.dev match — pin a provider/model or set fixed rates',
    ambiguous: 'Listed by several providers — pick one',
  }
  return (
    <div className="mt-2 space-y-1 text-xs text-muted-foreground">
      {pricing.runtime_model ? (
        <p className="break-all font-mono text-foreground">
          {pricing.runtime_model} →{' '}
          {pricing.catalog_provider_id && pricing.catalog_model_id
            ? `${pricing.catalog_provider_id} / ${pricing.catalog_model_id}`
            : 'No models.dev row selected'}
        </p>
      ) : null}
      {pricing.status !== 'priced' ? <p role="status">{statusCopy[pricing.status]}</p> : null}
      {pricing.status === 'ambiguous' && pricing.runtime_model ? (
        <label className="flex flex-wrap items-center gap-2">
          <span>models.dev provider</span>
          <select
            aria-label="Choose models.dev provider"
            className="h-8 rounded-md border border-input bg-background px-2 text-xs text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            defaultValue=""
            disabled={pending}
            onChange={(event) => onChooseProvider(event.target.value)}
          >
            <option value="">Pick a provider</option>
            {pricing.candidate_providers.map((provider) => (
              <option key={provider} value={provider}>
                {provider}
              </option>
            ))}
          </select>
        </label>
      ) : null}
      <p>List rates: {ratePair(pricing.catalog_rates)}</p>
      <p>Effective rates: {ratePair(pricing.effective_rates)}</p>
      <p>
        {pricing.source === 'agent'
          ? `Agent override: ${pricingSettingsSummary(pricing.settings)}`
          : pricing.source === 'provider'
            ? `Inherited from provider${providerLabel ? ` (${providerLabel})` : ''}: ${pricingSettingsSummary(pricing.provider_settings)}`
            : 'List price'}
      </p>
    </div>
  )
}

export function PricingSettingsEditor({ subject }: { subject: PricingSubject }) {
  const fieldId = useId()
  const providerQuery = useProviderPricingQuery(
    subject.kind === 'provider' ? subject.id : undefined,
  )
  const cliQuery = useCliRuntimePricingQuery(
    subject.kind === 'cli_runtime' ? subject.daemonId : undefined,
    subject.kind === 'cli_runtime' ? subject.executorType : undefined,
  )
  const agentQuery = useAgentPricingQuery(subject.kind === 'agent' ? subject.id : undefined)
  const updateProvider = useUpdateProviderPricingMutation()
  const resetProvider = useResetProviderPricingMutation()
  const updateCli = useUpdateCliRuntimePricingMutation()
  const resetCli = useResetCliRuntimePricingMutation()
  const updateAgent = useUpdateAgentPricingMutation()
  const resetAgent = useResetAgentPricingMutation()
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState<Draft | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const isAgent = subject.kind === 'agent'
  const catalogModels = usePricingCatalogModelsQuery(
    {
      limit: 20,
      provider_id: isAgent ? draft?.providerId.trim() || undefined : undefined,
      query: isAgent ? draft?.modelId.trim() || undefined : undefined,
    },
    { enabled: isAgent && editing },
  )
  const query = isAgent ? agentQuery : subject.kind === 'provider' ? providerQuery : cliQuery
  const agentPricing = isAgent ? agentQuery.data : undefined
  const settings = isAgent
    ? (agentPricing?.settings ?? null)
    : subject.kind === 'provider'
      ? (providerQuery.data?.settings ?? null)
      : (cliQuery.data?.settings ?? null)
  const pending =
    updateProvider.isPending ||
    resetProvider.isPending ||
    updateCli.isPending ||
    resetCli.isPending ||
    updateAgent.isPending ||
    resetAgent.isPending

  function beginEdit() {
    setDraft(
      draftFrom(
        settings ?? (isAgent ? (agentPricing?.provider_settings ?? null) : null),
        isAgent ? (agentPricing?.runtime_model ?? null) : null,
      ),
    )
    setError(null)
    setNotice(null)
    setEditing(true)
  }

  async function save(input: UpdatePricingSettingsRequest) {
    if (subject.kind === 'provider')
      await updateProvider.mutateAsync({ subjectId: subject.id, input })
    else if (subject.kind === 'cli_runtime')
      await updateCli.mutateAsync({
        daemonId: subject.daemonId,
        executorType: subject.executorType,
        input,
      })
    else await updateAgent.mutateAsync({ agentId: subject.id, input })
  }

  async function reset() {
    if (!settings) return
    setError(null)
    setNotice(null)
    try {
      if (subject.kind === 'provider')
        await resetProvider.mutateAsync({ subjectId: subject.id, version: settings.version })
      else if (subject.kind === 'cli_runtime')
        await resetCli.mutateAsync({
          daemonId: subject.daemonId,
          executorType: subject.executorType,
          version: settings.version,
        })
      else await resetAgent.mutateAsync({ agentId: subject.id, version: settings.version })
      setEditing(false)
      setNotice(isAgent ? 'Using provider setting.' : 'Reset to list price.')
    } catch (cause) {
      setError(
        isVersionConflict(cause)
          ? 'Pricing changed in another session. Refresh and try again.'
          : cause instanceof Error
            ? cause.message
            : 'Pricing could not be reset.',
      )
    }
  }

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!draft) return
    const invalid = validationError(draft, isAgent, agentPricing?.runtime_model ?? null)
    if (invalid) {
      setError(invalid)
      return
    }
    setError(null)
    setNotice(null)
    try {
      await save(requestFrom(draft, settings?.version ?? 0, isAgent))
      setEditing(false)
      setNotice('Pricing saved.')
    } catch (cause) {
      setError(
        isVersionConflict(cause)
          ? 'Pricing changed in another session. Refresh and try again.'
          : cause instanceof Error
            ? cause.message
            : 'Pricing could not be saved.',
      )
    }
  }

  async function chooseProvider(providerId: string) {
    if (!agentPricing?.runtime_model || !providerId) return
    const next = draftFrom(
      agentPricing.settings ?? agentPricing.provider_settings,
      agentPricing.runtime_model,
    )
    next.providerId = providerId
    next.modelId = agentPricing.runtime_model
    setError(null)
    try {
      await save(requestFrom(next, agentPricing.settings?.version ?? 0, true))
      setNotice('models.dev model pinned.')
    } catch (cause) {
      setError(
        isVersionConflict(cause)
          ? 'Pricing changed in another session. Refresh and try again.'
          : cause instanceof Error
            ? cause.message
            : 'Model could not be pinned.',
      )
    }
  }

  return (
    <section
      className="mt-4 rounded-md border border-border-subtle bg-card p-3"
      aria-label="Pricing"
    >
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <SectionKicker>Pricing</SectionKicker>
          {query.isLoading ? (
            <p className="mt-1 text-xs text-muted-foreground">Loading pricing…</p>
          ) : null}
          {!query.isLoading && !query.isError ? (
            <p className="mt-1 text-sm font-medium text-foreground">
              {pricingSettingsSummary(
                settings ?? (isAgent ? (agentPricing?.provider_settings ?? null) : null),
              )}
            </p>
          ) : null}
        </div>
        {!query.isLoading && !query.isError && !editing ? (
          <Button size="sm" variant="outline" onClick={beginEdit}>
            <PencilSimple size={14} aria-hidden /> Edit
          </Button>
        ) : null}
      </div>
      {query.isError ? (
        <p className="mt-2 text-xs text-destructive" role="alert">
          Pricing unavailable. Refresh to try again.
        </p>
      ) : null}
      {!isAgent && settings?.catalog_provider_id ? (
        <p className="mt-1 text-xs text-muted-foreground">
          Price as models.dev provider: {settings.catalog_provider_id}
        </p>
      ) : null}
      {isAgent && agentPricing ? (
        <AgentResolution
          pricing={agentPricing}
          providerLabel={subject.providerLabel}
          onChooseProvider={(value) => void chooseProvider(value)}
          pending={pending}
        />
      ) : null}
      {!isAgent ? (
        <p className="mt-2 text-xs text-muted-foreground">
          {subject.kind === 'provider'
            ? 'Applies to every agent using this provider.'
            : 'Applies to every agent using this runtime.'}{' '}
          Agents can override.
        </p>
      ) : null}
      {editing && draft ? (
        <form
          className="mt-3 space-y-3 border-t border-border-subtle pt-3"
          onSubmit={(event) => void submit(event)}
        >
          <div>
            <Label>Adjustment</Label>
            <div className="mt-1 flex flex-wrap gap-1" role="group" aria-label="Pricing adjustment">
              {(
                [
                  ['list', 'List'],
                  ['discount', 'Discount %'],
                  ['fixed', 'Fixed rates'],
                ] as const
              ).map(([mode, label]) => (
                <Button
                  key={mode}
                  type="button"
                  size="sm"
                  variant={draft.mode === mode ? 'secondary' : 'ghost'}
                  aria-pressed={draft.mode === mode}
                  onClick={() => setDraft({ ...draft, mode })}
                >
                  {label}
                </Button>
              ))}
            </div>
          </div>
          {draft.mode === 'discount' ? (
            <div className="max-w-40 space-y-1">
              <Label htmlFor={`${fieldId}-discount`}>Discount percent</Label>
              <Input
                id={`${fieldId}-discount`}
                inputMode="decimal"
                value={draft.discount}
                onChange={(event) => setDraft({ ...draft, discount: event.target.value })}
                aria-invalid={Boolean(
                  draft.discount &&
                  validationError(draft, isAgent, agentPricing?.runtime_model ?? null),
                )}
              />
            </div>
          ) : null}
          {draft.mode === 'fixed' ? (
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
              {RATE_FIELDS.map(({ key, label }) => (
                <div key={key} className="space-y-1">
                  <Label htmlFor={`${fieldId}-${key}`}>{label} · USD per 1M tokens</Label>
                  <Input
                    id={`${fieldId}-${key}`}
                    inputMode="decimal"
                    value={draft.rates[key]}
                    onChange={(event) =>
                      setDraft({ ...draft, rates: { ...draft.rates, [key]: event.target.value } })
                    }
                    aria-invalid={Boolean(rateError(draft.rates[key]))}
                  />
                </div>
              ))}
            </div>
          ) : null}
          <div className="space-y-1">
            <Label htmlFor={`${fieldId}-provider`}>models.dev provider</Label>
            <Input
              id={`${fieldId}-provider`}
              value={draft.providerId}
              onChange={(event) => setDraft({ ...draft, providerId: event.target.value })}
            />
            <p className="text-xs text-muted-foreground">
              Leave empty to infer from the model name
            </p>
          </div>
          {isAgent ? (
            <div className="space-y-1">
              <Label htmlFor={`${fieldId}-model`}>models.dev model</Label>
              <Input
                id={`${fieldId}-model`}
                value={draft.modelId}
                onChange={(event) => setDraft({ ...draft, modelId: event.target.value })}
              />
              <p className="text-xs text-muted-foreground">
                Defaults to the agent&apos;s runtime model. Set a provider to pin this model.
              </p>
              {catalogModels.data?.items.length ? (
                <select
                  aria-label="Choose a catalog model"
                  className="h-8 w-full rounded-md border border-input bg-background px-2 text-xs text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  value=""
                  onChange={(event) => {
                    const row = catalogModels.data?.items.find(
                      (item) =>
                        JSON.stringify([item.provider_id, item.model_id]) === event.target.value,
                    )
                    if (row)
                      setDraft({ ...draft, providerId: row.provider_id, modelId: row.model_id })
                  }}
                >
                  <option value="">Pick a matching catalog row</option>
                  {catalogModels.data.items.map((row) => (
                    <option
                      key={JSON.stringify([row.provider_id, row.model_id])}
                      value={JSON.stringify([row.provider_id, row.model_id])}
                    >
                      {row.provider_id} / {row.model_id}
                    </option>
                  ))}
                </select>
              ) : null}
            </div>
          ) : null}
          <div className="flex flex-wrap gap-2">
            <Button type="submit" size="sm" disabled={pending}>
              {pending ? 'Saving…' : 'Save'}
            </Button>
            <Button
              type="button"
              size="sm"
              variant="ghost"
              disabled={pending}
              onClick={() => {
                setEditing(false)
                setError(null)
              }}
            >
              Cancel
            </Button>
            {settings ? (
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={pending}
                onClick={() => void reset()}
              >
                {isAgent ? 'Use provider setting' : 'Reset to list price'}
              </Button>
            ) : null}
          </div>
        </form>
      ) : null}
      {error ? (
        <div className="mt-2 text-xs text-destructive" role="alert">
          {error}{' '}
          {error.startsWith('Pricing changed') ? (
            <button type="button" className="underline" onClick={() => void query.refetch()}>
              Refresh
            </button>
          ) : null}
        </div>
      ) : null}
      {notice ? (
        <p className="mt-2 text-xs text-muted-foreground" role="status">
          {notice}
        </p>
      ) : null}
    </section>
  )
}

import type { ReactNode } from 'react'
import type {
  AgentUsageBreakdown,
  CostSummary,
  ModelUsageBreakdown,
  ProjectUsageBreakdown,
  SurfaceUsageBreakdown,
  TokenCounters,
  UsageAnalytics,
  UsageSurface,
} from '@/types/generated'
import { cn } from '@/lib/cn'
import { ActivityCountsView, CostSummaryView, TokenCountersView } from './CostSummary'
import {
  ACCOUNT_USAGE_SURFACES,
  formatCount,
  formatNullableIdentity,
  formatUsageSurface,
  sortBySurface,
  USAGE_SURFACE_ORDER,
} from './analytics-format'

const ZERO_TOKENS: TokenCounters = {
  input_tokens: 0,
  output_tokens: 0,
  cache_read_tokens: 0,
  cache_write_tokens: 0,
}

function noUsageCost(): CostSummary {
  return {
    kind: 'none',
    coverage: 'no_usage',
    provider_reported: null,
    estimated: null,
    known_subtotal: null,
    complete_total: null,
    usage_coverage: {
      total_runs_or_turns: 0,
      pending_runs_or_turns: 0,
      no_provider_call_runs_or_turns: 0,
      fully_metered_runs_or_turns: 0,
      fully_costed_runs_or_turns: 0,
      partially_costed_runs_or_turns: 0,
      unavailable_cost_runs_or_turns: 0,
      total_provider_attempts: 0,
      settled_provider_attempts: 0,
      pending_provider_attempts: 0,
      unsettled_provider_attempts: 0,
      metered_provider_attempts: 0,
      unmetered_provider_attempts: 0,
      costed_provider_attempts: 0,
      unpriced_provider_attempts: 0,
      priced_tokens: { ...ZERO_TOKENS },
      unpriced_tokens: { ...ZERO_TOKENS },
      reasons: [],
    },
    sources: [],
  }
}

function noUsageSurface(surface: UsageSurface): SurfaceUsageBreakdown {
  return {
    surface,
    counts: {
      task_execution_count: 0,
      chat_turn_count: 0,
      inquiry_count: 0,
      provider_attempt_count: 0,
    },
    tokens: { ...ZERO_TOKENS },
    cost: noUsageCost(),
  }
}

function materializeSurfaceRows(
  rows: readonly SurfaceUsageBreakdown[],
  surfaces: readonly UsageSurface[],
): SurfaceUsageBreakdown[] {
  const rowsBySurface = new Map<UsageSurface, SurfaceUsageBreakdown>(
    rows.map((row) => [row.surface, row]),
  )
  return surfaces.map((surface) => rowsBySurface.get(surface) ?? noUsageSurface(surface))
}

function UsageTable({
  caption,
  children,
  className,
}: {
  caption: string
  children: ReactNode
  className?: string
}) {
  return (
    <div
      className={cn(
        'min-w-0 overflow-x-auto rounded-md border border-border-subtle focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2',
        className,
      )}
      tabIndex={0}
      role="region"
      aria-label={caption}
    >
      <table className="min-w-[980px] table-fixed text-xs">
        <caption className="sr-only">{caption}</caption>
        {children}
      </table>
    </div>
  )
}

function CountCells({
  counts,
  includeInquiry = true,
}: {
  counts: SurfaceUsageBreakdown['counts']
  includeInquiry?: boolean
}) {
  return (
    <>
      <td className="whitespace-nowrap px-3 py-2 align-top font-mono tabular-nums">
        {formatCount(counts.task_execution_count)}
      </td>
      <td className="whitespace-nowrap px-3 py-2 align-top font-mono tabular-nums">
        {formatCount(counts.chat_turn_count)}
      </td>
      {includeInquiry ? (
        <td className="whitespace-nowrap px-3 py-2 align-top font-mono tabular-nums">
          {formatCount(counts.inquiry_count)}
        </td>
      ) : null}
      <td className="whitespace-nowrap px-3 py-2 align-top font-mono tabular-nums">
        {formatCount(counts.provider_attempt_count)}
      </td>
    </>
  )
}

function TokenCells({ tokens }: { tokens: SurfaceUsageBreakdown['tokens'] }) {
  return (
    <>
      <td className="whitespace-nowrap px-3 py-2 align-top font-mono tabular-nums">
        {formatCount(tokens.input_tokens)}
      </td>
      <td className="whitespace-nowrap px-3 py-2 align-top font-mono tabular-nums">
        {formatCount(tokens.output_tokens)}
      </td>
      <td className="whitespace-nowrap px-3 py-2 align-top font-mono tabular-nums">
        {formatCount(tokens.cache_read_tokens)}
      </td>
      <td className="whitespace-nowrap px-3 py-2 align-top font-mono tabular-nums">
        {formatCount(tokens.cache_write_tokens)}
      </td>
    </>
  )
}

function TableHead({ includeInquiry = true }: { includeInquiry?: boolean }) {
  return (
    <thead className="bg-muted/50 text-left">
      <tr>
        <th scope="col" className="w-40 px-3 py-2 font-medium text-foreground">
          Group
        </th>
        <th scope="col" className="px-3 py-2 font-medium text-foreground">
          Task executions
        </th>
        <th scope="col" className="px-3 py-2 font-medium text-foreground">
          Chat turns
        </th>
        {includeInquiry ? (
          <th scope="col" className="px-3 py-2 font-medium text-foreground">
            Inquiries
          </th>
        ) : null}
        <th scope="col" className="px-3 py-2 font-medium text-foreground">
          Provider attempts
        </th>
        <th scope="col" className="px-3 py-2 font-medium text-foreground">
          Input tokens
        </th>
        <th scope="col" className="px-3 py-2 font-medium text-foreground">
          Output tokens
        </th>
        <th scope="col" className="px-3 py-2 font-medium text-foreground">
          Cache read
        </th>
        <th scope="col" className="px-3 py-2 font-medium text-foreground">
          Cache write
        </th>
        <th scope="col" className="min-w-72 px-3 py-2 font-medium text-foreground">
          Cost summary
        </th>
      </tr>
    </thead>
  )
}

export function SurfaceUsageTable({
  rows,
  surfaces = ACCOUNT_USAGE_SURFACES,
}: {
  rows: readonly SurfaceUsageBreakdown[]
  surfaces?: readonly UsageSurface[]
}) {
  const materializedRows = materializeSurfaceRows(rows, surfaces)

  return (
    <div className="space-y-2">
      <div>
        <h4 className="text-sm font-semibold text-foreground">By surface</h4>
        <p className="mt-0.5 text-xs text-muted-foreground">
          Product surfaces stay separate; a Genesis turn is never counted again as ordinary Main
          Chat.
        </p>
      </div>
      {materializedRows.length === 0 ? (
        <p
          className="rounded-md border border-dashed border-border-subtle bg-muted/20 px-3 py-4 text-xs text-muted-foreground"
          role="status"
        >
          No surface usage was returned for this window.
        </p>
      ) : (
        <UsageTable caption="Usage and cost by product surface">
          <TableHead />
          <tbody>
            {sortBySurface(materializedRows).map((row) => (
              <tr key={row.surface} className="border-t border-border-subtle align-top">
                <th scope="row" className="px-3 py-2 text-left font-medium text-foreground">
                  {formatUsageSurface(row.surface)}
                </th>
                <CountCells counts={row.counts} />
                <TokenCells tokens={row.tokens} />
                <td className="min-w-72 px-3 py-2 align-top">
                  <CostSummaryView summary={row.cost} compact />
                </td>
              </tr>
            ))}
          </tbody>
        </UsageTable>
      )}
    </div>
  )
}

export function ModelUsageTable({ rows }: { rows: readonly ModelUsageBreakdown[] }) {
  return (
    <div className="space-y-2">
      <div>
        <h4 className="text-sm font-semibold text-foreground">By provider / model</h4>
        <p className="mt-0.5 text-xs text-muted-foreground">
          Exact historical provider and model identities are shown; missing identity remains
          unknown.
        </p>
      </div>
      {rows.length === 0 ? (
        <p
          className="rounded-md border border-dashed border-border-subtle bg-muted/20 px-3 py-4 text-xs text-muted-foreground"
          role="status"
        >
          No provider/model usage was returned for this window.
        </p>
      ) : (
        <UsageTable caption="Usage and cost by provider and model">
          <TableHead />
          <tbody>
            {rows.map((row, index) => (
              <tr
                key={`${row.provider_id ?? 'unknown-provider'}:${row.model_id ?? 'unknown-model'}:${index}`}
                className="border-t border-border-subtle align-top"
              >
                <th
                  scope="row"
                  className="min-w-40 px-3 py-2 text-left font-medium text-foreground"
                >
                  <span className="block break-all" title={row.provider_id ?? undefined}>
                    {formatNullableIdentity(row.provider_id, 'Unknown provider')}
                  </span>
                  <span
                    className="mt-1 block break-all font-mono text-xs font-normal text-muted-foreground"
                    title={row.model_id ?? undefined}
                  >
                    {formatNullableIdentity(row.model_id, 'Unknown model')}
                  </span>
                </th>
                <CountCells counts={row.counts} />
                <TokenCells tokens={row.tokens} />
                <td className="min-w-72 px-3 py-2 align-top">
                  <CostSummaryView summary={row.cost} compact />
                </td>
              </tr>
            ))}
          </tbody>
        </UsageTable>
      )}
    </div>
  )
}

export function AgentUsageTable({ rows }: { rows: readonly AgentUsageBreakdown[] }) {
  return (
    <div className="space-y-2">
      <div>
        <h4 className="text-sm font-semibold text-foreground">By agent</h4>
        <p className="mt-0.5 text-xs text-muted-foreground">
          Agent/profile labels are immutable usage snapshots, not current configuration lookups.
        </p>
      </div>
      {rows.length === 0 ? (
        <p
          className="rounded-md border border-dashed border-border-subtle bg-muted/20 px-3 py-4 text-xs text-muted-foreground"
          role="status"
        >
          No agent usage was returned for this window.
        </p>
      ) : (
        <UsageTable caption="Usage and cost by agent">
          <TableHead />
          <tbody>
            {rows.map((row, index) => (
              <tr
                key={`${row.agent_id ?? 'unknown-agent'}:${row.profile_id ?? 'unknown-profile'}:${index}`}
                className="border-t border-border-subtle align-top"
              >
                <th
                  scope="row"
                  className="min-w-48 px-3 py-2 text-left font-medium text-foreground"
                >
                  <span className="block break-words">
                    {formatNullableIdentity(row.agent_name_snapshot, 'Unknown agent')}
                  </span>
                  <span
                    className="mt-1 block break-all font-mono text-xs font-normal text-muted-foreground"
                    title={row.agent_id ?? undefined}
                  >
                    {formatNullableIdentity(row.agent_id, 'No agent ID')}
                  </span>
                  <span
                    className="mt-1 block break-all font-mono text-xs font-normal text-muted-foreground"
                    title={row.profile_id ?? undefined}
                  >
                    Profile {formatNullableIdentity(row.profile_id, 'unknown')}
                  </span>
                  <span className="mt-1 block break-all text-xs font-normal text-muted-foreground">
                    {formatNullableIdentity(row.executor_type, 'Unknown runtime')}
                  </span>
                </th>
                <CountCells counts={row.counts} />
                <TokenCells tokens={row.tokens} />
                <td className="min-w-72 px-3 py-2 align-top">
                  <CostSummaryView summary={row.cost} compact />
                </td>
              </tr>
            ))}
          </tbody>
        </UsageTable>
      )}
    </div>
  )
}

export function ProjectUsageTable({ rows }: { rows: readonly ProjectUsageBreakdown[] }) {
  return (
    <div className="space-y-2">
      <div>
        <h4 className="text-sm font-semibold text-foreground">By Project</h4>
        <p className="mt-0.5 text-xs text-muted-foreground">
          Only usage with an immutable Project scope appears here; account-only Main Chat and
          inquiry usage stays unassigned.
        </p>
      </div>
      {rows.length === 0 ? (
        <p
          className="rounded-md border border-dashed border-border-subtle bg-muted/20 px-3 py-4 text-xs text-muted-foreground"
          role="status"
        >
          No Project-scoped usage was returned for this window.
        </p>
      ) : (
        <UsageTable caption="Usage and cost by Project">
          <TableHead />
          <tbody>
            {rows.map((row, index) => (
              <tr
                key={`${row.project_id ?? 'account-only'}:${index}`}
                className="border-t border-border-subtle align-top"
              >
                <th
                  scope="row"
                  className="min-w-48 px-3 py-2 text-left font-medium text-foreground"
                >
                  <span className="block break-words">
                    {formatNullableIdentity(
                      row.project_name_snapshot,
                      row.project_id ? 'Unknown Project' : 'Account-only usage',
                    )}
                  </span>
                  <span
                    className="mt-1 block break-all font-mono text-xs font-normal text-muted-foreground"
                    title={row.project_id ?? undefined}
                  >
                    {formatNullableIdentity(row.project_id, 'No Project ID')}
                  </span>
                </th>
                <CountCells counts={row.counts} />
                <TokenCells tokens={row.tokens} />
                <td className="min-w-72 px-3 py-2 align-top">
                  <CostSummaryView summary={row.cost} compact />
                </td>
              </tr>
            ))}
          </tbody>
        </UsageTable>
      )}
    </div>
  )
}

export function UsageAnalyticsPanel({
  analytics,
  heading = 'Usage and cost',
  description,
  surfaces = USAGE_SURFACE_ORDER,
}: {
  analytics: UsageAnalytics
  heading?: string
  description?: string
  surfaces?: readonly UsageSurface[]
}) {
  return (
    <section
      aria-labelledby="usage-analytics-heading"
      className="space-y-6 border-b border-border-subtle py-6"
    >
      <div>
        <h3 id="usage-analytics-heading" className="text-lg font-semibold text-foreground">
          {heading}
        </h3>
        <p className="mt-1 text-sm text-muted-foreground">
          {description ??
            'Token buckets, provider attempts, and monetary coverage use the shared immutable usage ledger.'}
        </p>
      </div>

      <div className="space-y-3">
        <div>
          <h4 className="text-sm font-semibold text-foreground">Activity counts</h4>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Counts are explicit and are not merged into an ambiguous execution total.
          </p>
        </div>
        <ActivityCountsView counts={analytics.counts} />
      </div>

      <div className="space-y-3">
        <div>
          <h4 className="text-sm font-semibold text-foreground">Token evidence</h4>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Input, output, cache-read, and cache-write counters remain independent.
          </p>
        </div>
        <TokenCountersView counters={analytics.tokens} />
      </div>

      <CostSummaryView summary={analytics.cost} />
      <SurfaceUsageTable rows={analytics.by_surface} surfaces={surfaces} />
      <ModelUsageTable rows={analytics.by_model} />
      <AgentUsageTable rows={analytics.by_agent} />
    </section>
  )
}

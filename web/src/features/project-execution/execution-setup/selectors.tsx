// Execution setup principal and repository selectors.

import type { ReactNode } from 'react'
import { Link } from '@tanstack/react-router'
import { ArrowClockwise, ArrowUpRight, CircleNotch, GitBranch } from '@phosphor-icons/react'
import { useReposQuery } from '@/api/hooks'
import { Button } from '@/components/ui/button'
import type { ExecutionPrincipalResponse } from '@/types/generated'

function PrincipalOption({ principal }: { principal: ExecutionPrincipalResponse }) {
  const details = [principal.provider, principal.model].filter(Boolean).join(' · ')
  return (
    <option value={principal.identity_id}>
      {principal.name}
      {details ? ` · ${details}` : ''}
    </option>
  )
}

export function ActionSelect({
  id,
  label,
  value,
  options,
  onChange,
  buttonLabel,
  pending,
  onSubmit,
  icon,
  compact = false,
}: {
  id: string
  label: string
  value: string
  options: ExecutionPrincipalResponse[]
  onChange: (value: string) => void
  buttonLabel: string
  pending: boolean
  onSubmit: () => void
  icon: ReactNode
  compact?: boolean
}) {
  return (
    <div className="min-w-0">
      <label htmlFor={id} className="text-xs font-medium text-foreground">
        {label}
      </label>
      <div
        className={`mt-2 flex min-w-0 flex-col gap-2 ${compact ? '' : 'xl:flex-row xl:items-center'}`}
      >
        <select
          id={id}
          value={value}
          onChange={(event) => onChange(event.target.value)}
          className="min-w-0 flex-1 rounded-md border border-input bg-background px-3 py-2 text-sm text-foreground shadow-xs outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring"
          aria-describedby={`${id}-hint`}
          disabled={pending}
        >
          <option value="">Select a principal…</option>
          {options.map((principal) => (
            <PrincipalOption key={principal.identity_id} principal={principal} />
          ))}
        </select>
        <Button
          type="button"
          size="sm"
          className={`w-full shrink-0 ${compact ? '' : 'sm:w-auto'}`}
          onClick={onSubmit}
          disabled={!value || pending}
        >
          {pending ? (
            <CircleNotch
              size={14}
              className="animate-spin motion-reduce:animate-none"
              aria-hidden
            />
          ) : (
            icon
          )}
          {pending ? 'Saving…' : buttonLabel}
        </Button>
      </div>
      <p id={`${id}-hint`} className="mt-2 text-xs leading-5 text-muted-foreground">
        Forge checks current eligibility and project version again when you submit.
      </p>
    </div>
  )
}

export function RepositorySelect({
  projectId,
  value,
  onChange,
  pending,
  onSubmit,
  compact = false,
}: {
  projectId: string
  value: string
  onChange: (value: string) => void
  pending: boolean
  onSubmit: () => void
  compact?: boolean
}) {
  const reposQuery = useReposQuery(projectId, { enabled: true })
  const repos = reposQuery.data?.items ?? []

  if (reposQuery.isLoading) {
    return (
      <p className="flex items-center gap-2 text-xs text-muted-foreground" role="status">
        <CircleNotch size={14} className="animate-spin motion-reduce:animate-none" aria-hidden />{' '}
        Loading repositories…
      </p>
    )
  }

  if (reposQuery.isError) {
    return (
      <div className="flex min-w-0 flex-wrap items-center gap-3">
        <p className="min-w-0 break-words text-xs text-muted-foreground">
          Forge could not load repositories for this Project.
        </p>
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="w-full sm:w-auto"
          onClick={() => void reposQuery.refetch()}
        >
          <ArrowClockwise size={14} aria-hidden /> Retry repository list
        </Button>
      </div>
    )
  }

  if (repos.length === 0) {
    return (
      <Link
        to="/projects/$projectId/settings"
        params={{ projectId }}
        className="inline-flex w-full max-w-full justify-center items-center gap-1.5 text-xs font-semibold text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto"
      >
        Create or attach repository <ArrowUpRight size={13} aria-hidden />
      </Link>
    )
  }

  return (
    <div className="min-w-0">
      <label htmlFor="execution-primary-repository" className="text-xs font-medium text-foreground">
        Primary repository
      </label>
      <div
        className={`mt-2 flex min-w-0 flex-col gap-2 ${compact ? '' : 'xl:flex-row xl:items-center'}`}
      >
        <select
          id="execution-primary-repository"
          value={value}
          onChange={(event) => onChange(event.target.value)}
          className="min-w-0 flex-1 rounded-md border border-input bg-background px-3 py-2 text-sm text-foreground shadow-xs outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring"
          aria-describedby="execution-primary-repository-hint"
          disabled={pending}
        >
          <option value="">Select a repository…</option>
          {repos.map((repo) => (
            <option key={repo.id} value={repo.id}>
              {repo.name} · {repo.default_branch}
            </option>
          ))}
        </select>
        <Button
          type="button"
          size="sm"
          className={`w-full shrink-0 ${compact ? '' : 'sm:w-auto'}`}
          onClick={onSubmit}
          disabled={!value || pending}
        >
          {pending ? (
            <CircleNotch
              size={14}
              className="animate-spin motion-reduce:animate-none"
              aria-hidden
            />
          ) : (
            <GitBranch size={14} aria-hidden />
          )}
          {pending ? 'Saving…' : 'Attach repository'}
        </Button>
      </div>
      <p
        id="execution-primary-repository-hint"
        className="mt-2 text-xs leading-5 text-muted-foreground"
      >
        The repository is an explicit binding; its presence does not infer Task intent.
      </p>
    </div>
  )
}

import {
  ChatIcon as Chat,
  CheckSquareIcon as CheckSquare,
  ClockCounterClockwiseIcon as ClockCounterClockwise,
  GitBranchIcon as GitBranch,
  HouseIcon as House,
  PlayIcon as Play,
  TerminalWindowIcon as TerminalWindow,
} from '@phosphor-icons/react'
import { Link } from '@tanstack/react-router'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/cn'
import { productTerm } from '@/lib/i18n'
import type { Task, TaskStatus } from '@/types/generated'
import { stripRunSuffix } from './utils'

const statusClassNames: Record<TaskStatus, string> = {
  todo: 'bg-slate-100 text-slate-800',
  in_progress: 'bg-sky-100 text-sky-900',
  review: 'bg-amber-100 text-amber-900',
  merging: 'bg-indigo-100 text-indigo-900',
  merge_failed: 'bg-red-100 text-red-900',
  blocked: 'bg-orange-100 text-orange-900',
  done: 'bg-emerald-100 text-emerald-900',
  cancelled: 'bg-zinc-100 text-zinc-700',
}

function statusBadge(status: TaskStatus) {
  return <Badge className={cn('border-transparent', statusClassNames[status])}>{status}</Badge>
}

type ActiveTab = 'overview' | 'executions' | 'review' | 'diff' | 'terminal' | 'comments' | 'history'

interface TaskDetailSidebarProps {
  task: Task | undefined
  isLoading: boolean
  taskId: string
  runSuffix: string
  activeTab: ActiveTab
  executionCount?: number
  commentCount?: number
  showReviewTab: boolean
}

export function TaskDetailSidebar({
  task,
  isLoading,
  taskId,
  runSuffix,
  activeTab,
  executionCount,
  commentCount,
  showReviewTab,
}: TaskDetailSidebarProps) {
  const navItems = [
    {
      id: 'overview' as const,
      label: 'Overview',
      Icon: House,
      badge: undefined as string | undefined,
    },
    {
      id: 'executions' as const,
      label: productTerm('run', 0),
      Icon: Play,
      badge: executionCount != null && executionCount > 0 ? String(executionCount) : undefined,
    },
    ...(showReviewTab
      ? [
          {
            id: 'review' as const,
            label: 'Review',
            Icon: CheckSquare,
            badge: undefined as string | undefined,
          },
        ]
      : []),
    { id: 'diff' as const, label: 'Diff', Icon: GitBranch, badge: undefined as string | undefined },
    {
      id: 'terminal' as const,
      label: 'Terminal',
      Icon: TerminalWindow,
      badge: undefined as string | undefined,
    },
    {
      id: 'comments' as const,
      label: 'Comments',
      Icon: Chat,
      badge: commentCount != null && commentCount > 0 ? String(commentCount) : undefined,
    },
    {
      id: 'history' as const,
      label: 'History',
      Icon: ClockCounterClockwise,
      badge: undefined as string | undefined,
    },
  ]

  return (
    <aside className="flex w-60 shrink-0 flex-col border-r bg-background">
      <div className="border-b px-4 py-3">
        <p className="font-mono text-micro font-semibold uppercase tracking-[1px] text-muted-foreground">
          Task
        </p>
        {isLoading ? (
          <>
            <Skeleton className="mt-1 h-4 w-3/4" />
            <Skeleton className="mt-2 h-5 w-16" />
          </>
        ) : task ? (
          <>
            <p className="mt-0.5 line-clamp-2 text-sm font-semibold text-foreground">
              {stripRunSuffix(task.title, runSuffix)}
            </p>
            <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
              {statusBadge(task.status)}
              {task.task_type ? <Badge variant="secondary">{task.task_type}</Badge> : null}
            </div>
          </>
        ) : null}
      </div>
      <nav className="flex flex-1 flex-col gap-0.5 p-2">
        {navItems.map(({ id, label, Icon: NavIcon, badge }) => (
          <Link
            key={id}
            to={id === 'overview' ? '/tasks/$taskId' : '/tasks/$taskId/$tab'}
            params={{ taskId, tab: id }}
            className={cn(
              'relative flex w-full cursor-pointer items-center gap-2.5 rounded-lg px-2.5 py-[7px] text-left text-[13px] leading-none font-medium transition-colors',
              activeTab === id
                ? 'bg-[var(--ember-surface)] text-sidebar-active-foreground before:absolute before:left-0 before:top-1/2 before:-translate-y-1/2 before:h-4 before:w-[3px] before:rounded-r-full before:bg-primary'
                : 'text-sidebar-foreground hover:bg-accent/50 hover:text-foreground',
            )}
          >
            <NavIcon size={16} />
            <span>{label}</span>
            {badge ? (
              <span className="ml-auto inline-flex h-5 min-w-[20px] items-center justify-center rounded-full bg-muted-foreground/10 px-1.5 text-micro font-medium">
                {badge}
              </span>
            ) : null}
          </Link>
        ))}
      </nav>
    </aside>
  )
}

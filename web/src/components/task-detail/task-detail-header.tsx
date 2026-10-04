import { ArrowSquareOut, X } from '@phosphor-icons/react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { TaskCapacityNotice } from '@/components/task-capacity-notice'
import { taskStatusColors } from '@/components/task-controls'
import { cn } from '@/lib/cn'
import type { KeyboardEvent } from 'react'
import type { Task } from '@/types/generated'

const statusColors = taskStatusColors

interface TaskDetailHeaderProps {
  task?: Task
  editingTitle: boolean
  titleDraft: string
  updatePending: boolean
  onTitleChange: (value: string) => void
  onTitleKeyDown: (event: KeyboardEvent<HTMLInputElement>) => void
  onSaveTitle: () => void
  onCancelTitle: () => void
  onEditTitle: () => void
  onOpenFullPage: () => void
  onClose: () => void
}

export function TaskDetailHeader({
  task,
  editingTitle,
  titleDraft,
  updatePending,
  onTitleChange,
  onTitleKeyDown,
  onSaveTitle,
  onCancelTitle,
  onEditTitle,
  onOpenFullPage,
  onClose,
}: TaskDetailHeaderProps) {
  const colors = task ? statusColors[task.status] : undefined

  return (
    <header className="flex shrink-0 items-center justify-between border-b px-6 py-4">
      <div className="min-w-0 flex-1 space-y-2 pr-4">
        <div className="flex items-center gap-3">
          {task ? (
            <span
              className={cn(
                'inline-flex items-center gap-1.5 rounded-md px-2 py-1 text-xs font-medium',
                colors?.bg ?? 'bg-muted',
                colors?.text ?? 'text-muted-foreground',
              )}
            >
              <span className={cn('h-1.5 w-1.5 rounded-full', colors?.dot ?? 'bg-muted-foreground')} />
              {task.status.replace('_', ' ')}
            </span>
          ) : (
            <Skeleton className="h-6 w-32" />
          )}
          {task ? (
            <>
              {task.review_passed_at ? (
                <span className="inline-flex items-center rounded-md border border-emerald-200 bg-emerald-50 px-2 py-1 text-xs font-medium text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950 dark:text-emerald-300">
                  review passed
                </span>
              ) : null}
              <p className="truncate text-xs text-muted-foreground">{task.id}</p>
            </>
          ) : (
            <Skeleton className="h-4 w-48" />
          )}
        </div>

        {task ? (
          editingTitle ? (
            <div className="space-y-2">
              <Input
                autoFocus
                className="text-lg font-semibold"
                value={titleDraft}
                onChange={(e) => onTitleChange(e.target.value)}
                onKeyDown={onTitleKeyDown}
              />
              <div className="flex items-center gap-2">
                <Button
                  size="sm"
                  disabled={updatePending || !titleDraft.trim()}
                  onClick={onSaveTitle}
                >
                  Save
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={onCancelTitle}
                >
                  Cancel
                </Button>
              </div>
            </div>
          ) : (
            <button
              className="block w-full rounded-md p-1 text-left text-lg font-semibold leading-snug hover:bg-accent"
              type="button"
              onClick={onEditTitle}
            >
              {task.title}
            </button>
          )
        ) : null}
        {task ? <TaskCapacityNotice task={task} /> : null}
      </div>

      <div className="flex items-center gap-1">
        <button
          type="button"
          className="flex h-8 w-8 cursor-pointer items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          title="Open full page"
          onClick={onOpenFullPage}
        >
          <ArrowSquareOut size={16} />
        </button>
        <button
          type="button"
          className="flex h-8 w-8 cursor-pointer items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          onClick={onClose}
        >
          <X size={16} />
        </button>
      </div>
    </header>
  )
}

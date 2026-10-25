// Project hook form conversion, validation, and display utilities.

import { ApiError } from '@/api/client'
import { getApiErrorMessage } from '@/lib/api-error'
import type { TaskType } from '@/types/generated'
import type { ProjectHookAction } from '@/types/generated/bindings/ProjectHookAction'
import type { ProjectHookRule } from '@/types/generated/bindings/ProjectHookRule'
import type { ProjectHookRunResponse } from '@/types/generated/bindings/ProjectHookRunResponse'
import type { ProjectHookRunStatus } from '@/types/generated/bindings/ProjectHookRunStatus'

type ProjectHookActionType = ProjectHookAction['type']

type ProjectHookTriggerType = ProjectHookRule['trigger']['type']

type PriorityOption = '' | 'low' | 'medium' | 'high' | 'critical'

type SeverityOption = '' | 'info' | 'warning' | 'error'

export type ProjectHookRuleFormState = {
  id: string
  name: string
  enabled: boolean
  triggerType: ProjectHookTriggerType
  actionType: ProjectHookActionType
  dispatchAgentId: string
  dispatchPrompt: string
  dispatchFollowUp: unknown
  createTaskTitle: string
  createTaskDescription: string
  createTaskPriority: PriorityOption
  createTaskType: TaskType | null
  addCommentTargetTaskId: string
  addCommentContent: string
  notifyTitle: string
  notifyMessage: string
  notifySeverity: SeverityOption
  cooldownSeconds: string
  maxConcurrentRuns: string
  filters: unknown
}

type ProjectHookActionWire =
  | { type: 'dispatch_agent'; agent_id: string; prompt: string | null; follow_up: unknown }
  | {
      type: 'create_task'
      title: string
      description: string | null
      task_type: TaskType | null
      priority: number | null
    }
  | { type: 'add_comment'; target_task_id: string | null; content: string }
  | { type: 'notify'; title: string; message: string; severity: string | null }

export type ProjectHookRuleWire = {
  id: string
  enabled: boolean
  name: string
  trigger: { type: ProjectHookTriggerType }
  filters: unknown
  action: ProjectHookActionWire
  cooldown_seconds: number | null
  max_concurrent_runs: number
}

export type DialogMode = { type: 'add' } | { type: 'edit'; index: number }

export const ACTION_OPTIONS: Array<{ value: ProjectHookActionType; label: string }> = [
  { value: 'dispatch_agent', label: 'dispatch_agent' },
  { value: 'create_task', label: 'create_task' },
  { value: 'add_comment', label: 'add_comment' },
  { value: 'notify', label: 'notify' },
]

export const TRIGGER_OPTIONS: Array<{ value: ProjectHookTriggerType; label: string }> = [
  { value: 'project.all_work_completed', label: 'project.all_work_completed' },
]

export const PRIORITY_OPTIONS: Array<{ value: PriorityOption; label: string }> = [
  { value: '', label: 'None' },
  { value: 'low', label: 'Low' },
  { value: 'medium', label: 'Medium' },
  { value: 'high', label: 'High' },
  { value: 'critical', label: 'Critical' },
]

export const SEVERITY_OPTIONS: Array<{ value: SeverityOption; label: string }> = [
  { value: '', label: 'None' },
  { value: 'info', label: 'Info' },
  { value: 'warning', label: 'Warning' },
  { value: 'error', label: 'Error' },
]

const PRIORITY_VALUES: Record<Exclude<PriorityOption, ''>, number> = {
  low: 0,
  medium: 1,
  high: 2,
  critical: 3,
}

export const RUN_STATUS_CLASSES: Record<ProjectHookRunStatus, string> = {
  queued: 'border-muted-foreground/30 bg-muted text-muted-foreground',
  running: 'border-blue-500/30 bg-blue-500/10 text-blue-300',
  dispatched: 'border-violet-500/30 bg-violet-500/10 text-violet-300',
  skipped: 'border-amber-500/30 bg-amber-500/10 text-amber-300',
  failed: 'border-red-500/30 bg-red-500/10 text-red-300',
  completed: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300',
}

export const RUN_STATUS_DOT: Record<ProjectHookRunStatus, string> = {
  queued: 'bg-muted-foreground/60',
  running: 'bg-blue-400',
  dispatched: 'bg-violet-400',
  skipped: 'bg-amber-400',
  failed: 'bg-red-400',
  completed: 'bg-emerald-400',
}

export function emptyRuleForm(): ProjectHookRuleFormState {
  return {
    id: '',
    name: '',
    enabled: false,
    triggerType: 'project.all_work_completed',
    actionType: 'dispatch_agent',
    dispatchAgentId: '',
    dispatchPrompt: '',
    dispatchFollowUp: null,
    createTaskTitle: '',
    createTaskDescription: '',
    createTaskPriority: '',
    createTaskType: null,
    addCommentTargetTaskId: '',
    addCommentContent: '',
    notifyTitle: '',
    notifyMessage: '',
    notifySeverity: '',
    cooldownSeconds: '',
    maxConcurrentRuns: '1',
    filters: null,
  }
}

export function compactId(value: string | null | undefined): string {
  if (!value) return '-'
  return value.length > 12 ? `${value.slice(0, 8)}...` : value
}

export function formatRelativeTime(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  const diffMs = date.getTime() - Date.now()
  const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' })
  const seconds = Math.round(diffMs / 1000)
  if (Math.abs(seconds) < 60) return rtf.format(seconds, 'second')
  const minutes = Math.round(seconds / 60)
  if (Math.abs(minutes) < 60) return rtf.format(minutes, 'minute')
  const hours = Math.round(minutes / 60)
  if (Math.abs(hours) < 24) return rtf.format(hours, 'hour')
  return rtf.format(Math.round(hours / 24), 'day')
}

export function formatDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString()
}

export function formatCooldown(seconds: bigint | number | null | undefined): string {
  const parsed = numberOrNull(seconds)
  if (parsed === null || parsed === 0) return 'no cooldown'
  if (parsed < 60) return `${parsed}s`
  if (parsed < 3600) return `${Math.round(parsed / 60)}m`
  if (parsed < 86400) return `${Math.round(parsed / 3600)}h`
  return `${Math.round(parsed / 86400)}d`
}

export function shortTrigger(triggerType: string): string {
  const last = triggerType.split('.').pop() ?? triggerType
  return last.replace(/_/g, ' ')
}

export function actionSummary(
  action: ProjectHookRule['action'],
  agentNames?: Map<string, string>,
): string {
  switch (action.type) {
    case 'dispatch_agent': {
      const prompt = action.prompt?.trim()
      const suffix = prompt
        ? ` with prompt "${prompt.length > 60 ? `${prompt.slice(0, 57)}...` : prompt}"`
        : ''
      const agentLabel = agentNames?.get(action.agent_id) ?? action.agent_id
      return `Dispatch ${agentLabel}${suffix}`
    }
    case 'create_task':
      return `Create task "${action.title}"`
    case 'add_comment': {
      const target = action.target_task_id ? compactId(action.target_task_id) : 'source task'
      return `Comment on ${target}`
    }
    case 'notify':
      return `Notify: ${action.title}`
  }
}

function numberOrNull(value: bigint | number | null | undefined): number | null {
  if (value === null || value === undefined) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function priorityOptionFromValue(value: bigint | number | null | undefined): PriorityOption {
  const parsed = numberOrNull(value)
  if (parsed === 0) return 'low'
  if (parsed === 1) return 'medium'
  if (parsed === 2) return 'high'
  if (parsed === 3) return 'critical'
  return ''
}

export function isActionType(value: string): value is ProjectHookActionType {
  return ACTION_OPTIONS.some((option) => option.value === value)
}

export function isPriorityOption(value: string): value is PriorityOption {
  return PRIORITY_OPTIONS.some((option) => option.value === value)
}

export function isSeverityOption(value: string): value is SeverityOption {
  return SEVERITY_OPTIONS.some((option) => option.value === value)
}

export function ruleToWire(rule: ProjectHookRule): ProjectHookRuleWire {
  const base = {
    id: rule.id,
    enabled: rule.enabled,
    name: rule.name,
    trigger: rule.trigger,
    filters: rule.filters ?? null,
    cooldown_seconds: numberOrNull(rule.cooldown_seconds),
    max_concurrent_runs: rule.max_concurrent_runs,
  }

  switch (rule.action.type) {
    case 'dispatch_agent':
      return {
        ...base,
        action: {
          type: 'dispatch_agent',
          agent_id: rule.action.agent_id,
          prompt: rule.action.prompt,
          follow_up: rule.action.follow_up ?? null,
        },
      }
    case 'create_task':
      return {
        ...base,
        action: {
          type: 'create_task',
          title: rule.action.title,
          description: rule.action.description,
          task_type: rule.action.task_type,
          priority: numberOrNull(rule.action.priority),
        },
      }
    case 'add_comment':
      return {
        ...base,
        action: {
          type: 'add_comment',
          target_task_id: rule.action.target_task_id,
          content: rule.action.content,
        },
      }
    case 'notify':
      return {
        ...base,
        action: {
          type: 'notify',
          title: rule.action.title,
          message: rule.action.message,
          severity: rule.action.severity,
        },
      }
  }
}

export function formFromRule(rule: ProjectHookRule): ProjectHookRuleFormState {
  const form = {
    ...emptyRuleForm(),
    id: rule.id,
    name: rule.name,
    enabled: rule.enabled,
    triggerType: rule.trigger.type,
    cooldownSeconds: rule.cooldown_seconds == null ? '' : String(rule.cooldown_seconds),
    maxConcurrentRuns: String(rule.max_concurrent_runs),
    filters: rule.filters ?? null,
  }

  switch (rule.action.type) {
    case 'dispatch_agent':
      return {
        ...form,
        actionType: 'dispatch_agent',
        dispatchAgentId: rule.action.agent_id,
        dispatchPrompt: rule.action.prompt ?? '',
        dispatchFollowUp: rule.action.follow_up ?? null,
      }
    case 'create_task':
      return {
        ...form,
        actionType: 'create_task',
        createTaskTitle: rule.action.title,
        createTaskDescription: rule.action.description ?? '',
        createTaskPriority: priorityOptionFromValue(rule.action.priority),
        createTaskType: rule.action.task_type,
      }
    case 'add_comment':
      return {
        ...form,
        actionType: 'add_comment',
        addCommentTargetTaskId: rule.action.target_task_id ?? '',
        addCommentContent: rule.action.content,
      }
    case 'notify': {
      const severityRaw = rule.action.severity ?? ''
      const notifySeverity: SeverityOption = isSeverityOption(severityRaw) ? severityRaw : ''

      return {
        ...form,
        actionType: 'notify',
        notifyTitle: rule.action.title,
        notifyMessage: rule.action.message,
        notifySeverity,
      }
    }
  }
}

export function buildRule(form: ProjectHookRuleFormState): ProjectHookRuleWire | string {
  const id = form.id.trim()
  if (!id) return 'Project hook rule id is required'

  const name = form.name.trim()
  if (!name) return 'Project hook rule name is required'

  const cooldownInput = form.cooldownSeconds.trim()
  const cooldownSeconds =
    cooldownInput === '' ? null : Number.parseInt(cooldownInput, 10)
  if (
    cooldownSeconds !== null &&
    (!Number.isInteger(cooldownSeconds) || cooldownSeconds < 0)
  ) {
    return 'Cooldown seconds must be 0 or greater'
  }

  const maxConcurrentInput = form.maxConcurrentRuns.trim() || '1'
  const maxConcurrentRuns = Number.parseInt(maxConcurrentInput, 10)
  if (!Number.isInteger(maxConcurrentRuns) || maxConcurrentRuns < 1 || maxConcurrentRuns > 10) {
    return 'Max concurrent runs must be between 1 and 10'
  }

  let action: ProjectHookActionWire
  switch (form.actionType) {
    case 'dispatch_agent': {
      const agentId = form.dispatchAgentId.trim()
      if (!agentId) return 'dispatch_agent.agent_id is required'
      action = {
        type: 'dispatch_agent',
        agent_id: agentId,
        prompt: form.dispatchPrompt.trim() || null,
        follow_up: form.dispatchFollowUp ?? null,
      }
      break
    }
    case 'create_task': {
      const title = form.createTaskTitle.trim()
      if (!title) return 'create_task.title is required'
      action = {
        type: 'create_task',
        title,
        description: form.createTaskDescription.trim() || null,
        task_type: form.createTaskType,
        priority: form.createTaskPriority ? PRIORITY_VALUES[form.createTaskPriority] : null,
      }
      break
    }
    case 'add_comment': {
      const content = form.addCommentContent.trim()
      if (!content) return 'add_comment.content is required'
      action = {
        type: 'add_comment',
        target_task_id: form.addCommentTargetTaskId.trim() || null,
        content,
      }
      break
    }
    case 'notify': {
      const title = form.notifyTitle.trim()
      const message = form.notifyMessage.trim()
      if (!title) return 'notify.title is required'
      if (!message) return 'notify.message is required'
      action = {
        type: 'notify',
        title,
        message,
        severity: form.notifySeverity || null,
      }
      break
    }
  }

  return {
    id,
    enabled: form.enabled,
    name,
    trigger: { type: form.triggerType },
    filters: form.filters ?? null,
    action,
    cooldown_seconds: cooldownSeconds,
    max_concurrent_runs: maxConcurrentRuns,
  }
}

export function serverErrorMessage(error: unknown, fallback = 'Project hook update failed'): string {
  if (error instanceof ApiError) {
    if (error.status === 400) {
      try {
        const parsed = JSON.parse(error.message) as { message?: unknown }
        if (typeof parsed.message === 'string' && parsed.message) return parsed.message
      } catch {
        return error.message || fallback
      }
      return error.message || fallback
    }
    return getApiErrorMessage(error, fallback)
  }
  if (error instanceof Error) return error.message
  return fallback
}

type RuleRunStats = {
  lastRun: ProjectHookRunResponse | null
  total: number
  completed: number
  failed: number
}

export function computeRunStats(
  runs: ProjectHookRunResponse[],
): Map<string, RuleRunStats> {
  const stats = new Map<string, RuleRunStats>()
  for (const run of runs) {
    const existing = stats.get(run.rule_id) ?? {
      lastRun: null,
      total: 0,
      completed: 0,
      failed: 0,
    }
    existing.total += 1
    if (run.status === 'completed') existing.completed += 1
    if (run.status === 'failed') existing.failed += 1
    if (!existing.lastRun) existing.lastRun = run
    stats.set(run.rule_id, existing)
  }
  return stats
}

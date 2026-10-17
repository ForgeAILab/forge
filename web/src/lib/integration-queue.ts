import type {
  IntegrationPhase,
  IntegrationWait,
  TaskIntegrationSummary,
} from '@/types/generated'

/**
 * Words for a Task's place in the merge queue. Everything here reads the
 * server's `integration` summary: no workflow state name is consulted, so a
 * custom workflow's merge and repair states read the same.
 *
 * The summary's enums are open: a newer server sends states, phases, wait
 * codes and hints this build has no words for. Each of them reads neutrally
 * ("In merge queue", the server's own message) and none throws or prints
 * `undefined`.
 */

const PHASE_TEXT: Partial<Record<string, string>> & Record<IntegrationPhase, string> = {
  validating: 'validating',
  rebasing: 'rebasing',
  checking: 'checking',
  awaiting_carry: 'carrying its review',
  awaiting_authorization: 'authorizing',
  fast_forwarding: 'fast-forwarding',
  reconciling: 'confirming the result',
}

function machineName(wait: IntegrationWait): string {
  return wait.machine_name ?? wait.machine_id ?? 'its machine'
}

/** The cause of a wait, as the short phrase that follows "Waiting". */
export function integrationWaitText(wait: IntegrationWait): string {
  switch (wait.code) {
    case 'queue':
      return 'others are ahead'
    case 'queue_suspended':
      return 'the target checkout is not ready'
    case 'queue_quarantined':
      return 'another merge has an unknown result'
    case 'task_held':
      return 'held'
    case 'task_blocked':
      return 'blocked'
    case 'task_failed':
      return 'failed'
    case 'task_entry_blocked':
      return 'entry blocked'
    case 'task_awaiting_human':
      return "a person's decision"
    case 'project_paused':
      return 'project paused'
    case 'owner_offline':
      return `for ${machineName(wait)}`
    case 'target_dirty':
      return 'the target checkout has local changes'
    case 'infrastructure':
      return 'retrying after a failure'
    case 'budget_exhausted':
      return 'retries spent'
    case 'owner_required':
      return 'the owner must decide'
    case 'unresolved_result':
      return `merge result unknown on ${machineName(wait)}`
    case 'cancel_requested':
      return 'cancelling'
    default:
      // A cause this build does not know: the server's message says it.
      return wait.message?.trim() || 'see the Task for details'
  }
}

/** One short label: "In merge queue (2 ahead)", "Merging: rebasing", ... */
export function integrationLabel(summary: TaskIntegrationSummary): string {
  switch (summary.state) {
    case 'waiting': {
      const wait = summary.wait
      if (wait && wait.code === 'owner_offline') return `Waiting for ${machineName(wait)}`
      if (wait && wait.code !== 'queue') return `Waiting: ${integrationWaitText(wait)}`
      if (summary.position && summary.position > 0) {
        return `In merge queue (${summary.position} ahead)`
      }
      return summary.position === 0 || !wait ? 'In merge queue (next)' : 'In merge queue'
    }
    case 'owned':
      return `Merging: ${summary.phase ? (PHASE_TEXT[summary.phase] ?? 'in progress') : 'starting'}`
    case 'repair':
      return 'Fix merge'
    case 'review_required':
      return 'Merge needs a fresh review'
    case 'candidate_check_failed':
      return 'Merge check failed'
    case 'deferred': {
      const wait = summary.wait
      if (wait && wait.code === 'owner_offline') return `Waiting for ${machineName(wait)}`
      return wait ? `Merge stopped: ${integrationWaitText(wait)}` : 'Merge stopped'
    }
    case 'applied':
      return 'Merged'
    default:
      return 'In merge queue'
  }
}

export type IntegrationTone = 'progress' | 'waiting' | 'attention'

/** Whether the label reads as work in progress, a wait, or something to act on. */
export function integrationTone(summary: TaskIntegrationSummary): IntegrationTone {
  switch (summary.state) {
    case 'owned':
    case 'applied':
      return 'progress'
    case 'waiting':
      return summary.action_hint ? 'attention' : 'waiting'
    case 'repair':
    case 'review_required':
    case 'candidate_check_failed':
    case 'deferred':
      return 'attention'
    default:
      return summary.action_hint ? 'attention' : 'waiting'
  }
}

const ACTION_TEXT: Partial<Record<string, string>> = {
  retry: 'Retry the merge to put the Task back in line.',
  release: 'Release the Task to continue the merge.',
  resume_project: 'Resume the Project to continue the merge.',
}

/** A server message as a sentence of the notice. */
function sentence(text: string): string {
  const trimmed = text.trim()
  if (!trimmed) return trimmed
  const capitalised = trimmed.charAt(0).toUpperCase() + trimmed.slice(1)
  return /[.!?]$/.test(capitalised) ? capitalised : `${capitalised}.`
}

/** The Task detail notice: the label, then what is known and what moves it. */
export function integrationNotice(
  summary: TaskIntegrationSummary,
): { title: string; message: string; tone: IntegrationTone } {
  const parts: string[] = []
  if (summary.target_branch) parts.push(`Target branch ${summary.target_branch}.`)
  switch (summary.state) {
    case 'waiting':
      if (!summary.wait || summary.wait.code === 'queue') {
        parts.push(
          summary.position && summary.position > 0
            ? `${summary.position} ${summary.position === 1 ? 'Task merges' : 'Tasks merge'} before this one.`
            : 'This Task merges next.',
        )
      }
      break
    case 'owned':
      parts.push('Forge is merging this change; nothing is needed from you.')
      break
    case 'repair':
      parts.push('The change was sent back to its coder to repair the merge. It keeps its place in line.')
      break
    case 'review_required':
      parts.push('The rebased change needs a fresh review before it can merge.')
      break
    case 'candidate_check_failed':
      parts.push('The checks of the rebased change failed. It was sent back to its coder.')
      break
    default:
      break
  }
  // An unknown wait code already reads as the server's message in the title.
  const titled = integrationLabel(summary)
  if (summary.wait?.message && !titled.includes(summary.wait.message.trim())) {
    parts.push(sentence(summary.wait.message))
  }
  if (summary.wait?.retry_at) {
    parts.push(`Forge retries at ${new Date(summary.wait.retry_at).toLocaleTimeString()}.`)
  }
  const action = summary.action_hint ? ACTION_TEXT[summary.action_hint] : undefined
  if (action) parts.push(action)
  return { title: titled, message: parts.join(' '), tone: integrationTone(summary) }
}

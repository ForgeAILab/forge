import { useRef } from 'react'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { useResolveAttentionMutation } from '@/features/federation/hooks'
import type { AttentionItem } from '@/features/federation/types'
import { ApiError } from '@/api/client'
import { getApiErrorMessage } from '@/lib/api-error'

/** AttentionItem.category is the public projection of stored attention_type. */
export function isLeadInput(item: AttentionItem): boolean {
  return item.category === 'human_input_required' && item.recommended_action === 'answer_lead_input'
}

export function leadQuestion(item: AttentionItem): string | null {
  return typeof item.details.question === 'string' && item.details.question.trim()
    ? item.details.question
    : null
}

/**
 * Why an answer was refused. A 409 is a stale item version (another tab, or
 * the question was withdrawn with its root): the server's text for it names
 * neither, so the form says what happened and that the draft is kept.
 */
function submitErrorMessage(error: unknown): string {
  if (error instanceof ApiError && error.status === 409) {
    return 'This question changed while you were answering. Your answer is kept: check the question, then send it again.'
  }
  return getApiErrorMessage(error, 'Could not answer the lead')
}

/** Resolving a lead question requires the owner's answer at this item's version. */
export function LeadInputAnswer({
  item,
  answer,
  onAnswerChange,
  onAnswered,
}: {
  item: AttentionItem
  answer: string
  onAnswerChange: (answer: string) => void
  onAnswered: () => void
}) {
  const submit = useResolveAttentionMutation()
  // The pending flag reaches this component a render later than a second submit can.
  const sending = useRef(false)
  const inputId = `lead-answer-${item.id}`
  const helpId = `${inputId}-help`
  const errorId = `${inputId}-error`
  const submitErrorId = `${inputId}-submit-error`
  // The server trims the answer and counts Unicode scalar values, as Rust chars().
  const length = Array.from(answer.trim()).length
  const invalid = length === 0 || length > 4000
  const validation =
    length > 4000
      ? 'Keep your answer to 4000 characters or fewer.'
      : answer.length > 0 && length === 0
        ? 'Enter an answer with at least one non-space character.'
        : null
  const question = leadQuestion(item)

  return (
    <div className="mt-3 space-y-2">
      <p className="whitespace-pre-wrap break-words text-sm leading-5 text-foreground">
        {question ?? 'The lead’s question is unavailable. Refresh Mission Control.'}
      </p>
      {item.lifecycle !== 'resolved' ? (
        <form
          className="space-y-2"
          onSubmit={(event) => {
            event.preventDefault()
            if (invalid || sending.current || submit.isPending || submit.isSuccess || !question) {
              return
            }
            sending.current = true
            submit.mutate(
              { id: item.id, expectedVersion: item.version, answer },
              {
                onSuccess: onAnswered,
                onSettled: () => {
                  sending.current = false
                },
              },
            )
          }}
        >
          <label htmlFor={inputId} className="block text-xs font-medium text-foreground">
            Answer the lead
          </label>
          <Textarea
            id={inputId}
            required
            value={answer}
            disabled={submit.isPending || submit.isSuccess || !question}
            aria-invalid={Boolean(validation)}
            aria-describedby={`${helpId}${validation ? ` ${errorId}` : ''}${submit.error ? ` ${submitErrorId}` : ''}`}
            onChange={(event) => onAnswerChange(event.target.value)}
          />
          <p id={helpId} className="text-xs text-muted-foreground">
            1–4000 characters · {length}/4000
          </p>
          {validation ? (
            <p id={errorId} className="text-xs text-destructive" role="alert">
              {validation}
            </p>
          ) : null}
          <Button
            type="submit"
            variant="outline"
            size="sm"
            disabled={invalid || submit.isPending || submit.isSuccess || !question}
          >
            {submit.isPending ? 'Resolving…' : submit.isSuccess ? 'Answered' : 'Resolve'}
          </Button>
          {submit.error ? (
            <p id={submitErrorId} className="text-xs text-destructive" role="alert">
              {submitErrorMessage(submit.error)}
            </p>
          ) : null}
        </form>
      ) : null}
    </div>
  )
}

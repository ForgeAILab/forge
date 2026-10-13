import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { useResolveAttentionMutation } from '@/features/federation/hooks'
import type { AttentionItem } from '@/features/federation/types'
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

/** Resolving a lead question requires the owner's answer at this item's version. */
export function LeadInputAnswer({ item }: { item: AttentionItem }) {
  const [answer, setAnswer] = useState('')
  const submit = useResolveAttentionMutation()
  const inputId = `lead-answer-${item.id}`
  const helpId = `${inputId}-help`
  const errorId = `${inputId}-error`
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
            if (invalid || submit.isPending || submit.isSuccess || !question) return
            submit.mutate({ id: item.id, expectedVersion: item.version, answer })
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
            aria-describedby={`${helpId}${validation ? ` ${errorId}` : ''}`}
            onChange={(event) => setAnswer(event.target.value)}
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
            <p className="text-xs text-destructive" role="alert">
              {getApiErrorMessage(submit.error, 'Could not answer the lead')}
            </p>
          ) : null}
        </form>
      ) : null}
    </div>
  )
}

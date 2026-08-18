import { ChatEntryContainer } from '@/components/chat/chat-entry-container'
import { ChatMarkdown } from '@/components/chat/chat-markdown'
import type { ChatAssistantEntry } from '@/components/chat/types'

type ChatAssistantMessageProps = {
  entry: ChatAssistantEntry
}

export function ChatAssistantMessage({ entry }: ChatAssistantMessageProps) {
  const text = typeof entry.text === 'string' ? entry.text : JSON.stringify(entry.text)

  return (
    <ChatEntryContainer variant="assistant" header="Assistant" defaultCollapsed={false}>
      <div className="prose prose-sm max-w-none dark:prose-invert">
        <ChatMarkdown text={text} />
      </div>
    </ChatEntryContainer>
  )
}

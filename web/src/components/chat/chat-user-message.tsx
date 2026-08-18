import { ChatEntryContainer } from '@/components/chat/chat-entry-container'
import { ChatMarkdown } from '@/components/chat/chat-markdown'
import type { ChatUserEntry } from '@/components/chat/types'

type ChatUserMessageProps = {
  entry: ChatUserEntry
}

export function ChatUserMessage({ entry }: ChatUserMessageProps) {
  const text = typeof entry.text === 'string' ? entry.text : JSON.stringify(entry.text)

  return (
    <ChatEntryContainer variant="user" header="User" defaultCollapsed={false}>
      <div className="prose prose-sm max-w-none dark:prose-invert">
        <ChatMarkdown text={text} />
      </div>
    </ChatEntryContainer>
  )
}

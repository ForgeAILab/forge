import { ChatEntryContainer } from '@/components/chat/chat-entry-container'
import type { ChatThinkingEntry } from '@/components/chat/types'

type ChatThinkingProps = {
  entry: ChatThinkingEntry
}

export function ChatThinking({ entry }: ChatThinkingProps) {
  return (
    <ChatEntryContainer
      variant="session"
      header={entry.isStreaming ? 'Thinking…' : 'Thinking'}
      defaultCollapsed={!entry.isStreaming}
    >
      <div className="max-h-96 overflow-auto whitespace-pre-wrap p-3 text-xs italic text-muted-foreground">
        {entry.text}
      </div>
    </ChatEntryContainer>
  )
}

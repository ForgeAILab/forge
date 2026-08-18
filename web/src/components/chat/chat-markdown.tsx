import { lazy, Suspense, type ComponentType } from 'react'
import ReactMarkdown, { type Components } from 'react-markdown'
import { type SyntaxHighlighterProps } from 'react-syntax-highlighter'
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism'
import rehypeSanitize from 'rehype-sanitize'
import remarkGfm from 'remark-gfm'

import { cn } from '@/lib/cn'

const SyntaxHighlighter = lazy(async () => {
  const module = await import('react-syntax-highlighter')
  return { default: module.Prism as ComponentType<SyntaxHighlighterProps> }
})

const markdownComponents: Components = {
  code({ className, children, ...props }) {
    const match = /language-(\w+)/.exec(className ?? '')
    const code = String(children).replace(/\n$/, '')

    if (!match) {
      return (
        <code className={cn('rounded-sm bg-muted px-1 py-0.5 font-mono text-[0.85em]', className)} {...props}>
          {children}
        </code>
      )
    }

    return (
      <Suspense fallback={<pre className="overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs">{code}</pre>}>
        <SyntaxHighlighter
          language={match[1]}
          style={oneDark}
          customStyle={{ margin: 0, borderRadius: '6px' }}
          codeTagProps={{ className: 'text-xs' }}
          wrapLongLines
        >
          {code}
        </SyntaxHighlighter>
      </Suspense>
    )
  },
}

export function ChatMarkdown({ text }: { text: string }) {
  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      rehypePlugins={[rehypeSanitize]}
      components={markdownComponents}
    >
      {text}
    </ReactMarkdown>
  )
}

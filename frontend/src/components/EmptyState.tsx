import { Inbox } from 'lucide-react'
import { cn } from '@/lib/utils'

export interface EmptyStateProps {
  message?: string
  hint?: string
  className?: string
}

export function EmptyState({
  message = 'No matches in this filter',
  hint = 'Pick another event.',
  className,
}: EmptyStateProps) {
  return (
    <div className={cn('flex flex-col items-center justify-center gap-2 px-4 py-10 text-center', className)}>
      <Inbox className="size-5 text-muted-foreground" aria-hidden />
      <p className="text-sm font-medium text-foreground">{message}</p>
      {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
    </div>
  )
}

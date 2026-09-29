import { AlertCircle } from 'lucide-react'
import type { ApiError } from '@/hooks/useApi'
import { cn } from '@/lib/utils'

export interface ErrorCardProps {
  error: ApiError
  /** What failed to load, e.g. "Team profile". */
  title?: string
  className?: string
}

/** Per-card error: one failed request never blanks the page. */
export function ErrorCard({ error, title = 'This section', className }: ErrorCardProps) {
  return (
    <div role="alert" className={cn('flex gap-3 rounded-xl border border-down/25 bg-down/[0.06] p-4 text-sm', className)}>
      <AlertCircle className="mt-0.5 size-4 shrink-0 text-down" aria-hidden />
      <div className="min-w-0">
        <p className="font-medium text-foreground">{title} did not load</p>
        <p className="mt-0.5 break-words text-muted-foreground">
          {error.status ? `${error.status} · ` : ''}
          {error.message}
        </p>
      </div>
    </div>
  )
}

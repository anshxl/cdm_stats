import type { ReactNode } from 'react'
import { cn } from '@/lib/utils'

export interface CardProps {
  title?: ReactNode
  /** Right side of the header, e.g. a toggle. */
  action?: ReactNode
  /** Dim the body while a refetch runs. */
  busy?: boolean
  /** No body padding: for edge-to-edge content such as MapTable. */
  flush?: boolean
  className?: string
  bodyClassName?: string
  children: ReactNode
}

export function Card({ title, action, busy, flush, className, bodyClassName, children }: CardProps) {
  return (
    <section className={cn('rounded-xl border border-line bg-surface', className)}>
      {(title || action) && (
        <header className="flex min-h-12 items-center justify-between gap-3 border-b border-line px-4 py-2.5">
          {title && <h2 className="text-sm font-semibold text-foreground">{title}</h2>}
          {action}
        </header>
      )}
      <div
        className={cn('transition-opacity', flush ? 'overflow-hidden rounded-b-xl' : 'p-4', busy && 'opacity-60', bodyClassName)}
        aria-busy={busy || undefined}
      >
        {children}
      </div>
    </section>
  )
}

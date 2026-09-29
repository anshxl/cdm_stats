import type { ReactNode } from 'react'
import type { Flag } from '@/api/models'
import { cn } from '@/lib/utils'
import { FlagPill } from './FlagPill'

export interface StatTileProps {
  label: string
  value: ReactNode
  /** Small text under the value, e.g. "36–26 maps". */
  sub?: ReactNode
  flag?: Flag | null
  /** Sample size, shown as "n=…". */
  n?: number
  className?: string
}

export function StatTile({ label, value, sub, flag, n, className }: StatTileProps) {
  return (
    <div className={cn('flex min-w-0 flex-col gap-1.5 rounded-xl border border-line bg-surface px-4 py-3.5', className)}>
      <div className="flex items-center justify-between gap-2">
        <span className="truncate text-[13px] text-muted-foreground">{label}</span>
        {n !== undefined && <span className="shrink-0 text-[11px] text-muted-foreground/80">n={n}</span>}
      </div>
      <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1">
        <span className="text-2xl leading-8 font-semibold tracking-tight text-foreground">{value}</span>
        {flag && <FlagPill flag={flag} />}
      </div>
      {sub && <div className="text-xs text-muted-foreground">{sub}</div>}
    </div>
  )
}

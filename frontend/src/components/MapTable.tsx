import { Fragment, useState, type ReactNode } from 'react'
import { ChevronRight } from 'lucide-react'
import type { Flag, Tag } from '@/api/models'
import { pct } from '@/lib/format'
import { cn } from '@/lib/utils'
import { FlagPill } from './FlagPill'

/** Samples below this are dimmed and marked "low sample" (project rule: n < 4). */
export const MIN_N = 4

export interface MapTableColumn<R> {
  key: string
  header: ReactNode
  render: (row: R) => ReactNode
  align?: 'left' | 'right'
  /** Hide below the sm breakpoint to keep phones readable. */
  hideOnMobile?: boolean
}

export interface MapTableProps<R> {
  rows: R[]
  rowKey: (row: R) => string
  mapName: (row: R) => ReactNode
  /** Value columns between the map name and the win-rate bar. */
  columns?: MapTableColumn<R>[]
  /** Drives the win-rate bar and the low-sample rule. */
  winRate: (row: R) => { value: number | null; n: number }
  /** Header over the bar column. Default "Win rate". */
  winRateLabel?: string
  tags?: (row: R) => Tag[]
  flag?: (row: R) => Flag | null | undefined
  /** When set, rows expand to show this content. */
  renderDetail?: (row: R) => ReactNode
  className?: string
}

function WinBar({ value }: { value: number | null }) {
  return (
    <div className="flex items-center gap-2.5">
      <div className="relative h-1.5 w-12 overflow-hidden rounded-full bg-white/[0.06] sm:w-24" aria-hidden>
        {value != null && (
          <div className="absolute inset-y-0 left-0 rounded-full bg-foreground/70" style={{ width: `${value * 100}%` }} />
        )}
        <div className="absolute inset-y-0 left-1/2 w-px bg-page" />
      </div>
      <span className="w-10 text-right text-[13px] font-medium text-foreground">{pct(value)}</span>
    </div>
  )
}

/** Tags, then a non-low-sample flag (low sample is already shown under the map name). */
function Marks({ tags, flag, className }: { tags: Tag[]; flag: Flag | null | undefined; className?: string }) {
  const showFlag = flag && flag.kind !== 'low_sample'
  if (!tags.length && !showFlag) return null
  return (
    <div className={cn('flex flex-wrap gap-1.5', className)}>
      {tags.map((t) => <FlagPill key={t} tag={t} />)}
      {showFlag && <FlagPill flag={flag} />}
    </div>
  )
}

/** Map rows with value columns, a win-rate bar, tags/flags, and optional expandable detail.
 * Put it in <Card flush> so rows run edge to edge. */
export function MapTable<R>({
  rows,
  rowKey,
  mapName,
  columns = [],
  winRate,
  winRateLabel = 'Win rate',
  tags,
  flag,
  renderDetail,
  className,
}: MapTableProps<R>) {
  const [open, setOpen] = useState<Set<string>>(new Set())
  const toggle = (k: string) =>
    setOpen((prev) => {
      const next = new Set(prev)
      if (next.has(k)) next.delete(k)
      else next.add(k)
      return next
    })

  const hasMarks = Boolean(tags || flag)
  const colCount = 2 + columns.length + (hasMarks ? 1 : 0)
  const th = 'h-9 px-3 text-xs font-medium text-muted-foreground whitespace-nowrap'

  return (
    <div className={cn('overflow-x-auto', className)}>
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="border-b border-line">
            <th scope="col" className={cn(th, 'w-full pl-4 text-left')}>Map</th>
            {columns.map((c) => (
              <th
                key={c.key}
                scope="col"
                className={cn(th, c.align === 'left' ? 'text-left' : 'text-right', c.hideOnMobile && 'hidden sm:table-cell')}
              >
                {c.header}
              </th>
            ))}
            <th scope="col" className={cn(th, 'text-left', hasMarks ? 'max-sm:pr-4' : 'pr-4')}>{winRateLabel}</th>
            {hasMarks && <th scope="col" className={cn(th, 'hidden pr-4 text-left sm:table-cell')}><span className="sr-only">Tags</span></th>}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const k = rowKey(row)
            const wr = winRate(row)
            const low = wr.n < MIN_N
            const expanded = open.has(k)
            const rowTags = tags?.(row) ?? []
            const rowFlag = flag?.(row)
            return (
              <Fragment key={k}>
                <tr
                  className={cn(
                    'border-b border-line/70 last:border-b-0',
                    renderDetail && 'cursor-pointer hover:bg-white/[0.025]',
                    expanded && 'bg-white/[0.025]',
                  )}
                  onClick={renderDetail ? () => toggle(k) : undefined}
                >
                  <td className={cn('py-2.5 pr-3 pl-4', low && 'opacity-55')}>
                    <div className="flex items-center gap-1.5">
                      {renderDetail && (
                        <button
                          type="button"
                          aria-expanded={expanded}
                          aria-label={expanded ? 'Hide details' : 'Show details'}
                          onClick={(e) => {
                            e.stopPropagation()
                            toggle(k)
                          }}
                          className="-ml-1 grid size-5 place-items-center rounded text-muted-foreground hover:text-foreground"
                        >
                          <ChevronRight className={cn('size-3.5 transition-transform', expanded && 'rotate-90')} />
                        </button>
                      )}
                      <div className="min-w-0">
                        <div className="font-medium text-foreground">{mapName(row)}</div>
                        {low && <div className="text-[11px] whitespace-nowrap text-muted-foreground">n={wr.n} · low sample</div>}
                        {hasMarks && <Marks tags={rowTags} flag={rowFlag} className="mt-1 sm:hidden" />}
                      </div>
                    </div>
                  </td>
                  {columns.map((c) => (
                    <td
                      key={c.key}
                      className={cn(
                        'px-3 py-2.5 whitespace-nowrap text-foreground/90',
                        c.align === 'left' ? 'text-left' : 'text-right',
                        c.hideOnMobile && 'hidden sm:table-cell',
                        low && 'opacity-55',
                      )}
                    >
                      {c.render(row)}
                    </td>
                  ))}
                  <td className={cn('px-3 py-2.5', hasMarks ? 'max-sm:pr-4' : 'pr-4', low && 'opacity-55')}>
                    <WinBar value={wr.value} />
                  </td>
                  {hasMarks && (
                    <td className="hidden py-2.5 pr-4 pl-3 sm:table-cell">
                      <Marks tags={rowTags} flag={rowFlag} />
                    </td>
                  )}
                </tr>
                {renderDetail && expanded && (
                  <tr className="border-b border-line/70 bg-white/[0.015]">
                    <td colSpan={colCount} className="px-4 py-3">
                      {renderDetail(row)}
                    </td>
                  </tr>
                )}
              </Fragment>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

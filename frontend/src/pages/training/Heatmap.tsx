import type { Training } from '@/api/models'
import { shortDate } from '@/lib/format'
import { cn } from '@/lib/utils'

// Sequential, one hue: gold opacity by sessions that day (4+ is full).
const STEPS = [0.3, 0.55, 0.8, 1]
const fill = (sessions: number) => `rgb(241 198 27 / ${STEPS[Math.min(sessions, STEPS.length) - 1]})`

export interface HeatmapProps {
  month: string
  days: number
  /** Rows in table order. */
  usernames: string[]
  daily: Training['daily']
  /** Days after this one have not happened yet (current month only). */
  lastDay?: number
}

/** One row per player, one cell per day. Scrolls inside its card on narrow screens. */
export function Heatmap({ month, days, usernames, daily, lastDay = days }: HeatmapProps) {
  const counts = new Map(daily.map((d) => [`${d.username}|${Number(d.date.slice(8, 10))}`, d.sessions]))
  const dayNums = Array.from({ length: days }, (_, i) => i + 1)
  const cols = { gridTemplateColumns: `minmax(88px, 132px) repeat(${days}, minmax(18px, 1fr))` }

  return (
    <div className="flex flex-col gap-3">
      <div className="overflow-x-auto">
        <div className="grid min-w-[680px] gap-[2px]" style={cols} role="grid" aria-label="Sessions per player per day">
          <div aria-hidden className="sticky left-0 z-10 bg-surface" />
          {dayNums.map((d) => (
            <div
              key={d}
              aria-hidden
              className={cn('pb-1 text-center text-[10px] leading-none text-muted-foreground', d > lastDay && 'opacity-40')}
            >
              {d}
            </div>
          ))}
          {usernames.map((u) => (
            <div key={u} role="row" className="contents">
              <div role="rowheader" className="sticky left-0 z-10 truncate bg-surface pr-3 text-[13px] leading-6 text-foreground" title={u}>{u}</div>
              {dayNums.map((d) => {
                const n = counts.get(`${u}|${d}`) ?? 0
                const date = shortDate(`${month}-${String(d).padStart(2, '0')}`)
                const future = d > lastDay
                const text = future ? `${date} · upcoming` : `${date} · ${n} ${n === 1 ? 'session' : 'sessions'}`
                return (
                  <div
                    key={d}
                    role="gridcell"
                    title={text}
                    aria-label={`${u}, ${text}`}
                    className={cn(
                      'h-6 rounded-[3px]',
                      n === 0 && (future ? 'border border-dashed border-white/[0.07]' : 'bg-white/[0.045]'),
                    )}
                    style={n ? { backgroundColor: fill(n) } : undefined}
                  />
                )
              })}
            </div>
          ))}
        </div>
      </div>
      <div className="flex flex-wrap items-center justify-end gap-1.5 text-[11px] whitespace-nowrap text-muted-foreground" aria-hidden>
        <span className="mr-1 hidden sm:inline">Sessions per day</span>
        <span className="size-3 rounded-[3px] bg-white/[0.045]" />
        <span>0</span>
        {STEPS.map((_, i) => (
          <span key={i} className="flex items-center gap-1.5">
            <span className="ml-1 size-3 rounded-[3px]" style={{ backgroundColor: fill(i + 1) }} />
            <span>{i + 1 === STEPS.length ? `${i + 1}+` : i + 1}</span>
          </span>
        ))}
      </div>
    </div>
  )
}

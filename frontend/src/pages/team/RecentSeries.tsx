import { useState } from 'react'
import { ChevronRight } from 'lucide-react'
import type { Players } from '@/api/models'
import { shortDate } from '@/lib/format'
import { cn } from '@/lib/utils'

type Series = Players['recent_series'][number]
type SeriesMap = Series['maps'][number]

const Missing = () => <span className="text-muted-foreground">—</span>

/** Scoreboard and footage are ingested separately, so either side can be missing ("—", never 0). */
function MapPlayers({ m }: { m: SeriesMap }) {
  const th = 'h-7 px-3 text-right text-[11px] font-medium text-muted-foreground whitespace-nowrap'
  const td = 'px-3 py-1.5 text-right whitespace-nowrap'
  return (
    <div className="min-w-0 rounded-lg border border-line/70 bg-page/40">
      <div className="flex items-baseline justify-between gap-2 border-b border-line/70 px-3 py-2 text-[13px]">
        <span className="font-medium text-foreground">
          {m.map_name} <span className="font-normal text-muted-foreground">{m.mode}</span>
        </span>
        <span className={cn('font-medium', m.won ? 'text-foreground' : 'text-muted-foreground')}>
          {m.won ? 'W' : 'L'} {m.our_score}–{m.their_score}
        </span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-xs">
          <thead>
            <tr>
              <th className={cn(th, 'text-left')}>Player</th>
              <th className={th}>K/D</th>
              <th className={th}>Pos eng</th>
              <th className={th}>Op K/pull</th>
            </tr>
          </thead>
          <tbody>
            {m.players.map((p) => {
              const hasScore = p.kills != null && p.deaths != null
              const k = p.kills ?? 0, d = p.deaths ?? 0, a = p.assists ?? 0
              const eng = k + d + a > 0 ? ((k + a) / (k + d + a)) * 100 : null
              return (
                <tr key={p.player_name} className="border-t border-line/50">
                  <td className={cn(td, 'text-left font-medium text-foreground')}>{p.player_name}</td>
                  <td className={td}>
                    {hasScore ? (
                      <>
                        <span className="font-medium text-foreground">{d === 0 ? '∞' : (k / d).toFixed(2)}</span>
                        <span className="ml-1.5 text-muted-foreground">{k}/{d}/{a}</span>
                      </>
                    ) : <Missing />}
                  </td>
                  <td className={td}>{hasScore && eng != null ? `${eng.toFixed(1)}%` : <Missing />}</td>
                  <td className={td}>
                    {p.op_kills != null && p.op_pulls ? (
                      <>
                        <span className="font-medium text-foreground">{(p.op_kills / p.op_pulls).toFixed(2)}</span>
                        <span className="ml-1.5 text-muted-foreground">{p.op_kills}/{p.op_pulls}</span>
                      </>
                    ) : <Missing />}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

/** Series newest first; the newest opens by default. */
export function RecentSeries({ series }: { series: Series[] }) {
  const [open, setOpen] = useState<Set<number> | null>(null)
  const isOpen = (id: number) => (open ? open.has(id) : id === series[0]?.match_id)
  const toggle = (id: number) =>
    setOpen((prev) => {
      const next = new Set(prev ?? (series[0] ? [series[0].match_id] : []))
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })

  return (
    <ul>
      {series.map((s) => {
        const expanded = isOpen(s.match_id)
        const won = s.our_maps > s.their_maps
        return (
          <li key={s.match_id} className="border-b border-line/70 last:border-b-0">
            <button
              type="button"
              aria-expanded={expanded}
              onClick={() => toggle(s.match_id)}
              className={cn(
                'flex w-full items-center gap-3 px-4 py-2.5 text-left text-sm hover:bg-white/[0.025]',
                expanded && 'bg-white/[0.025]',
              )}
            >
              <ChevronRight className={cn('size-3.5 shrink-0 text-muted-foreground transition-transform', expanded && 'rotate-90')} />
              <span className="w-14 shrink-0 text-[13px] text-muted-foreground">{shortDate(s.match_date)}</span>
              <span className="flex-1 font-medium text-foreground">vs {s.opponent}</span>
              <span className={cn('text-[13px] font-medium', won ? 'text-foreground' : 'text-muted-foreground')}>
                {won ? 'W' : 'L'} {s.our_maps}–{s.their_maps}
              </span>
            </button>
            {expanded && (
              <div className="grid gap-3 bg-white/[0.015] px-4 pt-1 pb-4 md:grid-cols-2 xl:grid-cols-3">
                {s.maps.map((m) => <MapPlayers key={m.result_id} m={m} />)}
              </div>
            )}
          </li>
        )
      })}
    </ul>
  )
}

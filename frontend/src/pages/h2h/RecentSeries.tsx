import type { H2H, TeamInfo } from '@/api/models'
import { Card } from '@/components/Card'
import { TeamBadge } from '@/components/TeamBadge'
import { shortDate } from '@/lib/format'
import { cn } from '@/lib/utils'

type Series = H2H['opp_recent_series'][number]

/** The opponent's most recent series in the filter, newest first, from their side. */
export function RecentSeries({ opp, rows, teams, busy }: { opp: TeamInfo; rows: Series[]; teams: TeamInfo[]; busy?: boolean }) {
  const byAbbr = new Map(teams.map((t) => [t.abbreviation, t]))
  return (
    <Card flush busy={busy} title={<span className="inline-flex items-center gap-2"><TeamBadge team={opp} size="sm" />{opp.abbreviation} recent series</span>}>
      {rows.length === 0 ? (
        <p className="px-4 py-6 text-sm text-muted-foreground">No series in this filter.</p>
      ) : (
        <ul>
          {rows.map((s, i) => {
            const vs = byAbbr.get(s.opponent)
            return (
              <li key={i} className="flex items-center gap-3 border-b border-line/70 px-4 py-2.5 last:border-b-0">
                <span
                  className={cn(
                    'grid size-5 shrink-0 place-items-center rounded text-[11px] font-semibold',
                    s.result === 'W' ? 'bg-up/14 text-up' : 'bg-down/14 text-down',
                  )}
                  aria-label={s.result === 'W' ? 'Win' : 'Loss'}
                >
                  {s.result}
                </span>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 text-[13px]">
                    <span className="font-medium text-foreground">{s.score}</span>
                    <span className="text-muted-foreground">vs</span>
                    {vs ? <TeamBadge team={vs} size="sm" showName /> : <span className="font-medium">{s.opponent}</span>}
                  </div>
                  <p className="mt-0.5 truncate text-xs text-muted-foreground" title={s.label}>{s.label}</p>
                </div>
                <span className="shrink-0 self-start pt-0.5 text-xs text-muted-foreground">{shortDate(s.match_date)}</span>
              </li>
            )
          })}
        </ul>
      )}
    </Card>
  )
}

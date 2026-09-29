import type { H2H } from '@/api/models'
import { FlagPill } from '@/components/FlagPill'
import { TeamBadge } from '@/components/TeamBadge'
import { num } from '@/lib/format'
import { cn } from '@/lib/utils'

/** Series cap of `/api/h2h` opp_recent_series (db.queries_views.recent_series limit). */
const RECENT_LIMIT = 10

/**
 * Series and map record between the two teams.
 * Maps: exact, summed from the per-map H2H counts.
 * Series: counted from the opponent's recent series (the API has no H2H series
 * total), so it can be partial when that list is full.
 */
export function h2hRecord(d: H2H) {
  let mapW = 0
  let mapL = 0
  for (const m of d.modes) for (const r of m.maps) {
    mapW += r.h2h.wins
    mapL += r.h2h.losses
  }
  const meetings = d.opp_recent_series.filter((s) => s.opponent === d.team.abbreviation)
  // opp_recent_series is oriented to the opponent: their L is our W.
  const seriesW = meetings.filter((s) => s.result === 'L').length
  const seriesL = meetings.length - seriesW
  const partial = d.opp_recent_series.length >= RECENT_LIMIT
  return { mapW, mapL, seriesW, seriesL, partial }
}

function Side({ d, side }: { d: H2H; side: 'team' | 'opp' }) {
  const team = d[side]
  const elo = d.elo[side]
  const right = side === 'opp'
  return (
    <div className={cn('flex min-w-0 flex-col items-center gap-2 text-center sm:flex-row sm:gap-3 sm:text-left', right && 'sm:flex-row-reverse sm:text-right')}>
      <TeamBadge team={team} size="lg" className="shrink-0" />
      <div className="min-w-0">
        <p className="truncate text-base font-semibold tracking-tight sm:text-lg">
          <span className="sm:hidden">{team.abbreviation}</span>
          <span className="hidden sm:inline">{team.team_name}</span>
        </p>
        <div className={cn('mt-0.5 flex flex-wrap items-center justify-center gap-x-2 gap-y-1', right ? 'sm:justify-end' : 'sm:justify-start')}>
          <span className="text-[13px] text-muted-foreground">
            Elo <span className="font-medium text-foreground">{num(elo.elo)}</span>
          </span>
          {elo.low_confidence && <FlagPill flag={{ kind: 'low_sample', label: 'low confidence' }} />}
        </div>
      </div>
    </div>
  )
}

/** Face-off strip: both teams with Elo, and the head-to-head record between them.
 * The series record leads when it is complete; when it may be cut off by the
 * recent-series cap, the exact map record leads and the series count is labelled. */
export function MatchupHeader({ d }: { d: H2H }) {
  const r = h2hRecord(d)
  const series = r.seriesW + r.seriesL
  const seriesFirst = !r.partial
  const [w, l] = seriesFirst ? [r.seriesW, r.seriesL] : [r.mapW, r.mapL]
  const share = w + l ? w / (w + l) : 0.5
  const seriesText = !series
    ? 'No series'
    : r.partial
      ? `Series ${r.seriesW}–${r.seriesL} in ${d.opp.abbreviation}'s last ${RECENT_LIMIT}`
      : 'Series'
  return (
    <section
      aria-label="Match-up"
      className="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-3 rounded-xl border border-line bg-surface px-4 py-4 sm:gap-6 sm:px-6 sm:py-5"
    >
      <Side d={d} side="team" />
      <div className="flex max-w-32 flex-col items-center gap-1.5 px-1 sm:max-w-none">
        <p className="text-[28px] leading-none font-semibold tracking-tight sm:text-4xl" aria-label={`${seriesFirst ? 'Series' : 'Maps'} ${w} to ${l}`}>
          {w}
          <span className="px-1 text-muted-foreground/60">–</span>
          {l}
        </p>
        <div className="flex h-1 w-20 overflow-hidden rounded-full bg-white/[0.06] sm:w-28" aria-hidden>
          {w + l > 0 && (
            <>
              <div className="h-full bg-gold/80" style={{ width: `${share * 100}%` }} />
              <div className="h-full flex-1 bg-foreground/30" />
            </>
          )}
        </div>
        <p className="text-center text-xs leading-4 text-muted-foreground">
          {seriesFirst ? (
            <>{seriesText}<br />Maps {r.mapW}–{r.mapL}</>
          ) : (
            <>Maps<br />{seriesText}</>
          )}
        </p>
      </div>
      <Side d={d} side="opp" />
    </section>
  )
}

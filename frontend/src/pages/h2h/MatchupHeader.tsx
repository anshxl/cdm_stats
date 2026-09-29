import type { H2H } from '@/api/models'
import { FlagPill } from '@/components/FlagPill'
import { TeamBadge } from '@/components/TeamBadge'
import { num } from '@/lib/format'
import { cn } from '@/lib/utils'

/** Map record between the two teams, summed from the per-map H2H counts. */
function h2hMaps(d: H2H) {
  let wins = 0
  let losses = 0
  for (const m of d.modes) for (const r of m.maps) {
    wins += r.h2h.wins
    losses += r.h2h.losses
  }
  return { wins, losses }
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

/** Face-off strip: both teams with Elo, the head-to-head series record, and the map record beneath. */
export function MatchupHeader({ d }: { d: H2H }) {
  const { wins: w, losses: l } = d.h2h_series
  const maps = h2hMaps(d)
  const share = w + l ? w / (w + l) : 0.5
  return (
    <section
      aria-label="Match-up"
      className="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-3 rounded-xl border border-line bg-surface px-4 py-4 sm:gap-6 sm:px-6 sm:py-5"
    >
      <Side d={d} side="team" />
      <div className="flex max-w-32 flex-col items-center gap-1.5 px-1 sm:max-w-none">
        <p className="text-[28px] leading-none font-semibold tracking-tight sm:text-4xl" aria-label={`Series ${w} to ${l}`}>
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
          {w + l ? 'Series' : 'No series'}<br />Maps {maps.wins}–{maps.losses}
        </p>
      </div>
      <Side d={d} side="opp" />
    </section>
  )
}

import type { Elo } from '@/api/models'
import { FlagPill } from '@/components/FlagPill'
import { TeamBadge } from '@/components/TeamBadge'
import { teamLineColor } from '@/lib/teamColor'
import { cn } from '@/lib/utils'

type Row = Elo['current'][number]

export interface EloRankingProps {
  rows: Row[]
  seed: number
  focus: string | null
  onFocus: (abbr: string) => void
}

/** Current Elo, highest first. Bars start just below the lowest rating so the gaps read; a dashed tick marks the seed. */
export function EloRanking({ rows, seed, focus, onFocus }: EloRankingProps) {
  const sorted = [...rows].sort((a, b) => b.elo - a.elo)
  const values = sorted.map((r) => r.elo).concat(seed)
  const lo = Math.floor((Math.min(...values) - 50) / 25) * 25
  const hi = Math.ceil((Math.max(...values) + 10) / 25) * 25
  const pos = (v: number) => ((v - lo) / (hi - lo)) * 100
  const seedPos = pos(seed)

  return (
    <div className="flex flex-col">
      <div className="grid grid-cols-[1.75rem_minmax(0,1fr)_3.25rem] items-end gap-x-3 border-b border-line px-4 pb-2 text-xs text-muted-foreground sm:grid-cols-[1.75rem_16rem_minmax(0,1fr)_3.5rem]">
        <span>#</span>
        <span>Team</span>
        <span className="relative hidden h-4 sm:block">
          <span className="absolute left-0">{lo}</span>
          <span className="absolute -translate-x-1/2" style={{ left: `${seedPos}%` }}>Seed {seed.toFixed(0)}</span>
        </span>
        <span className="text-right">Elo</span>
      </div>
      <ol>
        {sorted.map((r, i) => {
          const isFocus = r.team.abbreviation === focus
          return (
            <li key={r.team.abbreviation} className="border-b border-line/70 last:border-b-0">
              <button
                type="button"
                onClick={() => onFocus(r.team.abbreviation)}
                aria-pressed={isFocus}
                className={cn(
                  'relative grid w-full grid-cols-[1.75rem_minmax(0,1fr)_3.25rem] items-center gap-x-3 gap-y-1.5 px-4 py-2.5 text-left text-sm transition-colors focus-visible:bg-white/[0.04] focus-visible:outline-none sm:grid-cols-[1.75rem_16rem_minmax(0,1fr)_3.5rem]',
                  isFocus ? 'bg-gold/[0.06]' : 'hover:bg-white/[0.025]',
                )}
              >
                {isFocus && <span className="absolute inset-y-0 left-0 w-0.5 bg-gold" aria-hidden />}
                <span className={cn('text-xs', isFocus ? 'font-semibold text-gold' : 'text-muted-foreground')}>{i + 1}</span>
                <span className="flex min-w-0 items-center gap-2">
                  <TeamBadge team={r.team} size="sm" className="shrink-0" />
                  <span className="truncate font-medium text-foreground" title={r.team.team_name}>{r.team.team_name}</span>
                  {r.low_confidence && (
                    <>
                      <FlagPill flag={{ kind: 'low_sample', label: 'low confidence' }} className="max-sm:hidden" />
                      <FlagPill flag={{ kind: 'low_sample', label: 'low conf.' }} className="sm:hidden" />
                    </>
                  )}
                </span>
                <span className="relative col-span-3 col-start-1 row-start-2 h-2 rounded-full bg-white/[0.04] sm:col-span-1 sm:col-start-3 sm:row-start-1" aria-hidden>
                  <span
                    className="absolute inset-y-0 left-0 rounded-full"
                    style={{
                      width: `${pos(r.elo)}%`,
                      background: isFocus ? teamLineColor(r.team) : 'rgb(231 232 234 / 0.28)',
                      opacity: r.low_confidence && !isFocus ? 0.6 : 1,
                    }}
                  />
                  <span className="absolute -inset-y-1 w-px border-l border-dashed border-[#8a8f98]" style={{ left: `${seedPos}%` }} />
                </span>
                <span className="text-right font-semibold text-foreground">{r.elo.toFixed(0)}</span>
              </button>
            </li>
          )
        })}
      </ol>
    </div>
  )
}

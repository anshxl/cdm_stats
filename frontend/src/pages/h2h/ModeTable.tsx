import { Fragment, useState } from 'react'
import { ChevronRight } from 'lucide-react'
import type { Flagged, H2HMap, H2HMode, TeamInfo } from '@/api/models'
import { Card } from '@/components/Card'
import { FlagPill } from '@/components/FlagPill'
import { MIN_N } from '@/components/MapTable'
import { TeamBadge } from '@/components/TeamBadge'
import { pct } from '@/lib/format'
import { cn } from '@/lib/utils'

// Page-local table: the shared MapTable draws one win-rate bar per row,
// and H2H needs both teams' rates side by side.

type Count = H2HMap['opp_bans']
type WL = H2HMap['h2h']

function RateCell({ rate, ours }: { rate: Flagged; ours: boolean }) {
  return (
    <div className="flex items-center gap-2">
      <div className="relative hidden h-1.5 w-12 overflow-hidden rounded-full bg-white/[0.06] sm:block" aria-hidden>
        {rate.value != null && (
          <div
            className={cn('absolute inset-y-0 left-0 rounded-full', ours ? 'bg-gold/75' : 'bg-foreground/45')}
            style={{ width: `${rate.value * 100}%` }}
          />
        )}
        <div className="absolute inset-y-0 left-1/2 w-px bg-surface" />
      </div>
      <span className="w-9 text-right text-[13px] font-medium text-foreground">{pct(rate.value)}</span>
      <span className="hidden w-8 text-[11px] text-muted-foreground sm:inline">n={rate.n}</span>
    </div>
  )
}

const wl = (r: WL) => `${r.wins}–${r.losses}`
const count = (c: Count) => (c.total ? `${c.count}/${c.total}` : '—')

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col">
      <dt className="text-[11px] text-muted-foreground">{label}</dt>
      <dd className="text-[13px] font-medium text-foreground">{value}</dd>
    </div>
  )
}

function strength(s: H2HMap['your_strength']) {
  return s.rating == null ? '—' : `${pct(s.rating)}${s.low_confidence ? '*' : ''}`
}

/** Expanded row: what the Dash row showed (strength, pick/defend, ban and pick counts). */
function Detail({ r, team, opp }: { r: H2HMap; team: TeamInfo; opp: TeamInfo }) {
  const line = 'grid grid-cols-[3.5rem_1fr] items-start gap-3 sm:grid-cols-[4rem_1fr]'
  const stats = 'grid grid-cols-3 gap-x-4 gap-y-2 sm:flex sm:flex-wrap sm:gap-x-6'
  return (
    <div className="flex flex-col gap-3">
      <div className={line}>
        <TeamBadge team={team} size="sm" showName className="text-xs" />
        <dl className={stats}>
          <Stat label="Overall" value={wl(r.your_wl)} />
          <Stat label="On own pick" value={wl(r.your_pick_wl)} />
          <Stat label="On their pick" value={wl(r.your_defend_wl)} />
          <Stat label="Map strength" value={strength(r.your_strength)} />
        </dl>
      </div>
      <div className={line}>
        <TeamBadge team={opp} size="sm" showName className="text-xs" />
        <dl className={stats}>
          <Stat label="Overall" value={wl(r.opp_wl)} />
          <Stat label="On own pick" value={wl(r.opp_pick_wl)} />
          <Stat label="On their pick" value={wl(r.opp_defend_wl)} />
          <Stat label="Map strength" value={strength(r.opp_strength)} />
          <Stat label="Bans, all" value={count(r.opp_bans)} />
          <Stat label={`Bans vs ${team.abbreviation}`} value={count(r.opp_bans_h2h)} />
          <Stat label="Picks" value={count(r.opp_picks)} />
        </dl>
      </div>
      <p className="text-[11px] text-muted-foreground">
        Strength gap {r.delta == null ? '—' : `${r.delta > 0 ? '+' : ''}${Math.round(r.delta * 100)}`} pts
        {(r.your_strength.low_confidence || r.opp_strength.low_confidence) && '. * Low-confidence strength.'}
        {' '}Ban and pick counts use series with recorded ban data only.
      </p>
    </div>
  )
}

function Tags({ r, className }: { r: H2HMap; className?: string }) {
  if (!r.tags.length) return null
  return (
    <div className={cn('flex flex-wrap gap-1.5', className)}>
      {r.tags.map((t) => <FlagPill key={t} tag={t} />)}
    </div>
  )
}

export function ModeTable({ mode, team, opp, busy }: { mode: H2HMode; team: TeamInfo; opp: TeamInfo; busy?: boolean }) {
  const [open, setOpen] = useState<Set<number>>(new Set())
  const toggle = (id: number) =>
    setOpen((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })

  const th = 'h-9 px-2 sm:px-3 text-xs font-medium text-muted-foreground whitespace-nowrap'

  return (
    <Card
      flush
      busy={busy}
      title={mode.mode}
      action={mode.not_enough_data ? <span className="text-xs text-muted-foreground">Not enough data</span> : undefined}
    >
      {mode.maps.length === 0 ? (
        <p className="px-4 py-6 text-sm text-muted-foreground">No maps played in this mode.</p>
      ) : (
        <div className="overflow-x-auto">
          {/* Fixed column widths so the three mode tables line up; the map column takes the rest. */}
          <table className="w-full table-fixed border-collapse text-sm sm:min-w-[790px]">
            <colgroup>
              <col />
              <col className="w-[76px] sm:w-[156px]" />
              <col className="w-[76px] sm:w-[156px]" />
              <col className="w-[64px]" />
              <col className="hidden w-[88px] sm:table-column" />
              <col className="hidden w-[156px] sm:table-column" />
            </colgroup>
            <thead>
              <tr className="border-b border-line">
                <th scope="col" className={cn(th, 'pl-4 text-left')}>Map</th>
                <th scope="col" className={cn(th, 'text-left')}>
                  <span className="inline-flex items-center gap-1.5"><TeamBadge team={team} size="sm" />{team.abbreviation}</span>
                </th>
                <th scope="col" className={cn(th, 'text-left')}>
                  <span className="inline-flex items-center gap-1.5"><TeamBadge team={opp} size="sm" />{opp.abbreviation}</span>
                </th>
                <th scope="col" className={cn(th, 'text-right max-sm:pr-4')} title="Map record between the two teams">H2H</th>
                <th scope="col" className={cn(th, 'hidden text-right sm:table-cell')} title={`Series where ${opp.abbreviation} banned this map / series with ban data`}>
                  {opp.abbreviation} bans
                </th>
                <th scope="col" className={cn(th, 'hidden pr-4 text-left sm:table-cell')}><span className="sr-only">Tags</span></th>
              </tr>
            </thead>
            <tbody>
              {mode.maps.map((r) => {
                const n = Math.min(r.your_rate.n, r.opp_rate.n)
                const low = n < MIN_N
                const expanded = open.has(r.map_id)
                const dim = low && 'opacity-55'
                return (
                  <Fragment key={r.map_id}>
                    <tr
                      className={cn(
                        'cursor-pointer border-b border-line/70 last:border-b-0 hover:bg-white/[0.025]',
                        expanded && 'bg-white/[0.025]',
                      )}
                      onClick={() => toggle(r.map_id)}
                    >
                      <td className="py-2.5 pr-2 pl-4 sm:pr-3">
                        <div className="flex items-start gap-1.5">
                          <button
                            type="button"
                            aria-expanded={expanded}
                            aria-label={`${expanded ? 'Hide' : 'Show'} ${r.map_name} details`}
                            onClick={(e) => {
                              e.stopPropagation()
                              toggle(r.map_id)
                            }}
                            className="-ml-1 grid size-5 shrink-0 place-items-center rounded text-muted-foreground hover:text-foreground"
                          >
                            <ChevronRight className={cn('size-3.5 transition-transform', expanded && 'rotate-90')} />
                          </button>
                          <div className="min-w-0">
                            <div className={cn('font-medium text-foreground sm:whitespace-nowrap', dim)}>{r.map_name}</div>
                            {low && <div className="text-[11px] whitespace-nowrap text-muted-foreground">low sample</div>}
                            <Tags r={r} className="mt-1 sm:hidden" />
                          </div>
                        </div>
                      </td>
                      <td className={cn('px-2 py-2.5 sm:px-3', dim)}><RateCell rate={r.your_rate} ours /></td>
                      <td className={cn('px-2 py-2.5 sm:px-3', dim)}><RateCell rate={r.opp_rate} ours={false} /></td>
                      <td className={cn('px-2 py-2.5 text-right whitespace-nowrap text-foreground/90 max-sm:pr-4 sm:px-3', dim)}>{wl(r.h2h)}</td>
                      <td className={cn('hidden px-3 py-2.5 text-right whitespace-nowrap text-foreground/90 sm:table-cell', dim)}>{count(r.opp_bans)}</td>
                      <td className="hidden py-2.5 pr-4 pl-3 sm:table-cell"><Tags r={r} className="flex-nowrap" /></td>
                    </tr>
                    {expanded && (
                      <tr className="border-b border-line/70 bg-white/[0.015]">
                        <td colSpan={6} className="px-4 py-3">
                          <Detail r={r} team={team} opp={opp} />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  )
}

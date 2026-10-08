import { useSearchParams } from 'react-router'
import type { Flag, ScrimOptions, Scrims } from '@/api/models'
import { PageBody } from '@/components/AppShell'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { ErrorCard } from '@/components/ErrorCard'
import { FilterBar } from '@/components/FilterBar'
import { MIN_N } from '@/components/MapTable'
import { Skeleton } from '@/components/Skeleton'
import { StatTile } from '@/components/StatTile'
import { TeamSelect } from '@/components/TeamSelect'
import { TrendChart } from '@/components/TrendChart'
import { useApi } from '@/hooks/useApi'
import { pct } from '@/lib/format'
import { CHAMPS_START, MapSelect, MODES, ModeToggle, PeriodToggle, type Mode, type Period } from './scrims/ScrimFilters'
import { ScrimMapTables } from './scrims/ScrimMapTables'
import { mondayOf, ScrimPlayers } from './scrims/ScrimPlayers'

const MODE_NAME: Record<Mode, string> = { SnD: 'Search & Destroy', HP: 'Hardpoint', Control: 'Control' }

/** Per-day points -> one point per Monday-start week, weighted by maps played. */
function weekly(trend: Scrims['trend']) {
  const weeks = new Map<string, { wins: number; played: number }>()
  for (const t of trend) {
    const key = mondayOf(t.match_date)
    const w = weeks.get(key) ?? { wins: 0, played: 0 }
    w.wins += t.wins
    w.played += t.played
    weeks.set(key, w)
  }
  return [...weeks].map(([date, w]) => ({ date, value: w.played ? w.wins / w.played : null }))
}

const lowSample = (n: number): Flag | null => (n < MIN_N ? { kind: 'low_sample', label: 'low sample' } : null)

/** The day before the cutoff, so 'before' ends where 'since' starts. */
const BEFORE_END = new Date(Date.parse(`${CHAMPS_START}T00:00:00Z`) - 86_400_000).toISOString().slice(0, 10)

// Scrims have no event family: dates only, picked by the period toggle.
export default function ScrimsPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  const period: Period = searchParams.get('period') === 'before' ? 'before' : 'since'
  const dateParams = period === 'since' ? { start: CHAMPS_START } : { end: BEFORE_END }
  // The champs block is short, so weeks would collapse it to a point or two.
  const daily = period === 'since'

  const modeParam = searchParams.get('mode')
  const mode = (MODES as readonly string[]).includes(modeParam ?? '') ? (modeParam as Mode) : null

  const options = useApi<ScrimOptions>('/api/scrims/options', { ...dateParams, mode })
  const mapParam = searchParams.get('map')
  const map = mapParam && options.data?.maps.includes(mapParam) ? mapParam : null
  const opponentParam = searchParams.get('opponent')
  const opponent = options.data?.opponents.some((o) => o.abbreviation === opponentParam) ? opponentParam : null

  const scrims = useApi<Scrims>(options.data ? '/api/scrims' : null, { ...dateParams, mode, map, opponent })
  const s = scrims.data

  const setParam = (key: string, value: string | null) =>
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev)
      if (value) next.set(key, value)
      else next.delete(key)
      return next
    }, { replace: true })

  const error = options.error ?? scrims.error
  const scope = [mode && MODE_NAME[mode], map, opponent && `vs ${opponent}`].filter(Boolean).join(', ')
  const periodToggle = <PeriodToggle value={period} onChange={(p) => setParam('period', p === 'since' ? null : p)} />

  return (
    <>
      <FilterBar hideEvent>
        <ModeToggle value={mode} onChange={(m) => setParam('mode', m)} />
        <MapSelect options={options.data?.maps ?? []} value={map} onChange={(m) => setParam('map', m)} />
        <TeamSelect
          label="Opponent"
          options={options.data?.opponents ?? []}
          value={opponent}
          onChange={(o) => setParam('opponent', o)}
          allLabel="All opponents"
        />
      </FilterBar>
      <PageBody>
        <header className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
          <h1 className="text-xl font-semibold tracking-tight text-foreground">Scrims</h1>
          <p className="text-[13px] text-muted-foreground">
            {period === 'since' ? 'Since Oct 5' : 'Before Oct 5'} · {scope || 'All modes, maps and opponents'}
          </p>
        </header>

        {error ? (
          <ErrorCard error={error} title="Scrims" />
        ) : !s ? (
          <>
            <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
              {[0, 1, 2, 3].map((i) => <Skeleton key={i} className="h-[98px]" />)}
            </div>
            <Skeleton className="h-80" />
          </>
        ) : s.overall.total === 0 ? (
          <Card>
            <EmptyState message="No scrims in this filter" hint="Clear the mode, map and opponent, or switch the period." />
            <div className="flex justify-center pb-4">{periodToggle}</div>
          </Card>
        ) : (
          <>
            <div className={scrims.loading ? 'opacity-60 transition-opacity' : 'transition-opacity'}>
              <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                <StatTile
                  label={mode ? `Overall, ${mode}` : 'Overall'}
                  value={pct(s.overall.win_pct)}
                  sub={`${s.overall.wins}–${s.overall.losses} maps`}
                  n={s.overall.total}
                  flag={lowSample(s.overall.total)}
                  className="border-gold/35"
                />
                {s.by_mode.map((m) => (
                  <StatTile
                    key={m.mode}
                    label={m.mode}
                    value={pct(m.win_pct)}
                    sub={`${m.wins}–${m.losses} maps`}
                    n={m.total}
                    flag={lowSample(m.total)}
                    className={mode && mode !== m.mode ? 'opacity-50' : undefined}
                  />
                ))}
              </div>
            </div>

            <Card
              title={daily ? 'Daily win rate' : 'Weekly win rate'}
              action={<span className="hidden text-xs text-muted-foreground sm:inline">{s.trend.length} scrim days{daily ? '' : ', grouped by week'}</span>}
              busy={scrims.loading}
            >
              <TrendChart
                series={[{
                  key: 'win',
                  label: 'Win rate',
                  color: '#e7e8ea',
                  points: daily ? s.trend.map((t) => ({ date: t.match_date, value: t.win_pct })) : weekly(s.trend),
                }]}
                height={260}
                yDomain={[0, 1]}
                referenceLine={{ y: 0.5, label: '50%' }}
                formatValue={(v) => pct(v)}
                ariaLabel={daily ? 'Scrim win rate by day' : 'Scrim win rate by week'}
              />
              {/* pl-12 = the chart's 48px y-axis, so the toggle starts where the x-axis does. */}
              <div className="pl-12">{periodToggle}</div>
            </Card>

            <Card
              title="Map breakdown"
              action={<span className="hidden text-xs text-muted-foreground sm:inline">{map ? 'Shows every map in the mode' : 'Select a row for its last 5 results'}</span>}
              busy={scrims.loading}
              flush
            >
              {s.maps.length ? <ScrimMapTables maps={s.maps} /> : <EmptyState message="No maps in this filter" hint="" />}
            </Card>

            <ScrimPlayers trend={s.kd_trend} daily={daily} busy={scrims.loading} />
          </>
        )}
      </PageBody>
    </>
  )
}

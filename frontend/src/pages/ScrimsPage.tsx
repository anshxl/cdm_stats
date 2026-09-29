import { useSearchParams } from 'react-router'
import type { ScrimOptions, Scrims } from '@/api/models'
import { PageBody } from '@/components/AppShell'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { ErrorCard } from '@/components/ErrorCard'
import { FilterBar } from '@/components/FilterBar'
import { Skeleton } from '@/components/Skeleton'
import { StatTile } from '@/components/StatTile'
import { TeamSelect } from '@/components/TeamSelect'
import { TrendChart } from '@/components/TrendChart'
import { useApi } from '@/hooks/useApi'
import { useFilter } from '@/hooks/useFilter'
import { pct } from '@/lib/format'

// Placeholder page: proves the shell wiring. Scrims have no event family: dates only.
export default function ScrimsPage() {
  const { dateParams } = useFilter()
  const [searchParams, setSearchParams] = useSearchParams()
  const options = useApi<ScrimOptions>('/api/scrims/options', { ...dateParams })
  const opponentParam = searchParams.get('opponent')
  const opponent = options.data?.opponents.some((o) => o.abbreviation === opponentParam) ? opponentParam : null
  const scrims = useApi<Scrims>(options.data ? '/api/scrims' : null, { ...dateParams, opponent })
  const s = scrims.data

  const setOpponent = (abbr: string | null) =>
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev)
      if (abbr) next.set('opponent', abbr)
      else next.delete('opponent')
      return next
    }, { replace: true })

  return (
    <>
      <FilterBar eventDisabledNote="Scrims: dates only">
        <TeamSelect
          label="Opponent"
          options={options.data?.opponents ?? []}
          value={opponent}
          onChange={setOpponent}
          allLabel="All opponents"
        />
      </FilterBar>
      <PageBody>
        {options.error || scrims.error ? (
          <ErrorCard error={(options.error ?? scrims.error)!} title="Scrims" />
        ) : !s ? (
          <Skeleton className="h-64" />
        ) : s.overall.total === 0 ? (
          <Card><EmptyState /></Card>
        ) : (
          <>
            <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
              <StatTile label="Overall" value={pct(s.overall.win_pct / 100)} sub={`${s.overall.wins}–${s.overall.losses}`} n={s.overall.total} />
              {s.by_mode.map((m) => (
                <StatTile key={m.mode} label={m.mode} value={pct(m.win_pct / 100)} sub={`${m.wins}–${m.losses}`} n={m.total} />
              ))}
            </div>
            <Card title="Win rate by day" busy={scrims.loading}>
              <TrendChart
                series={[{ key: 'win', label: 'Win rate', color: '#e7e8ea', points: s.trend.map((t) => ({ date: t.match_date, value: t.win_pct / 100 })) }]}
                yDomain={[0, 1]}
                referenceLine={{ y: 0.5, label: '50%' }}
                formatValue={(v) => pct(v)}
                ariaLabel="Scrim win rate over time"
              />
            </Card>
          </>
        )}
      </PageBody>
    </>
  )
}

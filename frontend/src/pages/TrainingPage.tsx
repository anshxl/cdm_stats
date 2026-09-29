import { useSearchParams } from 'react-router'
import type { Training } from '@/api/models'
import { PageBody } from '@/components/AppShell'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { ErrorCard } from '@/components/ErrorCard'
import { FilterBar } from '@/components/FilterBar'
import { Skeleton } from '@/components/Skeleton'
import { StatTile } from '@/components/StatTile'
import { useApi } from '@/hooks/useApi'
import { num } from '@/lib/format'
import { currentDay, currentMonth, daysInMonth, monthLabel, monthOptions, readMonth } from '@/lib/month'
import { LabeledSelect } from './team/LabeledSelect'
import { Heatmap } from './training/Heatmap'
import { RankTable } from './training/RankTable'

// Participation from the Discord training bot, one month at a time. No event filter.
export default function TrainingPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const current = currentMonth()
  const month = readMonth(searchParams.get('month')) ?? current

  const training = useApi<Training>('/api/training', { month })
  const t = training.data
  const days = daysInMonth(month)
  const isCurrent = month === current
  const lastDay = isCurrent ? currentDay() : days

  const setMonth = (m: string) =>
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev)
      if (m === current) next.delete('month')
      else next.set('month', m)
      return next
    }, { replace: true })

  const players = t?.players ?? []
  const totalSessions = players.reduce((s, p) => s + p.sessions, 0)
  const avgDays = players.length ? players.reduce((s, p) => s + p.days_trained, 0) / players.length : null
  const error = training.error

  return (
    <>
      <FilterBar hideEvent>
        <LabeledSelect
          label="Month"
          value={month}
          options={monthOptions(t?.months ?? [], current, month).map((m) => ({ value: m, label: monthLabel(m) }))}
          onChange={setMonth}
        />
      </FilterBar>
      <PageBody>
        <header className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
          <h1 className="text-xl font-semibold tracking-tight text-foreground">Training</h1>
          <p className="text-[13px] text-muted-foreground">
            {monthLabel(month)}{isCurrent ? `, through day ${lastDay} of ${days}` : ''}
          </p>
        </header>

        {error ? (
          <ErrorCard
            title="Training"
            error={error.status === 503
              ? { status: null, message: `Training data is unavailable right now. ${error.message}` }
              : error}
          />
        ) : !t ? (
          <>
            <div className="grid grid-cols-2 gap-3 lg:grid-cols-3">
              {[0, 1, 2].map((i) => <Skeleton key={i} className="h-[98px]" />)}
            </div>
            <Skeleton className="h-72" />
            <Skeleton className="h-64" />
          </>
        ) : players.length === 0 ? (
          <Card>
            <EmptyState message="No training sessions this month" hint="Pick another month." />
          </Card>
        ) : (
          <>
            <div className={training.loading ? 'opacity-60 transition-opacity' : 'transition-opacity'}>
              <div className="grid grid-cols-2 gap-3 lg:grid-cols-3">
                <StatTile
                  label="Total sessions"
                  value={totalSessions}
                  sub={`${num(totalSessions / players.length, 1)} per active player`}
                  className="border-gold/35"
                />
                <StatTile label="Active players" value={players.length} sub="At least one session" />
                <StatTile
                  label="Average days trained"
                  value={num(avgDays, 1)}
                  sub={`Per active player, of ${days} days`}
                  className="max-lg:col-span-2"
                />
              </div>
            </div>

            <Card title="Ranking" action={<span className="text-xs text-muted-foreground">By sessions</span>} busy={training.loading} flush>
              <RankTable players={players} days={days} />
            </Card>

            <Card
              title="Daily sessions"
              action={<span className="hidden text-xs text-muted-foreground sm:inline">Hover a cell for the count</span>}
              busy={training.loading}
            >
              <Heatmap
                month={month}
                days={days}
                usernames={players.map((p) => p.username)}
                daily={t.daily}
                lastDay={lastDay}
              />
            </Card>
          </>
        )}
      </PageBody>
    </>
  )
}

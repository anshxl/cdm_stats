import type { Elo } from '@/api/models'
import { PageBody } from '@/components/AppShell'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { ErrorCard } from '@/components/ErrorCard'
import { FilterBar } from '@/components/FilterBar'
import { Skeleton } from '@/components/Skeleton'
import { TrendChart } from '@/components/TrendChart'
import { useApi } from '@/hooks/useApi'
import { useFilter } from '@/hooks/useFilter'
import { teamLineColor } from '@/lib/teamColor'

// Placeholder page: proves the shell wiring and TrendChart. The full Elo page replaces the body.
export default function EloPage() {
  const { params } = useFilter()
  const elo = useApi<Elo>('/api/elo', { ...params, view: 'trajectory' })
  const d = elo.data

  return (
    <>
      <FilterBar />
      <PageBody>
        {elo.error ? (
          <ErrorCard error={elo.error} title="Elo" />
        ) : !d ? (
          <Skeleton className="h-96" />
        ) : d.trajectory.length === 0 ? (
          <Card><EmptyState /></Card>
        ) : (
          <Card title="Elo trajectory" busy={elo.loading}>
            <TrendChart
              height={380}
              series={d.trajectory.map((s) => ({
                key: s.team.abbreviation,
                label: s.team.abbreviation,
                color: teamLineColor(s.team),
                points: s.points.map((p) => ({ date: p.match_date, value: p.elo })),
              }))}
              referenceLine={{ y: d.seed, label: 'Seed' }}
              formatValue={(v) => v.toFixed(0)}
              ariaLabel="Elo rating over time by team"
            />
          </Card>
        )}
      </PageBody>
    </>
  )
}

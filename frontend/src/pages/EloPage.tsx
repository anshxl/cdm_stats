import { useSearchParams } from 'react-router'
import type { Elo, TeamInfo } from '@/api/models'
import { PageBody } from '@/components/AppShell'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { ErrorCard } from '@/components/ErrorCard'
import { FilterBar } from '@/components/FilterBar'
import { Skeleton } from '@/components/Skeleton'
import { TeamSelect } from '@/components/TeamSelect'
import { useApi } from '@/hooks/useApi'
import { useFilter } from '@/hooks/useFilter'
import { useScopedTeam } from '@/hooks/useScopedTeam'
import { cn } from '@/lib/utils'
import { EloRanking } from './elo/EloRanking'
import { EloTrajectoryChart } from './elo/EloTrajectoryChart'

type View = 'trajectory' | 'current'
const VIEWS: { key: View; label: string }[] = [
  { key: 'trajectory', label: 'Trajectory' },
  { key: 'current', label: 'Current' },
]

function ViewToggle({ value, onChange }: { value: View; onChange: (v: View) => void }) {
  return (
    <div role="radiogroup" aria-label="Elo view" className="inline-flex h-8 rounded-md border border-line bg-page p-0.5">
      {VIEWS.map((v) => (
        <button
          key={v.key}
          type="button"
          role="radio"
          aria-checked={value === v.key}
          onClick={() => onChange(v.key)}
          className={cn(
            'rounded-[5px] px-3 text-[13px] font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gold/40',
            value === v.key ? 'bg-raised text-foreground shadow-[inset_0_0_0_1px_var(--border)]' : 'text-muted-foreground hover:text-foreground',
          )}
        >
          {v.label}
        </button>
      ))}
    </div>
  )
}

export default function EloPage() {
  const { params } = useFilter()
  const [searchParams, setSearchParams] = useSearchParams()
  const view: View = searchParams.get('view') === 'current' ? 'current' : 'trajectory'
  const setView = (v: View) =>
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev)
        if (v === 'trajectory') next.delete('view')
        else next.set('view', v)
        return next
      },
      { replace: true },
    )

  const scope = useApi<TeamInfo[]>('/api/scope', { ...params })
  const [team, setTeam] = useScopedTeam(scope.data)
  const elo = useApi<Elo>('/api/elo', { ...params, view })
  // Keep showing the last data while a view switch loads, but only if it is the same view.
  const d = elo.data?.view === view ? elo.data : null
  const empty = d && (view === 'trajectory' ? d.trajectory.length === 0 : d.current.length === 0)

  return (
    <>
      <FilterBar>
        <TeamSelect label="Focus" options={scope.data ?? []} value={team} onChange={(a) => a && setTeam(a)} />
      </FilterBar>
      <PageBody>
        <Card
          title={view === 'trajectory' ? 'Elo over time' : 'Current Elo'}
          action={<ViewToggle value={view} onChange={setView} />}
          busy={elo.loading && Boolean(d)}
          flush={view === 'current' && Boolean(d) && !empty}
          bodyClassName={view === 'current' && d && !empty ? 'pt-2.5' : undefined}
        >
          {elo.error ? (
            <ErrorCard error={elo.error} title="Elo" />
          ) : !d ? (
            <Skeleton className={view === 'trajectory' ? 'h-[480px]' : 'h-[560px]'} />
          ) : empty ? (
            <EmptyState />
          ) : view === 'trajectory' ? (
            <EloTrajectoryChart series={d.trajectory} seed={d.seed} focus={team} onFocus={setTeam} />
          ) : (
            <EloRanking rows={d.current} seed={d.seed} focus={team} onFocus={setTeam} />
          )}
        </Card>
        <p className="text-xs text-muted-foreground">
          Ratings count every competition. The event and date filters only choose which teams and matches show.
        </p>
      </PageBody>
    </>
  )
}

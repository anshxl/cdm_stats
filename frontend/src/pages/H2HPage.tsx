import type { H2H, TeamInfo } from '@/api/models'
import { PageBody } from '@/components/AppShell'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { ErrorCard } from '@/components/ErrorCard'
import { FilterBar } from '@/components/FilterBar'
import { MapTable } from '@/components/MapTable'
import { Skeleton } from '@/components/Skeleton'
import { TeamBadge } from '@/components/TeamBadge'
import { TeamSelect } from '@/components/TeamSelect'
import { useApi } from '@/hooks/useApi'
import { useFilter } from '@/hooks/useFilter'
import { useScopedTeam } from '@/hooks/useScopedTeam'

// Placeholder page: proves the shell wiring. The full H2H page replaces the body.
export default function H2HPage() {
  const { params } = useFilter()
  const scope = useApi<TeamInfo[]>('/api/scope', { ...params })
  const [team, setTeam] = useScopedTeam(scope.data)
  const opponents = useApi<TeamInfo[]>(team ? `/api/teams/${team}/opponents` : null, { ...params })
  const [opp, setOpp] = useScopedTeam(opponents.data, 'opp')
  const h2h = useApi<H2H>('/api/h2h', { ...params, team, opp }, { required: ['team', 'opp'] })
  const d = h2h.data

  return (
    <>
      <FilterBar>
        <TeamSelect label="Team" options={scope.data ?? []} value={team} onChange={(a) => a && setTeam(a)} />
        <TeamSelect label="Opponent" options={opponents.data ?? []} value={opp} onChange={(a) => a && setOpp(a)} />
      </FilterBar>
      <PageBody>
        {scope.error || opponents.error || h2h.error ? (
          <ErrorCard error={(scope.error ?? opponents.error ?? h2h.error)!} title="Head to head" />
        ) : opponents.data?.length === 0 || scope.data?.length === 0 ? (
          <Card><EmptyState /></Card>
        ) : !d ? (
          <Skeleton className="h-80" />
        ) : (
          <div className={h2h.loading ? 'flex flex-col gap-4 opacity-60' : 'flex flex-col gap-4'}>
            <header className="flex items-center gap-3 text-lg font-semibold">
              <TeamBadge team={d.team} showName />
              <span className="text-sm font-normal text-muted-foreground">vs</span>
              <TeamBadge team={d.opp} showName />
            </header>
            {d.modes.map((m) => (
              <Card key={m.mode} title={m.mode} flush>
                {m.not_enough_data ? (
                  <EmptyState message="Not enough data for this mode" hint="" />
                ) : (
                  <MapTable
                    rows={m.maps}
                    rowKey={(r) => String(r.map_id)}
                    mapName={(r) => r.map_name}
                    columns={[
                      { key: 'h2h', header: 'H2H', render: (r) => `${r.h2h.wins}–${r.h2h.losses}` },
                      { key: 'opp', header: 'Opp W–L', render: (r) => `${r.opp_wl.wins}–${r.opp_wl.losses}`, hideOnMobile: true },
                    ]}
                    winRate={(r) => ({ value: r.your_rate.value, n: r.your_rate.n })}
                    winRateLabel="Your win rate"
                    tags={(r) => r.tags}
                  />
                )}
              </Card>
            ))}
          </div>
        )}
      </PageBody>
    </>
  )
}

import { useSearchParams } from 'react-router'
import type { H2H, TeamInfo } from '@/api/models'
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
import { BriefCard } from './h2h/BriefCard'
import { MatchupHeader } from './h2h/MatchupHeader'
import { ModeTable } from './h2h/ModeTable'
import { RecentSeries } from './h2h/RecentSeries'

function Loading() {
  return (
    <>
      <Skeleton className="h-[98px]" />
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[minmax(0,1fr)_22rem]">
        <div className="flex flex-col gap-4">
          {[0, 1, 2].map((i) => <Skeleton key={i} className="h-64" />)}
        </div>
        <Skeleton className="h-96" />
      </div>
    </>
  )
}

function Body({ d, teams, busy, event }: { d: H2H; teams: TeamInfo[]; busy: boolean; event: string }) {
  return (
    <>
      <div className={busy ? 'opacity-60 transition-opacity' : 'transition-opacity'}>
        <MatchupHeader d={d} />
      </div>
      <BriefCard
        key={`${d.team.abbreviation}|${d.opp.abbreviation}|${event}`}
        team={d.team.abbreviation}
        opp={d.opp.abbreviation}
        event={event}
      />
      <div className="grid grid-cols-1 items-start gap-4 lg:grid-cols-[minmax(0,1fr)_22rem]">
        <div className="flex flex-col gap-4">
          {d.modes.map((m) => (
            <ModeTable key={m.mode} mode={m} team={d.team} opp={d.opp} busy={busy} />
          ))}
          <p className="px-1 text-xs leading-5 text-muted-foreground">
            Tags are suggestions from win rates with n ≥ 4: SUGGESTED PICK = our biggest edge, SUGGESTED BAN = their
            biggest edge, THEY LIKELY BAN = their most-banned map (≥30%). Rows marked low sample have n &lt; 4 for at least one team.
          </p>
        </div>
        <div className="lg:sticky lg:top-[4.25rem]">
          <RecentSeries opp={d.opp} rows={d.opp_recent_series} teams={teams} busy={busy} />
        </div>
      </div>
    </>
  )
}

export default function H2HPage() {
  const { filter, params } = useFilter()
  const scope = useApi<TeamInfo[]>('/api/scope', { ...params })
  const [team, setTeam] = useScopedTeam(scope.data)
  const opponents = useApi<TeamInfo[]>(team ? `/api/teams/${team}/opponents` : null, { ...params })
  // Wait for the list that belongs to the current team, so a stale list does not rewrite `opp`.
  const [scopedOpp, setOpp] = useScopedTeam(opponents.loading ? null : opponents.data, 'opp')
  // While the list loads, keep showing the URL's opponent instead of blanking the page.
  const [searchParams] = useSearchParams()
  const urlOpp = searchParams.get('opp')
  const opp = opponents.loading ? (urlOpp !== team ? urlOpp : null) : scopedOpp
  const h2h = useApi<H2H>('/api/h2h', { ...params, team, opp }, { required: ['team', 'opp'] })

  let body: React.ReactNode
  if (scope.error) body = <ErrorCard error={scope.error} title="Team list" />
  else if (scope.data?.length === 0) body = <Card><EmptyState /></Card>
  else if (opponents.error) body = <ErrorCard error={opponents.error} title="Opponent list" />
  else if (!opponents.loading && opponents.data?.length === 0)
    body = (
      <Card>
        <EmptyState message={`${team} played no one in this filter`} hint="Pick another team or event." />
      </Card>
    )
  // An unchecked URL opponent can fail while the list loads; wait for the checked one.
  else if (h2h.error && !opponents.loading) body = <ErrorCard error={h2h.error} title="Head to head" />
  else if (!h2h.data) body = <Loading />
  else body = <Body d={h2h.data} teams={scope.data ?? []} busy={h2h.loading} event={filter.event} />

  return (
    <>
      <FilterBar>
        <TeamSelect label="Team" options={scope.data ?? []} value={team} onChange={(a) => a && setTeam(a)} />
        <TeamSelect label="Opponent" options={opponents.data ?? []} value={opp} onChange={(a) => a && setOpp(a)} />
      </FilterBar>
      <PageBody>{body}</PageBody>
    </>
  )
}

import type { Profile, TeamInfo } from '@/api/models'
import { PageBody } from '@/components/AppShell'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { ErrorCard } from '@/components/ErrorCard'
import { FilterBar } from '@/components/FilterBar'
import { FlagPill } from '@/components/FlagPill'
import { Skeleton } from '@/components/Skeleton'
import { StatTile } from '@/components/StatTile'
import { TeamBadge } from '@/components/TeamBadge'
import { TeamSelect } from '@/components/TeamSelect'
import { useApi } from '@/hooks/useApi'
import { useFilter } from '@/hooks/useFilter'
import { HOME_TEAM, useScopedTeam } from '@/hooks/useScopedTeam'
import { num, pct } from '@/lib/format'
import { MapStrength } from './team/MapStrength'
import { PlayerSection } from './team/PlayerSection'

const LOW_CONFIDENCE = { kind: 'low_sample', label: 'Low confidence' } as const

function ProfileSkeleton() {
  return (
    <>
      <div className="flex items-center gap-4">
        <Skeleton className="size-11" />
        <Skeleton className="h-8 w-56" />
      </div>
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        {[0, 1, 2, 3].map((i) => <Skeleton key={i} className="h-[92px]" />)}
      </div>
      {[0, 1, 2].map((i) => <Skeleton key={i} className="h-64" />)}
    </>
  )
}

function TeamProfile({ p, busy }: { p: Profile; busy: boolean }) {
  const { series_wins: sw, series_losses: sl, map_wins: mw, map_losses: ml } = p.record
  if (sw + sl === 0) {
    return (
      <>
        <Header p={p} />
        <Card><EmptyState /></Card>
      </>
    )
  }
  return (
    <>
      <Header p={p} />
      <div className={`grid grid-cols-2 gap-3 transition-opacity lg:grid-cols-4 ${busy ? 'opacity-60' : ''}`}>
        <StatTile label="Series" value={`${sw}–${sl}`} n={sw + sl} />
        <StatTile label="Maps" value={`${mw}–${ml}`} n={mw + ml} />
        <StatTile label="Map win rate" value={pct(p.overall_map_win_rate)} sub={`${mw} of ${mw + ml} maps won`} />
        <StatTile
          label="Elo"
          value={num(p.elo)}
          flag={p.low_confidence ? LOW_CONFIDENCE : null}
          sub="Rated across all competitions"
        />
      </div>
      <MapStrength profile={p} busy={busy} />
    </>
  )
}

function Header({ p }: { p: Profile }) {
  return (
    <header className="flex flex-wrap items-center gap-x-4 gap-y-2">
      <TeamBadge team={p.team} size="lg" />
      <div className="min-w-0">
        <h1 className="truncate text-2xl leading-tight font-semibold tracking-tight">{p.team.team_name}</h1>
        <p className="flex items-center gap-2 text-[13px] text-muted-foreground">
          <span>{p.team.abbreviation}</span>
          <span aria-hidden className="h-3 w-px bg-line" />
          <span>
            Elo <span className="font-semibold text-foreground">{num(p.elo)}</span>
          </span>
          {p.low_confidence && <FlagPill flag={LOW_CONFIDENCE} />}
        </p>
      </div>
    </header>
  )
}

export default function TeamPage() {
  const { params } = useFilter()
  const scope = useApi<TeamInfo[]>('/api/scope', { ...params })
  const [team, setTeam] = useScopedTeam(scope.data)
  const profile = useApi<Profile>(team ? `/api/teams/${team}/profile` : null, { ...params })
  const p = profile.data
  const hasMatches = Boolean(p && p.record.series_wins + p.record.series_losses > 0)

  return (
    <>
      <FilterBar>
        <TeamSelect label="Team" options={scope.data ?? []} value={team} onChange={(a) => a && setTeam(a)} />
      </FilterBar>
      <PageBody>
        {scope.error ? (
          <ErrorCard error={scope.error} title="Team list" />
        ) : scope.data && scope.data.length === 0 ? (
          <Card><EmptyState /></Card>
        ) : profile.error ? (
          <ErrorCard error={profile.error} title="Team profile" />
        ) : !p ? (
          <ProfileSkeleton />
        ) : (
          <TeamProfile p={p} busy={profile.loading} />
        )}
        {team === HOME_TEAM && (!p || hasMatches) && <PlayerSection team={team} params={params} />}
      </PageBody>
    </>
  )
}

import type { Profile, TeamInfo } from '@/api/models'
import { PageBody } from '@/components/AppShell'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { ErrorCard } from '@/components/ErrorCard'
import { FilterBar } from '@/components/FilterBar'
import { FlagPill } from '@/components/FlagPill'
import { MapTable } from '@/components/MapTable'
import { Skeleton } from '@/components/Skeleton'
import { StatTile } from '@/components/StatTile'
import { TeamBadge } from '@/components/TeamBadge'
import { TeamSelect } from '@/components/TeamSelect'
import { useApi } from '@/hooks/useApi'
import { useFilter } from '@/hooks/useFilter'
import { useScopedTeam } from '@/hooks/useScopedTeam'
import { num, pct, shortDate } from '@/lib/format'

// Placeholder page: proves the shell wiring. The full Team page replaces the body.
export default function TeamPage() {
  const { params } = useFilter()
  const scope = useApi<TeamInfo[]>('/api/scope', { ...params })
  const [team, setTeam] = useScopedTeam(scope.data)
  const profile = useApi<Profile>(team ? `/api/teams/${team}/profile` : null, { ...params })
  const p = profile.data

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
          <>
            <Skeleton className="h-12 w-64" />
            <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
              {[0, 1, 2, 3].map((i) => <Skeleton key={i} className="h-[92px]" />)}
            </div>
            <Skeleton className="h-80" />
          </>
        ) : (
          <div className={profile.loading ? 'opacity-60 transition-opacity' : 'transition-opacity'}>
            <div className="flex flex-col gap-4">
              <header className="flex items-center gap-3">
                <TeamBadge team={p.team} size="lg" />
                <div className="min-w-0">
                  <h1 className="truncate text-xl font-semibold tracking-tight">{p.team.team_name}</h1>
                  <p className="text-[13px] text-muted-foreground">{p.team.abbreviation}</p>
                </div>
              </header>
              <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                <StatTile
                  label="Elo"
                  value={num(p.elo)}
                  flag={p.low_confidence ? { kind: 'low_sample', label: 'low confidence' } : null}
                />
                <StatTile
                  label="Series"
                  value={`${p.record.series_wins}–${p.record.series_losses}`}
                  n={p.record.series_wins + p.record.series_losses}
                />
                <StatTile label="Maps" value={`${p.record.map_wins}–${p.record.map_losses}`} />
                <StatTile
                  label="Map win rate"
                  value={pct(p.overall_map_win_rate)}
                  n={p.record.map_wins + p.record.map_losses}
                />
              </div>
              {p.ban_summary.length > 0 && (
                <div className="flex flex-wrap gap-2">
                  {p.ban_summary.map((b) => (
                    <FlagPill key={`${b.mode}-${b.map_name}`} flag={b.flag} />
                  ))}
                </div>
              )}
              <Card title="Maps" flush>
                <MapTable
                  rows={p.maps}
                  rowKey={(m) => `${m.mode}-${m.map_name}`}
                  mapName={(m) => (
                    <span>
                      {m.map_name} <span className="font-normal text-muted-foreground">{m.mode}</span>
                    </span>
                  )}
                  columns={[
                    { key: 'wl', header: 'W–L', render: (m) => `${m.wins}–${m.losses}` },
                    { key: 'str', header: 'Strength', render: (m) => num(m.strength.rating, 2), hideOnMobile: true },
                  ]}
                  winRate={(m) => ({ value: m.win_rate.value, n: m.win_rate.n })}
                  flag={(m) => m.win_rate.flag}
                  renderDetail={(m) => (
                    <ul className="flex flex-col gap-1 text-xs text-muted-foreground">
                      {m.history.slice(0, 5).map((h, i) => (
                        <li key={i} className="flex gap-3">
                          <span className="w-12">{shortDate(h.match_date)}</span>
                          <span className={h.result === 'W' ? 'text-foreground' : ''}>{h.result} {h.score}</span>
                          <span>vs {h.opponent}</span>
                          <span className="truncate">{h.label}</span>
                        </li>
                      ))}
                    </ul>
                  )}
                />
              </Card>
            </div>
          </div>
        )}
      </PageBody>
    </>
  )
}

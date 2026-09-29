import { useCallback, useMemo } from 'react'
import { useSearchParams } from 'react-router'
import type { Players, TeamInfo } from '@/api/models'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { ErrorCard } from '@/components/ErrorCard'
import { Skeleton } from '@/components/Skeleton'
import { StatTile } from '@/components/StatTile'
import { TeamBadge } from '@/components/TeamBadge'
import { SERIES_COLORS, TrendChart, type TrendSeries } from '@/components/TrendChart'
import { useApi } from '@/hooks/useApi'
import type { FilterParams } from '@/hooks/useFilter'
import { num } from '@/lib/format'
import { LabeledSelect, type SelectOption } from './LabeledSelect'
import { RecentSeries } from './RecentSeries'

const ALL = 'all'
const MODE_OPTIONS: SelectOption[] = [
  { value: ALL, label: 'All modes' },
  { value: 'SnD', label: 'SnD' },
  { value: 'HP', label: 'HP' },
  { value: 'Control', label: 'Control' },
]

/** Color follows the player (roster order), never the rank or the selection. */
function playerColor(roster: string[], name: string): string {
  const i = roster.indexOf(name)
  return SERIES_COLORS[(i < 0 ? roster.length : i) % SERIES_COLORS.length]
}

function toSeries<P extends { player_name: string; match_date: string }>(
  points: P[], roster: string[], value: (p: P) => number | null,
): TrendSeries[] {
  const byPlayer = new Map<string, P[]>()
  for (const p of points) {
    const list = byPlayer.get(p.player_name) ?? []
    list.push(p)
    byPlayer.set(p.player_name, list)
  }
  return [...byPlayer.keys()].sort().map((name) => ({
    key: name,
    label: name,
    color: playerColor(roster, name),
    points: byPlayer.get(name)!.map((p) => ({ date: p.match_date, value: value(p) })),
  }))
}

function seriesTitle(series: string) {
  if (series === 'last10') return 'Last 10 series'
  if (series === 'all') return 'All series'
  return `Series vs ${series}`
}

/** GL-only: Player, Mode and Series selects drive the tiles, both charts and the series list. */
export function PlayerSection({ team, params }: { team: string; params: FilterParams }) {
  // Kept in the URL (player, mode, series) so reloads and shared links keep the view.
  const [searchParams, setSearchParams] = useSearchParams()
  const player = searchParams.get('player') ?? ALL
  const rawMode = searchParams.get('mode')
  const mode = MODE_OPTIONS.some((o) => o.value === rawMode) ? rawMode! : ALL
  const seriesPick = searchParams.get('series') ?? 'last10'
  const setParam = useCallback(
    (key: string, value: string, fallback: string) =>
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          if (value === fallback) next.delete(key)
          else next.set(key, value)
          return next
        },
        { replace: true },
      ),
    [setSearchParams],
  )
  const setPlayer = (v: string) => setParam('player', v, ALL)
  const setMode = (v: string) => setParam('mode', v, ALL)
  const setSeries = (v: string) => setParam('series', v, 'last10')

  const opponents = useApi<TeamInfo[]>(`/api/teams/${team}/opponents`, { ...params })
  // An opponent can drop out of the filter; fall back to Last 10 until one is picked again.
  const series =
    seriesPick === 'last10' || seriesPick === 'all' || opponents.data?.some((o) => o.abbreviation === seriesPick)
      ? seriesPick
      : 'last10'

  const res = useApi<Players>(`/api/teams/${team}/players`, {
    ...params,
    player: player === ALL ? null : player,
    mode: mode === ALL ? null : mode,
    series,
  })
  const d = res.data
  const roster = useMemo(() => d?.players ?? [], [d])
  // A stale ?player= that is not on the roster would show an empty select value.
  const playerValue = !d || roster.includes(player) ? player : ALL

  const kdSeries = useMemo(() => (d ? toSeries(d.kd_trend, roster, (p) => p.kd) : []), [d, roster])
  const opsSeries = useMemo(() => (d ? toSeries(d.ops_trend, roster, (p) => p.kills_per_pull) : []), [d, roster])
  const thinFootage = useMemo(() => {
    const maps = new Map<string, number>()
    for (const p of d?.ops_trend ?? []) maps.set(p.player_name, (maps.get(p.player_name) ?? 0) + p.maps)
    return [...maps].filter(([, n]) => n < 4).map(([name]) => name).sort()
  }, [d])

  const playerOptions: SelectOption[] = [
    { value: ALL, label: 'All players' },
    ...roster.map((p) => ({ value: p, label: p })),
  ]
  const seriesOptions: SelectOption[] = [
    { value: 'last10', label: 'Last 10' },
    { value: 'all', label: 'All' },
    ...(opponents.data ?? []).map((o) => ({
      value: o.abbreviation,
      title: o.team_name,
      label: <TeamBadge team={o} size="sm" showName />,
    })),
  ]
  const busy = res.loading && Boolean(d)

  return (
    <section aria-labelledby="players-heading" className="mt-4 flex flex-col gap-4">
      <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-3">
        <h2 id="players-heading" className="text-lg font-semibold tracking-tight">Players</h2>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2.5">
          <LabeledSelect label="Player" value={playerValue} options={playerOptions} onChange={setPlayer} />
          <LabeledSelect label="Mode" value={mode} options={MODE_OPTIONS} onChange={setMode} />
          <LabeledSelect label="Series" value={series} options={seriesOptions} onChange={setSeries} />
        </div>
      </div>

      {res.error ? (
        <ErrorCard error={res.error} title="Player stats" />
      ) : !d ? (
        <>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
            {[0, 1, 2, 3, 4].map((i) => <Skeleton key={i} className="h-[124px]" />)}
          </div>
          <div className="grid gap-4 lg:grid-cols-2">
            <Skeleton className="h-[340px]" />
            <Skeleton className="h-[340px]" />
          </div>
        </>
      ) : d.tiles.length === 0 ? (
        <Card><EmptyState message="No player stats in this selection" hint="Pick another series or mode, or widen the filter." /></Card>
      ) : (
        <>
          <div className={`grid grid-cols-1 gap-3 transition-opacity min-[440px]:grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 ${busy ? 'opacity-60' : ''}`}>
            {d.tiles.map((t) => (
              <StatTile
                key={t.player_name}
                label={t.player_name}
                value={<>{num(t.kd.value, 2)}<span className="ml-1 text-xs font-normal text-muted-foreground">K/D</span></>}
                flag={t.kd.flag}
                n={t.kd.n}
                sub={
                  <div className="flex flex-col gap-0.5">
                    <span>
                      <span className="text-foreground/90">{num(t.op_kills_per_pull.value, 2)}</span> op K/pull
                      {t.op_kills_per_pull.n > 0 && <span className="text-muted-foreground/70" title="Maps with operator footage"> · n={t.op_kills_per_pull.n}</span>}
                    </span>
                    <span>{t.kills}/{t.deaths}/{t.assists} K/D/A</span>
                    <span>{t.games} maps · {num(t.avg_pos_eng_pct, 1)}% pos eng</span>
                  </div>
                }
              />
            ))}
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            <Card title="K/D trend" busy={busy}>
              {kdSeries.length === 0 ? (
                <EmptyState message="No K/D data in this selection" hint="" />
              ) : (
                <TrendChart
                  series={kdSeries}
                  height={260}
                  referenceLine={{ y: 1, label: '1.0 K/D' }}
                  ariaLabel="K/D by match date, one line per player"
                />
              )}
            </Card>
            <Card title="Operator efficiency" action={<span className="text-xs text-muted-foreground">Op kills per pull</span>} busy={busy}>
              {opsSeries.length === 0 ? (
                <EmptyState message="No operator footage in this selection" hint="Operator footage starts in Season 2 and does not cover every mode." />
              ) : (
                <>
                  <TrendChart
                    series={opsSeries}
                    height={260}
                    referenceLine={{ y: 1, label: '1.00 K/pull' }}
                    ariaLabel="Operator kills per pull by match date, one line per player"
                  />
                  {thinFootage.length > 0 && (
                    <p className="mt-2 text-xs text-muted-foreground">
                      Under 4 maps of footage, read as directional only: {thinFootage.join(', ')}
                    </p>
                  )}
                </>
              )}
            </Card>
          </div>

          <Card
            title={seriesTitle(series)}
            action={<span className="text-xs text-muted-foreground">{d.recent_series.length} series</span>}
            flush
            busy={busy}
          >
            {d.recent_series.length === 0 ? (
              <EmptyState message="No series in this selection" hint="" />
            ) : (
              <RecentSeries key={`${series}-${player}-${mode}-${d.recent_series[0].match_id}`} series={d.recent_series} />
            )}
          </Card>
        </>
      )}
    </section>
  )
}

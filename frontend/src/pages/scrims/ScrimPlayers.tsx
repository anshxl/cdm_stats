import { useMemo } from 'react'
import { useSearchParams } from 'react-router'
import type { Flag, Scrims } from '@/api/models'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { MIN_N } from '@/components/MapTable'
import { StatTile } from '@/components/StatTile'
import { TrendChart } from '@/components/TrendChart'
import { num, shortDate } from '@/lib/format'
import { LabeledSelect, type SelectOption } from '../team/LabeledSelect'
import { toSeries } from '../team/PlayerSection'

type Point = Scrims['kd_trend'][number]

const ALL = 'all'
const LAST10 = 'last10'

/** "2026-10-08" -> the Monday that starts its week, "2026-10-05". */
export function mondayOf(iso: string): string {
  const d = new Date(`${iso.slice(0, 10)}T00:00:00Z`)
  d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7))
  return d.toISOString().slice(0, 10)
}

/** Sum points per player and per `dateOf` bucket; K/D comes from the summed kills and deaths. */
function sumBy(points: Point[], dateOf: (iso: string) => string): Point[] {
  const out = new Map<string, Point>()
  for (const p of points) {
    const date = dateOf(p.match_date)
    const key = `${p.player_name}|${date}`
    const s = out.get(key) ?? { player_name: p.player_name, match_date: date, kills: 0, deaths: 0, assists: 0, games: 0, kd: 0 }
    s.kills += p.kills
    s.deaths += p.deaths
    s.assists += p.assists
    s.games += p.games
    s.kd = s.deaths ? s.kills / s.deaths : 0
    out.set(key, s)
  }
  return [...out.values()]
}

const lowSample = (n: number): Flag | null => (n < MIN_N ? { kind: 'low_sample', label: `n=${n}` } : null)

/** Team-tab layout for scrims: Player and Series (scrim day) selects, a K/D tile per player, a K/D line per player. */
export function ScrimPlayers({ trend, daily, busy }: { trend: Scrims['kd_trend']; daily: boolean; busy: boolean }) {
  // Kept in the URL (player, series) like the Team tab.
  const [searchParams, setSearchParams] = useSearchParams()
  const setParam = (key: string, value: string, fallback: string) =>
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev)
      if (value === fallback) next.delete(key)
      else next.set(key, value)
      return next
    }, { replace: true })

  const roster = useMemo(() => [...new Set(trend.map((p) => p.player_name))].sort(), [trend])
  const days = useMemo(() => [...new Set(trend.map((p) => p.match_date))].sort().reverse(), [trend])

  // A stale ?player= or ?series= that is not in this filter falls back to the default.
  const playerParam = searchParams.get('player') ?? ALL
  const player = roster.includes(playerParam) ? playerParam : ALL
  const seriesParam = searchParams.get('series') ?? LAST10
  const series = seriesParam === ALL || days.includes(seriesParam) ? seriesParam : LAST10

  const rows = useMemo(() => {
    const picked = new Set(series === ALL ? days : series === LAST10 ? days.slice(0, 10) : [series])
    return trend.filter((p) => picked.has(p.match_date) && (player === ALL || p.player_name === player))
  }, [trend, days, series, player])
  const tiles = useMemo(() => sumBy(rows, () => ''), [rows])
  const chart = useMemo(
    () => toSeries(sumBy(rows, daily ? (d) => d : mondayOf), roster, (p) => p.kd),
    [rows, daily, roster],
  )

  const playerOptions: SelectOption[] = [{ value: ALL, label: 'All players' }, ...roster.map((p) => ({ value: p, label: p }))]
  const seriesOptions: SelectOption[] = [
    { value: LAST10, label: 'Last 10 days' },
    { value: ALL, label: 'All days' },
    ...days.map((d) => ({ value: d, label: shortDate(d) })),
  ]

  return (
    <section aria-labelledby="scrim-players-heading" className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-3">
        <h2 id="scrim-players-heading" className="text-lg font-semibold tracking-tight">Players</h2>
        {trend.length > 0 && (
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2.5">
            <LabeledSelect label="Player" value={player} options={playerOptions} onChange={(v) => setParam('player', v, ALL)} />
            <LabeledSelect label="Series" value={series} options={seriesOptions} onChange={(v) => setParam('series', v, LAST10)} />
          </div>
        )}
      </div>
      {tiles.length === 0 ? (
        <Card><EmptyState message="No player stats in this filter" hint="Player stats are entered from scrims_players.csv." /></Card>
      ) : (
        <>
          <div className={`grid grid-cols-1 gap-3 transition-opacity min-[440px]:grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 ${busy ? 'opacity-60' : ''}`}>
            {tiles.map((p) => (
              <StatTile
                key={p.player_name}
                label={p.player_name}
                value={<>{num(p.deaths ? p.kd : null, 2)}<span className="ml-1 text-xs font-normal text-muted-foreground">K/D</span></>}
                flag={lowSample(p.games)}
                sub={<><span className="text-foreground/90">{p.kills}–{p.deaths}–{p.assists}</span> K–D–A · {p.games} maps</>}
              />
            ))}
          </div>
          <Card title={daily ? 'K/D by day' : 'K/D by week'} busy={busy}>
            <TrendChart
              series={chart}
              height={260}
              referenceLine={{ y: 1, label: '1.0 K/D' }}
              ariaLabel={`K/D by ${daily ? 'scrim day' : 'week'}, one line per player`}
            />
          </Card>
        </>
      )}
    </section>
  )
}

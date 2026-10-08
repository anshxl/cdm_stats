import { useMemo } from 'react'
import { useSearchParams } from 'react-router'
import type { Flag, Players, Scrims } from '@/api/models'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { MIN_N } from '@/components/MapTable'
import { StatTile } from '@/components/StatTile'
import { TrendChart } from '@/components/TrendChart'
import { num, shortDate } from '@/lib/format'
import { LabeledSelect, type SelectOption } from '../team/LabeledSelect'
import { toSeries } from '../team/PlayerSection'
import { RecentSeries } from '../team/RecentSeries'

type Row = Scrims['player_maps'][number]
type Series = Players['recent_series'][number]

const ALL = 'all'
const LAST10 = 'last10'

/** "2026-10-08" -> the Monday that starts its week, "2026-10-05". */
export function mondayOf(iso: string): string {
  const d = new Date(`${iso.slice(0, 10)}T00:00:00Z`)
  d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7))
  return d.toISOString().slice(0, 10)
}

interface Totals {
  player_name: string
  match_date: string
  kills: number
  deaths: number
  maps: number
  op_kills: number
  op_pulls: number
  /** Maps with operator footage (HP / Control only). */
  op_maps: number
}

/** Sum rows per player and per `dateOf` bucket. */
function sumBy(rows: Row[], dateOf: (iso: string) => string): Totals[] {
  const out = new Map<string, Totals>()
  for (const r of rows) {
    const date = dateOf(r.date)
    const key = `${r.player_name}|${date}`
    const t = out.get(key) ?? { player_name: r.player_name, match_date: date, kills: 0, deaths: 0, maps: 0, op_kills: 0, op_pulls: 0, op_maps: 0 }
    t.kills += r.kills
    t.deaths += r.deaths
    t.maps += 1
    if (r.op_pulls != null) {
      t.op_kills += r.op_kills ?? 0
      t.op_pulls += r.op_pulls
      t.op_maps += 1
    }
    out.set(key, t)
  }
  return [...out.values()]
}

const kd = (t: Totals) => (t.deaths ? t.kills / t.deaths : null)
const perPull = (t: Totals) => (t.op_pulls ? t.op_kills / t.op_pulls : null)
const lowSample = (n: number): Flag | null => (n < MIN_N ? { kind: 'low_sample', label: `n=${n}` } : null)

/** Rows -> the Team tab's series shape: one entry per scrim (date + opponent), newest first. */
function toScrimSeries(rows: Row[]): Series[] {
  const scrims = new Map<string, Series>()
  for (const r of rows) {
    const key = `${r.date}|${r.opponent}`
    let s = scrims.get(key)
    if (!s) {
      s = { match_id: r.scrim_map_id, match_date: r.date, opponent: r.opponent, our_maps: 0, their_maps: 0, maps: [] }
      scrims.set(key, s)
    }
    let m = s.maps.find((x) => x.result_id === r.scrim_map_id)
    if (!m) {
      m = {
        result_id: r.scrim_map_id, slot: s.maps.length + 1, map_name: r.map_name, mode: r.mode,
        won: r.result === 'W', our_score: r.our_score, their_score: r.opp_score, players: [],
      }
      s.maps.push(m)
      if (m.won) s.our_maps += 1
      else s.their_maps += 1
    }
    m.players.push({
      player_name: r.player_name, kills: r.kills, deaths: r.deaths, assists: r.assists,
      op_kills: r.op_kills, op_pulls: r.op_pulls,
    })
  }
  return [...scrims.values()].reverse()
}

/** Team-tab layout for scrims: Player and Series (scrim day) selects, tiles, K/D and operator charts, map list. */
export function ScrimPlayers({ rows, daily, busy }: { rows: Scrims['player_maps']; daily: boolean; busy: boolean }) {
  // Kept in the URL (player, series) like the Team tab.
  const [searchParams, setSearchParams] = useSearchParams()
  const setParam = (key: string, value: string, fallback: string) =>
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev)
      if (value === fallback) next.delete(key)
      else next.set(key, value)
      return next
    }, { replace: true })

  const roster = useMemo(() => [...new Set(rows.map((r) => r.player_name))].sort(), [rows])
  const days = useMemo(() => [...new Set(rows.map((r) => r.date))].sort().reverse(), [rows])

  // A stale ?player= or ?series= that is not in this filter falls back to the default.
  const playerParam = searchParams.get('player') ?? ALL
  const player = roster.includes(playerParam) ? playerParam : ALL
  const seriesParam = searchParams.get('series') ?? LAST10
  const series = seriesParam === ALL || days.includes(seriesParam) ? seriesParam : LAST10

  const picked = useMemo(() => {
    const keep = new Set(series === ALL ? days : series === LAST10 ? days.slice(0, 10) : [series])
    return rows.filter((r) => keep.has(r.date) && (player === ALL || r.player_name === player))
  }, [rows, days, series, player])

  const tiles = useMemo(() => sumBy(picked, () => ''), [picked])
  const buckets = useMemo(() => sumBy(picked, daily ? (d) => d : mondayOf), [picked, daily])
  const kdSeries = useMemo(() => toSeries(buckets, roster, kd), [buckets, roster])
  const opsSeries = useMemo(() => toSeries(buckets.filter((t) => t.op_pulls > 0), roster, perPull), [buckets, roster])
  const thinFootage = tiles.filter((t) => t.op_maps > 0 && t.op_maps < MIN_N).map((t) => t.player_name)
  const scrims = useMemo(() => toScrimSeries(picked), [picked])

  const playerOptions: SelectOption[] = [{ value: ALL, label: 'All players' }, ...roster.map((p) => ({ value: p, label: p }))]
  const seriesOptions: SelectOption[] = [
    { value: LAST10, label: 'Last 10 days' },
    { value: ALL, label: 'All days' },
    ...days.map((d) => ({ value: d, label: shortDate(d) })),
  ]
  const by = daily ? 'by day' : 'by week'

  return (
    <section aria-labelledby="scrim-players-heading" className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-3">
        <h2 id="scrim-players-heading" className="text-lg font-semibold tracking-tight">Players</h2>
        {rows.length > 0 && (
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
            {tiles.map((t) => (
              <StatTile
                key={t.player_name}
                label={t.player_name}
                value={<>{num(kd(t), 2)}<span className="ml-1 text-xs font-normal text-muted-foreground">K/D</span></>}
                flag={lowSample(t.maps)}
                sub={<><span className="text-foreground/90">{num(perPull(t), 2)}</span> op K/pull</>}
              />
            ))}
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            <Card title={`K/D ${by}`} busy={busy}>
              <TrendChart
                series={kdSeries}
                height={260}
                referenceLine={{ y: 1, label: '1.0 K/D' }}
                ariaLabel={`K/D ${by}, one line per player`}
              />
            </Card>
            <Card title={`Operator efficiency ${by}`} action={<span className="text-xs text-muted-foreground">Op kills per pull</span>} busy={busy}>
              {opsSeries.length === 0 ? (
                <EmptyState message="No operator footage in this selection" hint="Operator footage starts Oct 5 and does not cover SnD." />
              ) : (
                <>
                  <TrendChart
                    series={opsSeries}
                    height={260}
                    referenceLine={{ y: 1, label: '1.00 K/pull' }}
                    ariaLabel={`Operator kills per pull ${by}, one line per player`}
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
            title={series === LAST10 ? 'Last 10 days' : series === ALL ? 'All days' : shortDate(series)}
            action={<span className="text-xs text-muted-foreground">{scrims.length} scrims</span>}
            flush
            busy={busy}
          >
            <RecentSeries key={`${series}-${player}-${scrims[0]?.match_id}`} series={scrims} />
          </Card>
        </>
      )}
    </section>
  )
}

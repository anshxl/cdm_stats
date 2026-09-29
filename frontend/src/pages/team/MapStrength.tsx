import type { Profile, ProfileMap } from '@/api/models'
import { Card } from '@/components/Card'
import { EmptyState } from '@/components/EmptyState'
import { FlagPill } from '@/components/FlagPill'
import { MapTable, type MapTableColumn } from '@/components/MapTable'
import { pct, shortDate } from '@/lib/format'
import { cn } from '@/lib/utils'

const MODES = ['SnD', 'HP', 'Control'] as const

type Rate = ProfileMap['banned']

function RateCell({ rate }: { rate: Rate }) {
  if (rate.total === 0) return <span className="text-muted-foreground">—</span>
  return (
    <span className={rate.count === 0 ? 'text-muted-foreground' : undefined}>
      {rate.count}
      <span className="text-muted-foreground">/{rate.total}</span>
    </span>
  )
}

const wl = (w: number, l: number) => (w + l === 0 ? <span className="text-muted-foreground">—</span> : `${w}–${l}`)

const COLUMNS: MapTableColumn<ProfileMap>[] = [
  { key: 'wl', header: 'W–L', render: (m) => wl(m.wins, m.losses) },
  {
    key: 'str',
    header: 'Strength',
    render: (m) =>
      m.strength.rating == null ? (
        <span className="text-muted-foreground">—</span>
      ) : (
        <span title={m.strength.low_confidence ? 'Low confidence: small weighted sample' : undefined}>
          {pct(m.strength.rating)}
          {m.strength.low_confidence && <span className="text-muted-foreground">*</span>}
        </span>
      ),
    hideOnMobile: true,
  },
  { key: 'pick', header: 'Pick', render: (m) => wl(m.pick_wins, m.pick_losses), hideOnMobile: true },
  { key: 'def', header: 'Defend', render: (m) => wl(m.defend_wins, m.defend_losses), hideOnMobile: true },
  { key: 'ban', header: 'Banned', render: (m) => <RateCell rate={m.banned} />, hideOnMobile: true },
  { key: 'picked', header: 'Picked', render: (m) => <RateCell rate={m.picked} />, hideOnMobile: true },
  { key: 'oppban', header: 'Opp banned', render: (m) => <RateCell rate={m.opp_banned} />, hideOnMobile: true },
]

function MapDetail({ m }: { m: ProfileMap }) {
  const th = 'pb-1.5 pr-4 text-left font-medium text-muted-foreground whitespace-nowrap'
  const td = 'py-1 pr-4 whitespace-nowrap'
  return (
    <div className="flex flex-col gap-3 text-xs">
      {/* Phones hide these columns in the row, so repeat them here. */}
      <dl className="flex flex-wrap gap-x-5 gap-y-1 text-muted-foreground">
        <div className="flex gap-1.5 sm:hidden"><dt>Strength</dt><dd className="text-foreground">{m.strength.rating == null ? '—' : pct(m.strength.rating)}</dd></div>
        <div className="flex gap-1.5"><dt>Pick</dt><dd className="text-foreground">{wl(m.pick_wins, m.pick_losses)}</dd></div>
        <div className="flex gap-1.5"><dt>Defend</dt><dd className="text-foreground">{wl(m.defend_wins, m.defend_losses)}</dd></div>
        <div className="flex gap-1.5 sm:hidden"><dt>Banned</dt><dd className="text-foreground"><RateCell rate={m.banned} /></dd></div>
        <div className="flex gap-1.5 sm:hidden"><dt>Picked</dt><dd className="text-foreground"><RateCell rate={m.picked} /></dd></div>
        <div className="flex gap-1.5 sm:hidden"><dt>Opp banned</dt><dd className="text-foreground"><RateCell rate={m.opp_banned} /></dd></div>
      </dl>
      {m.history.length === 0 ? (
        <p className="text-muted-foreground">No games on this map in the filter.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full border-collapse">
            <thead>
              <tr className="border-b border-line/70">
                <th className={th}>Date</th>
                <th className={th}>Event</th>
                <th className={th}>Opp</th>
                <th className={th}>Result</th>
                <th className={th}>Score</th>
                <th className={th}>Context</th>
                <th className={th}>Picked by</th>
              </tr>
            </thead>
            <tbody>
              {m.history.map((h, i) => (
                <tr key={i}>
                  <td className={cn(td, 'text-muted-foreground')}>{shortDate(h.match_date)}</td>
                  <td className={cn(td, 'text-muted-foreground')}>{h.label}</td>
                  <td className={td}>{h.opponent}</td>
                  <td className={cn(td, 'font-semibold', h.result === 'W' ? 'text-foreground' : 'text-muted-foreground')}>{h.result}</td>
                  <td className={td}>{h.score}</td>
                  <td className={cn(td, 'text-muted-foreground')}>{h.pick_context ?? '—'}</td>
                  <td className={cn(td, 'text-muted-foreground')}>{h.picked_by}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

/** One MapTable per mode, with the mode's ban highlight in the card header. */
export function MapStrength({ profile, busy }: { profile: Profile; busy?: boolean }) {
  return (
    <>
      {MODES.map((mode) => {
        const rows = profile.maps.filter((m) => m.mode === mode)
        const ban = profile.ban_summary.find((b) => b.mode === mode)
        return (
          <Card
            key={mode}
            flush
            busy={busy}
            title={<>{mode} <span className="font-normal text-muted-foreground">map strength</span></>}
            action={
              ban && (
                <span className="flex items-center gap-2 text-xs text-muted-foreground">
                  <span className="hidden sm:inline">Most banned: {ban.map_name}</span>
                  <FlagPill flag={ban.flag} />
                </span>
              )
            }
          >
            {rows.length === 0 ? (
              <EmptyState message={`No ${mode} maps in this filter`} hint="" className="py-6" />
            ) : (
              <MapTable
                rows={rows}
                rowKey={(m) => `${m.mode}-${m.map_name}`}
                mapName={(m) => m.map_name}
                columns={COLUMNS}
                winRate={(m) => ({ value: m.win_rate.value, n: m.win_rate.n })}
                flag={(m) => m.win_rate.flag}
                renderDetail={(m) => <MapDetail m={m} />}
              />
            )}
          </Card>
        )
      })}
      <p className="-mt-1 px-1 text-xs text-muted-foreground">
        Banned, Picked and Opp banned count series with recorded ban data. * Low-confidence strength rating.
      </p>
    </>
  )
}

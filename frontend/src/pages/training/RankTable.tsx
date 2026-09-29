import type { TrainingPlayer } from '@/api/models'

function Bar({ value, max, label }: { value: number; max: number; label: string }) {
  return (
    <div className="flex items-center justify-end gap-2 sm:gap-2.5">
      <div className="relative h-1.5 w-8 overflow-hidden rounded-full bg-white/[0.06] sm:w-32" aria-hidden>
        <div
          className="absolute inset-y-0 left-0 rounded-full bg-foreground/70"
          style={{ width: `${max ? Math.min(value / max, 1) * 100 : 0}%` }}
        />
      </div>
      <span className="w-7 text-right text-[13px] font-medium text-foreground sm:w-11">{label}</span>
    </div>
  )
}

/** Players in recap order (sessions desc, then username). */
export function RankTable({ players, days }: { players: TrainingPlayer[]; days: number }) {
  const maxSessions = Math.max(...players.map((p) => p.sessions))
  const th = 'h-9 px-2 sm:px-3 text-xs font-medium text-muted-foreground whitespace-nowrap'
  return (
    <div className="overflow-x-auto">
    <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-line">
          <th scope="col" className={`${th} w-9 pl-4 text-right sm:w-10 sm:pl-4`}>#</th>
          <th scope="col" className={`${th} text-left`}>Player</th>
          <th scope="col" className={`${th} text-right`}>Sessions</th>
          <th scope="col" className={`${th} pr-4 text-right sm:pr-4`}>
            Days<span className="hidden sm:inline"> trained <span className="font-normal text-muted-foreground/70">of {days}</span></span>
          </th>
        </tr>
      </thead>
      <tbody>
        {players.map((p, i) => (
          <tr key={p.username} className="border-b border-line last:border-0">
            <td className="py-2.5 pr-2 pl-4 text-right sm:pr-3 text-[13px] text-muted-foreground">{i + 1}</td>
            <td className="max-w-24 truncate px-2 py-2.5 sm:max-w-40 sm:px-3 font-medium text-foreground" title={p.username}>{p.username}</td>
            <td className="px-2 py-2.5 sm:px-3"><Bar value={p.sessions} max={maxSessions} label={String(p.sessions)} /></td>
            <td className="py-2.5 pr-4 pl-2 sm:pl-3"><Bar value={p.days_trained} max={days} label={String(p.days_trained)} /></td>
          </tr>
        ))}
      </tbody>
    </table>
    </div>
  )
}

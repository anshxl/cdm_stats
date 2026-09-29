import type { ScrimMap } from '@/api/models'
import { MapTable } from '@/components/MapTable'
import { shortDate } from '@/lib/format'
import { cn } from '@/lib/utils'

type Result = ScrimMap['recent'][number]

// Margin is mode-specific: never mix SnD rounds with HP points in one column.
const MODE_META: Record<string, { name: string; unit: string }> = {
  SnD: { name: 'Search & Destroy', unit: 'rounds' },
  HP: { name: 'Hardpoint', unit: 'points' },
  Control: { name: 'Control', unit: 'rounds' },
}

const signed = (v: number | null) => (v == null ? '—' : `${v > 0 ? '+' : ''}${v.toFixed(1)}`)
const date = (d: string) => (/^\d{4}-\d{2}-\d{2}/.test(d) ? shortDate(d) : d)

/** Last results as letter chips, newest on the left. Neutral: a win is filled, a loss is an outline. */
function ResultStrip({ results }: { results: Result[] }) {
  if (!results.length) return <span className="text-muted-foreground">—</span>
  return (
    <span className="inline-flex gap-1" aria-label={`Last ${results.length}: ${results.map((r) => r.result).join(' ')}`}>
      {results.map((r, i) => (
        <span
          key={i}
          title={`${date(r.date)} vs ${r.opponent}: ${r.our_score}–${r.opp_score}`}
          className={cn(
            'grid size-5 place-items-center rounded-[4px] text-[10px] font-semibold',
            r.result === 'W' ? 'bg-foreground/85 text-page' : 'border border-foreground/25 text-muted-foreground',
          )}
          aria-hidden
        >
          {r.result}
        </span>
      ))}
    </span>
  )
}

function RecentDetail({ results }: { results: Result[] }) {
  if (!results.length) return <p className="text-xs text-muted-foreground">No results in this range.</p>
  return (
    <table className="text-xs">
      <caption className="mb-1.5 text-left text-muted-foreground">Last {results.length} results, newest first</caption>
      <tbody>
        {results.map((r, i) => (
          <tr key={i}>
            <td className="py-0.5 pr-5 text-muted-foreground">{date(r.date)}</td>
            <td className="py-0.5 pr-5 font-medium text-foreground">{r.opponent}</td>
            <td className="py-0.5 pr-5 text-foreground/90">{r.our_score}–{r.opp_score}</td>
            <td className={cn('py-0.5 font-semibold', r.result === 'W' ? 'text-foreground' : 'text-muted-foreground')}>
              {r.result === 'W' ? 'Win' : 'Loss'}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

/** One MapTable per mode, so each margin column keeps its own unit. */
export function ScrimMapTables({ maps }: { maps: ScrimMap[] }) {
  const modes = [...new Set(maps.map((m) => m.mode))]
  return (
    <div className="flex flex-col">
      {modes.map((mode, i) => {
        const meta = MODE_META[mode] ?? { name: mode, unit: 'margin' }
        const rows = maps.filter((m) => m.mode === mode)
        return (
          <section key={mode} className={cn(i > 0 && 'border-t border-line')}>
            <h3 className="flex items-baseline gap-2 px-4 pt-3.5 pb-1 text-[13px] font-semibold text-foreground">
              {meta.name}
              <span className="text-xs font-normal text-muted-foreground">{rows.length} {rows.length === 1 ? 'map' : 'maps'}</span>
            </h3>
            <MapTable
              rows={rows}
              rowKey={(r) => r.map_name}
              mapName={(r) => r.map_name}
              winRate={(r) => ({ value: r.win_pct, n: r.played })}
              columns={[
                { key: 'wl', header: 'W–L', render: (r) => `${r.wins}–${r.losses}` },
                { key: 'margin', header: <span title={`Average ${meta.unit} per map`}>Avg {meta.unit}</span>, hideOnMobile: true, width: 104, render: (r) => signed(r.avg_margin) },
                { key: 'recent', header: 'Last 5', align: 'left', hideOnMobile: true, width: 148, render: (r) => <ResultStrip results={r.recent} /> },
              ]}
              renderDetail={(r) => <RecentDetail results={r.recent} />}
            />
          </section>
        )
      })}
    </div>
  )
}

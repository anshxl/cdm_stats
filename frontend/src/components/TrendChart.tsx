import { useMemo } from 'react'
import {
  CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
  type TooltipContentProps,
} from 'recharts'
import { shortDate } from '@/lib/format'

/** Default series colors (dark-mode categorical order), used when a series has no color. */
export const SERIES_COLORS = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300', '#9085e9', '#e66767']

const DAY = 86_400_000

export interface TrendPoint {
  /** ISO date, "YYYY-MM-DD". */
  date: string
  value: number | null
}

export interface TrendSeries {
  key: string
  label: string
  color?: string
  points: TrendPoint[]
}

export interface TrendChartProps {
  series: TrendSeries[]
  height?: number
  /** Break a line when two consecutive points of a series are more than this many days apart. Default 21. */
  gapDays?: number
  referenceLine?: { y: number; label?: string }
  yDomain?: [number | 'auto' | 'dataMin' | 'dataMax', number | 'auto' | 'dataMin' | 'dataMax']
  formatValue?: (v: number) => string
  /** Show a dot on every point. Default true when every series has ≤ 20 points. */
  dots?: boolean
  /** Accessible name for the chart. */
  ariaLabel?: string
}

type Row = { t: number } & Record<string, number | null>

const toTime = (iso: string) => Date.parse(`${iso.slice(0, 10)}T00:00:00Z`)

/**
 * Build one row per timestamp. Each series is split into segments at gaps
 * > gapDays; each segment is its own Line key (`key~i`) so a line never
 * bridges a gap. Within a segment connectNulls is on, so dates that only
 * other series have do not break the line.
 */
function buildRows(series: TrendSeries[], gapDays: number) {
  const rows = new Map<number, Row>()
  const segments: { dataKey: string; series: TrendSeries; color: string; count: number }[] = []
  const row = (t: number): Row => {
    let r = rows.get(t)
    if (!r) {
      r = { t } as Row
      rows.set(t, r)
    }
    return r
  }

  series.forEach((s, si) => {
    const color = s.color ?? SERIES_COLORS[si % SERIES_COLORS.length]
    // Last value wins when a series has two points on one date.
    const byDate = new Map<number, number | null>()
    for (const p of s.points) byDate.set(toTime(p.date), p.value)
    const times = [...byDate.keys()].sort((a, b) => a - b)
    let seg = 0
    let prev: number | null = null
    for (const t of times) {
      if (prev !== null && t - prev > gapDays * DAY) {
        seg += 1
      }
      const dataKey = `${s.key}~${seg}`
      const existing = segments.find((x) => x.dataKey === dataKey)
      if (existing) existing.count += 1
      else segments.push({ dataKey, series: s, color, count: 1 })
      row(t)[dataKey] = byDate.get(t) ?? null
      prev = t
    }
  })

  return { data: [...rows.values()].sort((a, b) => a.t - b.t), segments }
}

function ChartTooltip({
  active, payload, label, formatValue, labels, colors,
}: TooltipContentProps<number, string> & {
  formatValue: (v: number) => string
  labels: Map<string, string>
  colors: Map<string, string>
}) {
  if (!active || !payload?.length) return null
  const seen = new Set<string>()
  const items = payload
    .filter((p) => typeof p.value === 'number')
    .map((p) => ({ key: String(p.dataKey).split('~')[0], value: p.value as number }))
    .filter((p) => (seen.has(p.key) ? false : (seen.add(p.key), true)))
    .sort((a, b) => b.value - a.value)
  if (!items.length) return null
  return (
    <div className="min-w-36 rounded-lg border border-line bg-popover px-3 py-2 text-xs shadow-lg shadow-black/40">
      <div className="mb-1.5 text-muted-foreground">{shortDate(new Date(Number(label)).toISOString())}</div>
      <ul className="flex flex-col gap-1">
        {items.map((it) => (
          <li key={it.key} className="flex items-center gap-2">
            <span className="h-0.5 w-3 rounded-full" style={{ background: colors.get(it.key) }} aria-hidden />
            <span className="font-semibold text-foreground">{formatValue(it.value)}</span>
            <span className="truncate text-muted-foreground">{labels.get(it.key)}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

/** Line chart over dates, one line per series, broken across long gaps (e.g. between seasons). */
export function TrendChart({
  series,
  height = 280,
  gapDays = 21,
  referenceLine,
  yDomain = ['auto', 'auto'],
  formatValue = (v) => v.toFixed(2),
  dots,
  ariaLabel,
}: TrendChartProps) {
  const { data, segments } = useMemo(() => buildRows(series, gapDays), [series, gapDays])
  const labels = useMemo(() => new Map(series.map((s) => [s.key, s.label])), [series])
  const colors = useMemo(() => new Map(segments.map((s) => [s.series.key, s.color])), [segments])
  const showDots = dots ?? series.every((s) => s.points.length <= 20)
  const axisTick = { fill: '#8a8f98', fontSize: 11 }

  return (
    <figure className="flex flex-col gap-3" aria-label={ariaLabel}>
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="#23252b" vertical={false} />
            <XAxis
              dataKey="t"
              type="number"
              scale="time"
              domain={['dataMin', 'dataMax']}
              tickFormatter={(t: number) => shortDate(new Date(t).toISOString())}
              tick={axisTick}
              tickLine={false}
              axisLine={{ stroke: '#23252b' }}
              minTickGap={28}
            />
            <YAxis
              domain={yDomain}
              tick={axisTick}
              tickLine={false}
              axisLine={false}
              width={48}
              tickFormatter={(v: number) => formatValue(v)}
            />
            {referenceLine && (
              <ReferenceLine
                y={referenceLine.y}
                stroke="#5b606a"
                strokeDasharray="4 4"
                label={
                  referenceLine.label
                    ? { value: referenceLine.label, position: 'insideTopRight', fill: '#8a8f98', fontSize: 11 }
                    : undefined
                }
              />
            )}
            <Tooltip
              cursor={{ stroke: '#5b606a', strokeWidth: 1 }}
              content={(props) => (
                <ChartTooltip
                  {...(props as TooltipContentProps<number, string>)}
                  formatValue={formatValue}
                  labels={labels}
                  colors={colors}
                />
              )}
            />
            {segments.map((s) => (
              <Line
                key={s.dataKey}
                dataKey={s.dataKey}
                name={s.series.label}
                type="linear"
                stroke={s.color}
                strokeWidth={2}
                dot={showDots || s.count === 1 ? { r: 2.5, fill: s.color, stroke: '#141519', strokeWidth: 1.5 } : false}
                activeDot={{ r: 4.5, fill: s.color, stroke: '#141519', strokeWidth: 2 }}
                connectNulls
                isAnimationActive={false}
                legendType="none"
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
      {series.length > 1 && (
        <figcaption>
          <ul className="flex flex-wrap gap-x-4 gap-y-1.5 text-xs text-muted-foreground">
            {series.map((s) => (
              <li key={s.key} className="flex items-center gap-1.5">
                <span className="h-0.5 w-3.5 rounded-full" style={{ background: colors.get(s.key) }} aria-hidden />
                {s.label}
              </li>
            ))}
          </ul>
        </figcaption>
      )}
    </figure>
  )
}

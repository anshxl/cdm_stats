import { useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { EloSeries, TeamInfo } from '@/api/models'
import { TeamBadge } from '@/components/TeamBadge'
import { buildBreakAxis } from '@/lib/breakAxis'
import { shortDate } from '@/lib/format'
import { teamLineColor } from '@/lib/teamColor'
import { cn } from '@/lib/utils'

const DAY = 86_400_000
/** A line breaks when two matches are more than this far apart (the gap between seasons). */
const GAP_DAYS = 21
/** Same-day matches are spread this far apart so each one gets its own point. */
const SAME_DAY_STEP = 4 * 3_600_000
const HOVER_RADIUS = 28
/** Width of the band that stands in for a cut-out gap on the x-axis. */
const BREAK_PX = 30

const MUTED = '#41444c'
const HOVER = '#e7e8ea'
const GRID = '#23252b'
const AXIS_TEXT = '#8a8f98'
const SURFACE = '#141519'

const M = { top: 12, right: 64, bottom: 26, left: 44 }

type ApiPoint = EloSeries['points'][number]

interface Pt {
  x: number
  elo: number
  p: ApiPoint
}

interface Line {
  team: TeamInfo
  key: string
  low: boolean
  color: string
  segments: Pt[][]
  last: Pt | null
}

const toTime = (iso: string) => Date.parse(`${iso.slice(0, 10)}T00:00:00Z`)

function buildLines(series: EloSeries[]): Line[] {
  return series.map((s) => {
    const segments: Pt[][] = []
    let prevDay: number | null = null
    let sameDay = 0
    for (const p of s.points) {
      const day = toTime(p.match_date)
      sameDay = day === prevDay ? sameDay + 1 : 0
      if (prevDay === null || day - prevDay > GAP_DAYS * DAY) segments.push([])
      segments[segments.length - 1].push({
        x: day + sameDay * SAME_DAY_STEP,
        elo: p.elo,
        p,
      })
      prevDay = day
    }
    const lastSeg = segments[segments.length - 1]
    return {
      team: s.team,
      key: s.team.abbreviation,
      low: s.low_confidence,
      color: teamLineColor(s.team),
      segments,
      last: lastSeg ? lastSeg[lastSeg.length - 1] : null,
    }
  })
}

function niceStep(range: number): number {
  for (const s of [10, 25, 50, 100]) if (range / s <= 7) return s
  return 200
}

function useWidth<T extends HTMLElement>() {
  const ref = useRef<T>(null)
  const [width, setWidth] = useState(0)
  useLayoutEffect(() => {
    const el = ref.current
    if (!el) return
    const ro = new ResizeObserver(([e]) => setWidth(Math.floor(e.contentRect.width)))
    ro.observe(el)
    setWidth(Math.floor(el.getBoundingClientRect().width))
    return () => ro.disconnect()
  }, [])
  return [ref, width] as const
}

export interface EloTrajectoryChartProps {
  series: EloSeries[]
  seed: number
  focus: string | null
  onFocus: (abbr: string) => void
  height?: number
}

/** Every team's Elo over time: the focus team in its color, the rest muted grey. Hover a line or legend item to lift it. */
export function EloTrajectoryChart({ series, seed, focus, onFocus, height: fullHeight = 400 }: EloTrajectoryChartProps) {
  const lines = useMemo(() => buildLines(series), [series])
  const [wrapRef, width] = useWidth<HTMLDivElement>()
  // Shorter on phones so the narrow plot is not a tall sliver.
  const height = width > 0 && width < 520 ? 300 : fullHeight
  const [hoverKey, setHoverKey] = useState<string | null>(null)
  const [hoverPt, setHoverPt] = useState<{ key: string; pt: Pt } | null>(null)

  const all = lines.flatMap((l) => l.segments.flat())
  const ys = all.map((p) => p.elo).concat(seed)
  const step = niceStep(Math.max(...ys) - Math.min(...ys))
  const y0 = Math.floor((Math.min(...ys) - step / 4) / step) * step
  const y1 = Math.ceil((Math.max(...ys) + step / 4) / step) * step

  const innerW = Math.max(0, width - M.left - M.right)
  const innerH = height - M.top - M.bottom
  // Long spans with no matches in any team are cut out and drawn as a narrow band.
  const axis = useMemo(
    () => buildBreakAxis(lines.flatMap((l) => l.segments.flat().map((p) => p.x)), { gapDays: GAP_DAYS, width: innerW, breakWidth: BREAK_PX }),
    [lines, innerW],
  )
  const sx = (x: number) => M.left + axis.toX(x)
  const sy = (y: number) => M.top + (1 - (y - y0) / (y1 - y0)) * innerH

  const yTicks: number[] = []
  for (let v = y0; v <= y1; v += step) yTicks.push(v)
  const xTicks = axis.ticks(Math.max(2, Math.round(innerW / 110)))
  const narrow = innerW < 480

  const lit = hoverPt?.key ?? hoverKey
  const byKey = new Map(lines.map((l) => [l.key, l]))
  const focusLine = focus ? byKey.get(focus) : undefined
  const litLine = lit && lit !== focus ? byKey.get(lit) : undefined
  const muted = lines.filter((l) => l !== focusLine && l !== litLine)

  const path = (seg: Pt[]) => seg.map((p, i) => `${i ? 'L' : 'M'}${sx(p.x).toFixed(1)},${sy(p.elo).toFixed(1)}`).join('')

  function onMove(e: React.PointerEvent<SVGSVGElement>) {
    const r = e.currentTarget.getBoundingClientRect()
    const mx = e.clientX - r.left
    const my = e.clientY - r.top
    let best: { key: string; pt: Pt } | null = null
    let bestD = HOVER_RADIUS
    for (const l of lines) {
      for (const pt of l.segments.flat()) {
        const d = Math.hypot(sx(pt.x) - mx, sy(pt.elo) - my)
        // Ties go to the focus team so it is never hidden behind a grey line.
        if (d < bestD || (d === bestD && l.key === focus)) {
          bestD = d
          best = { key: l.key, pt }
        }
      }
    }
    setHoverPt(best)
  }

  // End labels for the focus and lit lines, nudged apart when they collide.
  const endLabels = [focusLine, litLine]
    .filter((l): l is Line => Boolean(l?.last))
    .map((l) => ({ l, y: sy(l.last!.elo) }))
  if (endLabels.length === 2 && Math.abs(endLabels[0].y - endLabels[1].y) < 14) {
    const [a, b] = endLabels
    const mid = (a.y + b.y) / 2
    const up = a.y <= b.y ? a : b
    const down = up === a ? b : a
    up.y = mid - 7
    down.y = mid + 7
  }

  const drawLine = (l: Line, stroke: string, w: number, dots: boolean) => (
    <g key={l.key} pointerEvents="none">
      {l.segments.map((seg, i) =>
        seg.length === 1 || dots ? (
          <g key={i}>
            {seg.length > 1 && <path d={path(seg)} fill="none" stroke={stroke} strokeWidth={w} strokeLinejoin="round" strokeLinecap="round" />}
            {seg.map((p, j) => (
              <circle key={j} cx={sx(p.x)} cy={sy(p.elo)} r={seg.length === 1 ? w + 0.5 : 2.5} fill={stroke} stroke={SURFACE} strokeWidth={1.5} />
            ))}
          </g>
        ) : (
          <path key={i} d={path(seg)} fill="none" stroke={stroke} strokeWidth={w} strokeLinejoin="round" strokeLinecap="round" />
        ),
      )}
    </g>
  )

  const tip = hoverPt ? { line: byKey.get(hoverPt.key)!, pt: hoverPt.pt } : null
  const tipLeft = tip ? sx(tip.pt.x) : 0
  const tipFlip = tipLeft > width - 220

  const legend = [...lines].sort((a, b) => (b.last?.elo ?? 0) - (a.last?.elo ?? 0))

  return (
    <figure className="flex flex-col gap-4" aria-label="Elo rating over time by team">
      <div ref={wrapRef} className="relative" style={{ height }}>
        {width > 0 && (
          <svg
            width={width}
            height={height}
            className="block touch-pan-y select-none"
            onPointerMove={onMove}
            onPointerLeave={() => setHoverPt(null)}
            role="img"
            aria-label={focusLine?.last ? `${focusLine.team.team_name} Elo ${focusLine.last.elo.toFixed(0)}` : 'Elo trajectory'}
          >
            {yTicks.map((v) => (
              <g key={v}>
                <line x1={M.left} x2={width - M.right} y1={sy(v)} y2={sy(v)} stroke={GRID} />
                <text x={M.left - 8} y={sy(v)} dy="0.32em" textAnchor="end" fill={AXIS_TEXT} fontSize={11}>{v}</text>
              </g>
            ))}
            {xTicks.map(({ t, x }) => (
              <text key={t} x={M.left + x} y={height - 6} textAnchor="middle" fill={AXIS_TEXT} fontSize={11}>
                {shortDate(new Date(t).toISOString())}
              </text>
            ))}
            {axis.breaks.map((b) => {
              const cx = M.left + (b.x0 + b.x1) / 2
              const cy = M.top + innerH / 2
              return (
                <g key={b.from} pointerEvents="none">
                  <rect x={M.left + b.x0} y={M.top} width={b.x1 - b.x0} height={innerH} fill="#ffffff" fillOpacity={0.035} />
                  <text x={cx} y={cy} transform={`rotate(-90 ${cx} ${cy})`} textAnchor="middle" dy="0.32em" fill="#6b707a" fontSize={10}>
                    {narrow ? 'No matches' : `No matches · ${shortDate(new Date(b.from).toISOString())} – ${shortDate(new Date(b.to).toISOString())}`}
                  </text>
                </g>
              )
            })}
            <line x1={M.left} x2={width - M.right} y1={sy(seed)} y2={sy(seed)} stroke="#5b606a" strokeDasharray="4 4" />
            <text x={width - M.right + 6} y={sy(seed)} dy="0.32em" fill={AXIS_TEXT} fontSize={11}>Seed</text>

            {muted.map((l) => drawLine(l, MUTED, 1.25, false))}
            {litLine && drawLine(litLine, HOVER, 2, false)}
            {focusLine && drawLine(focusLine, focusLine.color, 2.5, focusLine.segments.flat().length <= 20)}

            {endLabels.map(({ l, y }) => (
              <text key={l.key} x={sx(l.last!.x) + 8} y={y} dy="0.32em" fontSize={12} fontWeight={600} fill="#e7e8ea">
                {l.key} <tspan fontWeight={400} fill={AXIS_TEXT}>{l.last!.elo.toFixed(0)}</tspan>
              </text>
            ))}

            {tip && (
              <circle cx={sx(tip.pt.x)} cy={sy(tip.pt.elo)} r={4.5} pointerEvents="none"
                fill={tip.line === focusLine ? tip.line.color : HOVER} stroke={SURFACE} strokeWidth={2} />
            )}
          </svg>
        )}

        {tip && (
          <div
            className="pointer-events-none absolute z-10 w-max max-w-56 rounded-lg border border-line bg-popover px-3 py-2 text-xs shadow-lg shadow-black/40"
            style={{
              left: tipFlip ? undefined : tipLeft + 12,
              right: tipFlip ? width - tipLeft + 12 : undefined,
              top: Math.max(0, Math.min(sy(tip.pt.elo) - 24, height - 110)),
            }}
          >
            <div className="flex items-center gap-2">
              <TeamBadge team={tip.line.team} size="sm" />
              <span className="font-semibold text-foreground">{tip.line.team.abbreviation}</span>
              <span className="ml-auto pl-3 text-sm font-semibold text-foreground">{tip.pt.elo.toFixed(0)}</span>
            </div>
            <div className="mt-1.5 text-muted-foreground">
              <span className={tip.pt.p.result === 'W' ? 'text-up' : 'text-down'}>{tip.pt.p.result}</span>
              {' vs '}{tip.pt.p.opponent}
            </div>
            <div className="text-muted-foreground">{shortDate(tip.pt.p.match_date)} · {tip.pt.p.label}</div>
            {tip.line.low && <div className="mt-1 text-muted-foreground">Low confidence: fewer than 7 matches</div>}
          </div>
        )}
      </div>

      <figcaption>
        <ul className="flex flex-wrap gap-1.5" onMouseLeave={() => setHoverKey(null)}>
          {legend.map((l) => {
            const isFocus = l.key === focus
            const isLit = l.key === lit && !isFocus
            return (
              <li key={l.key}>
                <button
                  type="button"
                  onMouseEnter={() => setHoverKey(l.key)}
                  onFocus={() => setHoverKey(l.key)}
                  onBlur={() => setHoverKey(null)}
                  onClick={() => onFocus(l.key)}
                  aria-pressed={isFocus}
                  title={isFocus ? `${l.team.team_name} (focus)` : `Focus ${l.team.team_name}`}
                  className={cn(
                    'flex h-7 items-center gap-1.5 rounded-md border px-2 text-xs transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gold/40',
                    isFocus ? 'border-line bg-raised text-foreground' : 'border-transparent text-muted-foreground hover:bg-white/[0.04] hover:text-foreground',
                    isLit && 'bg-white/[0.04] text-foreground',
                  )}
                >
                  <span
                    className="h-0.5 w-3 rounded-full"
                    style={{ background: isFocus ? l.color : isLit ? HOVER : '#5b606a' }}
                    aria-hidden
                  />
                  <span className="font-medium">{l.key}</span>
                  {l.last && <span className="text-muted-foreground">{l.last.elo.toFixed(0)}</span>}
                  {l.low && <span className="text-[10px] text-muted-foreground/80" title="Low confidence: fewer than 7 matches">low conf.</span>}
                </button>
              </li>
            )
          })}
        </ul>
      </figcaption>
    </figure>
  )
}

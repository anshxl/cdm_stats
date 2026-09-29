/**
 * A time axis with long empty spans cut out. Dates are grouped into segments
 * wherever consecutive dates are more than `gapDays` apart; each segment keeps
 * a linear time scale, sized in proportion to its duration, and each gap is
 * replaced by a fixed `breakWidth` band. With no gap it is a plain linear scale.
 *
 * Erasable-only TypeScript so Node can run it (and its tests) directly.
 */

const DAY = 86_400_000

export interface BreakAxisOptions {
  /** A gap is more than this many days between consecutive dates. */
  gapDays: number
  /** Output range is [0, width]. */
  width: number
  /** Width of each break band, in output units. */
  breakWidth: number
}

export interface AxisSegment {
  start: number
  end: number
  x0: number
  x1: number
}

export interface AxisBreak {
  /** Last date before the gap. */
  from: number
  /** First date after the gap. */
  to: number
  x0: number
  x1: number
}

export interface AxisTick {
  t: number
  x: number
}

export interface BreakAxis {
  toX(t: number): number
  segments: AxisSegment[]
  breaks: AxisBreak[]
  /** About `count` ticks (UTC midnights) spread across segments by width; at least one per segment. */
  ticks(count?: number): AxisTick[]
}

export function buildBreakAxis(dates: number[], opts: BreakAxisOptions): BreakAxis {
  const { gapDays, width, breakWidth } = opts
  const sorted = [...new Set(dates)].sort((a, b) => a - b)
  if (!sorted.length) return { toX: () => width / 2, segments: [], breaks: [], ticks: () => [] }

  // Group into [start, end] runs.
  const runs: { start: number; end: number }[] = [{ start: sorted[0], end: sorted[0] }]
  for (const t of sorted.slice(1)) {
    const cur = runs[runs.length - 1]
    if (t - cur.end > gapDays * DAY) runs.push({ start: t, end: t })
    else cur.end = t
  }

  const avail = Math.max(0, width - (runs.length - 1) * breakWidth)
  const total = runs.reduce((s, r) => s + (r.end - r.start), 0)
  // All segments zero-length (e.g. single dates): share the width equally.
  const share = (r: { start: number; end: number }) => (total > 0 ? (r.end - r.start) / total : 1 / runs.length)

  const segments: AxisSegment[] = []
  const breaks: AxisBreak[] = []
  let x = 0
  runs.forEach((r, i) => {
    if (i > 0) {
      breaks.push({ from: runs[i - 1].end, to: r.start, x0: x, x1: x + breakWidth })
      x += breakWidth
    }
    const w = avail * share(r)
    segments.push({ start: r.start, end: r.end, x0: x, x1: x + w })
    x += w
  })

  const inSeg = (s: AxisSegment, t: number) =>
    s.end === s.start ? (s.x0 + s.x1) / 2 : s.x0 + ((t - s.start) / (s.end - s.start)) * (s.x1 - s.x0)

  function toX(t: number): number {
    const first = segments[0]
    const last = segments[segments.length - 1]
    if (t <= first.start) return first.end === first.start ? inSeg(first, t) : first.x0
    if (t >= last.end) return last.end === last.start ? inSeg(last, t) : last.x1
    for (let i = 0; i < segments.length; i++) {
      const s = segments[i]
      if (t <= s.end) {
        if (t >= s.start) return inSeg(s, t)
        // Inside a gap: slide across the break band.
        const b = breaks[i - 1]
        return b.x0 + ((t - b.from) / (b.to - b.from)) * breakWidth
      }
    }
    return last.x1
  }

  function ticks(count = 6): AxisTick[] {
    const out: AxisTick[] = []
    const segW = segments.reduce((s, g) => s + (g.x1 - g.x0), 0)
    for (const s of segments) {
      const lo = Math.ceil(s.start / DAY) * DAY
      const hi = Math.floor(s.end / DAY) * DAY
      if (lo > hi) continue
      const k = Math.max(1, Math.round((count * (s.x1 - s.x0)) / (segW || 1)))
      const seen = new Set<number>()
      for (let i = 0; i < k; i++) {
        const raw = s.start + ((i + 0.5) / k) * (s.end - s.start)
        const t = Math.min(hi, Math.max(lo, Math.round(raw / DAY) * DAY))
        if (seen.has(t)) continue
        seen.add(t)
        out.push({ t, x: toX(t) })
      }
    }
    return out
  }

  return { toX, segments, breaks, ticks }
}

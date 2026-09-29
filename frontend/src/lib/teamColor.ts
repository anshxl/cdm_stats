import type { TeamInfo } from '@/api/models'

function luminance(hex: string): number {
  const m = /^#?([0-9a-f]{6})$/i.exec(hex)
  if (!m) return 0
  const n = parseInt(m[1], 16)
  const [r, g, b] = [n >> 16, (n >> 8) & 255, n & 255].map((c) => {
    const s = c / 255
    return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4
  })
  return 0.2126 * r + 0.7152 * g + 0.0722 * b
}

/** Line color for a team on the dark surface: primary, or secondary when primary is too dark to see. */
export function teamLineColor(team: TeamInfo, fallback = '#8a8f98'): string {
  for (const c of [team.primary_color, team.secondary_color]) {
    if (c && luminance(c) > 0.045) return c
  }
  return fallback
}

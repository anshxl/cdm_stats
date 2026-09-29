/** 0.583 -> "58%". null -> "—". */
export function pct(v: number | null | undefined, digits = 0): string {
  return v == null ? '—' : `${(v * 100).toFixed(digits)}%`
}

/** Fixed decimals, null -> "—". */
export function num(v: number | null | undefined, digits = 0): string {
  return v == null ? '—' : v.toFixed(digits)
}

/** "2026-03-12" -> "Mar 12" (UTC, no timezone drift). */
export function shortDate(iso: string): string {
  return new Date(`${iso.slice(0, 10)}T00:00:00Z`).toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', timeZone: 'UTC',
  })
}

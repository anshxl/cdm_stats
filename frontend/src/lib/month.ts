/** Month keys are "YYYY-MM". Training months follow the bot's time zone. */
const MONTH = /^\d{4}-(0[1-9]|1[0-2])$/
// Must match the bot's TIMEZONE on Railway: months roll over at its midnight.
const TIMEZONE = 'Asia/Kolkata'

export function readMonth(v: string | null): string | null {
  return v && MONTH.test(v) ? v : null
}

/** The month `now` falls in, in New York time. */
export function currentMonth(now: Date = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: TIMEZONE, year: 'numeric', month: '2-digit' })
    .formatToParts(now)
  const get = (type: string) => parts.find((p) => p.type === type)?.value
  return `${get('year')}-${get('month')}`
}

/** Today's day of the month in New York time. */
export function currentDay(now: Date = new Date()): number {
  return Number(new Intl.DateTimeFormat('en-US', { timeZone: TIMEZONE, day: 'numeric' }).format(now))
}

export function daysInMonth(month: string): number {
  const [y, m] = month.split('-').map(Number)
  return new Date(Date.UTC(y, m, 0)).getUTCDate()
}

/** "2026-09" -> "September 2026". */
export function monthLabel(month: string): string {
  return new Date(`${month}-01T00:00:00Z`).toLocaleDateString('en-US', {
    month: 'long', year: 'numeric', timeZone: 'UTC',
  })
}

/** Picker options, newest first. The current and selected months are always offered. */
export function monthOptions(months: string[], current: string, selected: string): string[] {
  return [...new Set([...months, current, selected])].sort().reverse()
}

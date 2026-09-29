import { useCallback, useMemo } from 'react'
import { useSearchParams } from 'react-router'

export const EVENTS = ['all', 'spring', 'summer', 'regionals'] as const
export type EventKey = (typeof EVENTS)[number]

export interface Filter {
  event: EventKey
  /** Inclusive ISO date (YYYY-MM-DD) or null. */
  start: string | null
  end: string | null
}

/** Query params for the API. `event` is left out for scrims (dates only). */
export interface FilterParams {
  event?: EventKey
  start?: string
  end?: string
}

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/

function readDate(v: string | null): string | null {
  return v && ISO_DATE.test(v) ? v : null
}

/** The shared filter, stored in the URL query (`event`, `start`, `end`). */
export function useFilter() {
  const [searchParams, setSearchParams] = useSearchParams()

  const rawEvent = searchParams.get('event')
  const event: EventKey = (EVENTS as readonly string[]).includes(rawEvent ?? '')
    ? (rawEvent as EventKey)
    : 'all'
  const start = readDate(searchParams.get('start'))
  const end = readDate(searchParams.get('end'))

  const filter = useMemo<Filter>(() => ({ event, start, end }), [event, start, end])

  /** Params for league endpoints (event + dates). */
  const params = useMemo<FilterParams>(() => {
    const p: FilterParams = { event }
    if (start) p.start = start
    if (end) p.end = end
    return p
  }, [event, start, end])

  /** Params for scrim endpoints (dates only). */
  const dateParams = useMemo<FilterParams>(() => {
    const p: FilterParams = {}
    if (start) p.start = start
    if (end) p.end = end
    return p
  }, [start, end])

  const update = useCallback(
    (changes: Record<string, string | null>) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          for (const [k, v] of Object.entries(changes)) {
            if (v) next.set(k, v)
            else next.delete(k)
          }
          return next
        },
        { replace: true },
      )
    },
    [setSearchParams],
  )

  const setEvent = useCallback((e: EventKey) => update({ event: e === 'all' ? null : e }), [update])

  return { filter, params, dateParams, setEvent }
}

/** Query keys carried across page switches. */
export const SHARED_KEYS = ['event', 'start', 'end', 'team'] as const

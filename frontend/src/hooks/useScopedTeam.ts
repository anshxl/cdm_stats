import { useCallback, useEffect } from 'react'
import { useSearchParams } from 'react-router'
import type { TeamInfo } from '@/api/models'

export const HOME_TEAM = 'GL'

/** Spec fallback: keep `current` if it is an option, else GL, else the first option. */
export function resolveTeam(options: TeamInfo[], current: string | null): string | null {
  const abbrs = options.map((o) => o.abbreviation)
  if (current && abbrs.includes(current)) return current
  if (abbrs.includes(HOME_TEAM)) return HOME_TEAM
  return abbrs[0] ?? null
}

/**
 * A team selection stored in the URL under `param` (default `team`), kept valid
 * against `options`. Returns null while options load or when there are none.
 * When the URL value is not an option, the URL is rewritten to the fallback.
 */
export function useScopedTeam(
  options: TeamInfo[] | null,
  param = 'team',
): [string | null, (abbr: string) => void] {
  const [searchParams, setSearchParams] = useSearchParams()
  const current = searchParams.get(param)
  const resolved = options ? resolveTeam(options, current) : null

  const setTeam = useCallback(
    (abbr: string) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          next.set(param, abbr)
          return next
        },
        { replace: true },
      )
    },
    [param, setSearchParams],
  )

  useEffect(() => {
    if (resolved && resolved !== current) setTeam(resolved)
  }, [resolved, current, setTeam])

  return [resolved, setTeam]
}

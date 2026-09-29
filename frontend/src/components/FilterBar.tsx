import type { ReactNode } from 'react'
import { EVENTS, useFilter, type EventKey } from '@/hooks/useFilter'
import { cn } from '@/lib/utils'

const EVENT_LABEL: Record<EventKey, string> = {
  all: 'All',
  spring: 'Spring',
  summer: 'Summer',
  regionals: 'Regionals',
}

export interface FilterBarProps {
  /** The page's selects (TeamSelect etc.), placed after the shared filter. */
  children?: ReactNode
  /** Hides the event control (Scrims have no event). */
  hideEvent?: boolean
}

/** Shared top bar: event segmented control, then the page's selects. */
export function FilterBar({ children, hideEvent }: FilterBarProps) {
  const { filter, setEvent } = useFilter()

  return (
    <div className="sticky top-0 z-20 border-b border-line bg-page/90 backdrop-blur supports-[backdrop-filter]:bg-page/75">
      <div className="flex flex-wrap items-center gap-x-5 gap-y-2.5 px-4 py-2.5 md:px-6">
        {!hideEvent && (
          <div role="radiogroup" aria-label="Event" className="inline-flex h-8 rounded-md border border-line bg-surface p-0.5">
            {EVENTS.map((e) => {
              const selected = filter.event === e
              return (
                <button
                  key={e}
                  type="button"
                  role="radio"
                  aria-checked={selected}
                  onClick={() => setEvent(e)}
                  className={cn(
                    'rounded-[5px] px-2.5 text-[13px] font-medium transition-colors sm:px-3',
                    selected ? 'bg-gold text-page' : 'text-muted-foreground hover:text-foreground',
                  )}
                >
                  {EVENT_LABEL[e]}
                </button>
              )
            })}
          </div>
        )}

        {children && <div className="flex flex-wrap items-center gap-x-4 gap-y-2.5">{children}</div>}
      </div>
    </div>
  )
}

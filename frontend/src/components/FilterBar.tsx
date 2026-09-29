import type { ReactNode } from 'react'
import { X } from 'lucide-react'
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
  /** Disables the event control and shows this note (Scrims: "Scrims: dates only"). */
  eventDisabledNote?: string
}

const dateInput =
  'h-8 w-[8.75rem] rounded-md border border-line bg-surface px-2 text-[13px] text-foreground outline-none hover:bg-raised focus-visible:border-gold focus-visible:outline-none'

/** Shared top bar: event segmented control, date range, then the page's selects. */
export function FilterBar({ children, eventDisabledNote }: FilterBarProps) {
  const { filter, setEvent, setStart, setEnd, clearDates } = useFilter()
  const eventDisabled = Boolean(eventDisabledNote)

  return (
    <div className="sticky top-0 z-20 border-b border-line bg-page/90 backdrop-blur supports-[backdrop-filter]:bg-page/75">
      <div className="flex flex-wrap items-center gap-x-5 gap-y-2.5 px-4 py-2.5 md:px-6">
        <div className="flex items-center gap-2.5">
          <div
            role="radiogroup"
            aria-label="Event"
            aria-disabled={eventDisabled || undefined}
            className={cn('inline-flex h-8 rounded-md border border-line bg-surface p-0.5', eventDisabled && 'opacity-45')}
          >
            {EVENTS.map((e) => {
              const selected = !eventDisabled && filter.event === e
              return (
                <button
                  key={e}
                  type="button"
                  role="radio"
                  aria-checked={selected}
                  disabled={eventDisabled}
                  onClick={() => setEvent(e)}
                  className={cn(
                    'rounded-[5px] px-2.5 text-[13px] font-medium transition-colors disabled:cursor-not-allowed sm:px-3',
                    selected ? 'bg-gold text-page' : 'text-muted-foreground enabled:hover:text-foreground',
                  )}
                >
                  {EVENT_LABEL[e]}
                </button>
              )
            })}
          </div>
          {eventDisabledNote && <span className="text-xs text-muted-foreground">{eventDisabledNote}</span>}
        </div>

        <div className="flex items-center gap-1.5">
          <input
            type="date"
            aria-label="Start date"
            className={dateInput}
            value={filter.start ?? ''}
            max={filter.end ?? undefined}
            onChange={(ev) => setStart(ev.target.value || null)}
          />
          <span className="text-xs text-muted-foreground" aria-hidden>to</span>
          <input
            type="date"
            aria-label="End date"
            className={dateInput}
            value={filter.end ?? ''}
            min={filter.start ?? undefined}
            onChange={(ev) => setEnd(ev.target.value || null)}
          />
          {(filter.start || filter.end) && (
            <button
              type="button"
              onClick={clearDates}
              aria-label="Clear dates"
              title="Clear dates"
              className="grid size-8 place-items-center rounded-md text-muted-foreground hover:bg-white/[0.05] hover:text-foreground"
            >
              <X className="size-4" />
            </button>
          )}
        </div>

        {children && <div className="flex flex-wrap items-center gap-x-4 gap-y-2.5">{children}</div>}
      </div>
    </div>
  )
}

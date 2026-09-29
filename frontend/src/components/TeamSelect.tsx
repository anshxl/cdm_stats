import type { TeamInfo } from '@/api/models'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { cn } from '@/lib/utils'
import { TeamBadge } from './TeamBadge'

const ALL = '__all__'

export interface TeamSelectProps {
  /** Visible label before the trigger, e.g. "Team" or "Opponent". */
  label: string
  options: TeamInfo[]
  /** Selected abbreviation. null = nothing selected (or "all" when `allLabel` is set). */
  value: string | null
  /** Called with an abbreviation, or null when the "all" item is picked. */
  onChange: (abbr: string | null) => void
  /** Adds a first item that clears the selection, e.g. "All opponents". */
  allLabel?: string
  disabled?: boolean
  className?: string
}

export function TeamSelect({ label, options, value, onChange, allLabel, disabled, className }: TeamSelectProps) {
  const selectValue = value ?? (allLabel ? ALL : undefined)
  return (
    <label className={cn('flex items-center gap-2 text-[13px] text-muted-foreground', className)}>
      <span className="shrink-0">{label}</span>
      <Select
        value={selectValue}
        onValueChange={(v) => onChange(v === ALL ? null : v)}
        disabled={disabled || options.length === 0}
      >
        <SelectTrigger
          aria-label={label}
          className="h-8 min-w-28 border-line bg-surface text-foreground hover:bg-raised focus-visible:border-gold focus-visible:ring-gold/30 dark:bg-surface"
        >
          <SelectValue placeholder={options.length ? 'Select' : 'No teams'} />
        </SelectTrigger>
        <SelectContent position="popper" align="start" className="max-h-80 border-line">
          {allLabel && <SelectItem value={ALL}>{allLabel}</SelectItem>}
          {options.map((t) => (
            <SelectItem key={t.abbreviation} value={t.abbreviation} title={t.team_name}>
              <TeamBadge team={t} size="sm" showName />
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </label>
  )
}

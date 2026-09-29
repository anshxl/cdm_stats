import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { cn } from '@/lib/utils'

export const MODES = ['SnD', 'HP', 'Control'] as const
export type Mode = (typeof MODES)[number]

/** All / SnD / HP / Control, styled like the FilterBar event control. */
export function ModeToggle({ value, onChange }: { value: Mode | null; onChange: (m: Mode | null) => void }) {
  const items: { key: Mode | null; label: string }[] = [{ key: null, label: 'All modes' }, ...MODES.map((m) => ({ key: m, label: m }))]
  return (
    <div className="flex items-center gap-2 text-[13px] text-muted-foreground">
      <span className="shrink-0">Mode</span>
      <div role="radiogroup" aria-label="Mode" className="inline-flex h-8 rounded-md border border-line bg-surface p-0.5">
        {items.map((it) => {
          const selected = value === it.key
          return (
            <button
              key={it.label}
              type="button"
              role="radio"
              aria-checked={selected}
              onClick={() => onChange(it.key)}
              className={cn(
                'rounded-[5px] px-2.5 text-[13px] font-medium transition-colors focus-visible:outline-2 focus-visible:outline-gold',
                selected ? 'bg-gold text-page' : 'text-muted-foreground hover:text-foreground',
              )}
            >
              {it.label}
            </button>
          )
        })}
      </div>
    </div>
  )
}

const ALL = '__all__'

/** Map select; options come from /api/scrims/options for the current mode. */
export function MapSelect({ options, value, onChange }: { options: string[]; value: string | null; onChange: (m: string | null) => void }) {
  return (
    <label className="flex items-center gap-2 text-[13px] text-muted-foreground">
      <span className="shrink-0">Map</span>
      <Select value={value ?? ALL} onValueChange={(v) => onChange(v === ALL ? null : v)} disabled={options.length === 0}>
        <SelectTrigger
          aria-label="Map"
          className="h-8 min-w-32 border-line bg-surface text-foreground hover:bg-raised focus-visible:border-gold focus-visible:ring-gold/30 dark:bg-surface"
        >
          <SelectValue />
        </SelectTrigger>
        <SelectContent position="popper" align="start" className="max-h-80 border-line">
          <SelectItem value={ALL}>All maps</SelectItem>
          {options.map((m) => (
            <SelectItem key={m} value={m}>{m}</SelectItem>
          ))}
        </SelectContent>
      </Select>
    </label>
  )
}

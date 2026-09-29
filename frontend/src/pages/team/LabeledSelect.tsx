import type { ReactNode } from 'react'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

export interface SelectOption {
  value: string
  label: ReactNode
  title?: string
}

/** Label + dropdown with free-form options. TeamSelect only allows one extra
 * "all" item, and the Series select needs "Last 10" and "All" before the teams. */
export function LabeledSelect({
  label, value, options, onChange,
}: { label: string; value: string; options: SelectOption[]; onChange: (v: string) => void }) {
  return (
    <label className="flex items-center gap-2 text-[13px] text-muted-foreground">
      <span className="shrink-0">{label}</span>
      <Select value={value} onValueChange={onChange}>
        <SelectTrigger
          aria-label={label}
          className="h-8 min-w-24 border-line bg-surface text-foreground hover:bg-raised focus-visible:border-gold focus-visible:ring-gold/30 dark:bg-surface"
        >
          <SelectValue />
        </SelectTrigger>
        <SelectContent position="popper" align="start" className="max-h-80 border-line">
          {options.map((o) => (
            <SelectItem key={o.value} value={o.value} title={o.title}>
              {o.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </label>
  )
}

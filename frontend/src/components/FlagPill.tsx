import type { Flag, Tag } from '@/api/models'
import { cn } from '@/lib/utils'

export type FlagPillProps =
  | { flag: Flag; tag?: never; className?: string }
  | { tag: Tag; flag?: never; className?: string }

const FLAG_STYLE: Record<Flag['kind'], string> = {
  up: 'bg-up/14 text-up',
  down: 'bg-down/14 text-down',
  low_sample: 'bg-white/6 text-muted-foreground',
}

const TAG_STYLE: Record<Tag, { label: string; className: string }> = {
  pick: { label: 'SUGGESTED PICK', className: 'bg-up/14 text-up' },
  ban: { label: 'SUGGESTED BAN', className: 'bg-down/14 text-down' },
  they_ban: { label: 'THEY LIKELY BAN', className: 'bg-gold/14 text-gold' },
}

/** Soft pill for an API flag (up / down / low_sample) or an H2H tag (pick / ban / they_ban). */
export function FlagPill(props: FlagPillProps) {
  const { label, className } = props.tag
    ? TAG_STYLE[props.tag]
    : { label: props.flag.label, className: FLAG_STYLE[props.flag.kind] }
  return (
    <span
      className={cn(
        'inline-flex h-5 shrink-0 items-center rounded-full px-2 text-[11px] leading-none font-medium whitespace-nowrap',
        props.tag && 'tracking-wide font-semibold',
        className,
        props.className,
      )}
    >
      {label}
    </span>
  )
}

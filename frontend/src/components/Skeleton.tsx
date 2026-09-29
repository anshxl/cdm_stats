import { cn } from '@/lib/utils'

/** Grey pulsing block. Size it with className, e.g. "h-24 w-full". */
export function Skeleton({ className }: { className?: string }) {
  return <div aria-hidden className={cn('animate-pulse rounded-lg bg-white/[0.05]', className)} />
}

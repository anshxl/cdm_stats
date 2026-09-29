import type { TeamInfo } from '@/api/models'
import { cn } from '@/lib/utils'

export interface TeamBadgeProps {
  team: TeamInfo
  size?: 'sm' | 'md' | 'lg'
  /** Show the abbreviation (or full name with `showName="full"`) next to the logo. */
  showName?: boolean | 'full'
  className?: string
}

const SIZE = {
  sm: 'size-5 rounded-[5px] text-[8px]',
  md: 'size-7 rounded-md text-[10px]',
  lg: 'size-11 rounded-lg text-sm',
}

/** Team logo on a small tile edged in the team's primary color. */
export function TeamBadge({ team, size = 'md', showName = false, className }: TeamBadgeProps) {
  const color = team.primary_color ?? '#8a8f98'
  return (
    <span className={cn('inline-flex min-w-0 items-center gap-2', className)}>
      <span
        className={cn('grid shrink-0 place-items-center overflow-hidden bg-raised font-bold', SIZE[size])}
        style={{ boxShadow: `inset 0 0 0 1.5px ${color}` }}
        aria-hidden={showName ? true : undefined}
      >
        {team.logo ? (
          <img src={team.logo} alt={showName ? '' : team.abbreviation} className="size-[78%] object-contain" />
        ) : (
          <span style={{ color }}>{team.abbreviation.slice(0, 3)}</span>
        )}
      </span>
      {showName && (
        <span className="truncate font-medium">
          {showName === 'full' ? team.team_name : team.abbreviation}
        </span>
      )}
    </span>
  )
}

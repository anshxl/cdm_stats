import { Crosshair, LineChart, Shield, Swords, type LucideIcon } from 'lucide-react'
import { NavLink, useSearchParams } from 'react-router'
import { SHARED_KEYS } from '@/hooks/useFilter'
import { cn } from '@/lib/utils'

const LINKS: { to: string; label: string; icon: LucideIcon }[] = [
  { to: '/team', label: 'Team', icon: Shield },
  { to: '/h2h', label: 'Head to Head', icon: Swords },
  { to: '/scrims', label: 'Scrims', icon: Crosshair },
  { to: '/elo', label: 'Elo', icon: LineChart },
]

/** Page links. Icons only below md; the filter query is kept when switching pages. */
export function Sidebar() {
  const [searchParams] = useSearchParams()
  const carried = new URLSearchParams()
  for (const k of SHARED_KEYS) {
    const v = searchParams.get(k)
    if (v) carried.set(k, v)
  }
  const search = carried.toString() ? `?${carried}` : ''

  return (
    <nav
      aria-label="Pages"
      className="sticky top-0 flex h-dvh w-14 shrink-0 flex-col border-r border-line bg-page md:w-56"
    >
      <div className="flex h-14 items-center justify-center border-b border-line md:justify-start md:px-5">
        <span className="text-[15px] font-bold tracking-tight text-gold">
          <span className="md:hidden">CDM</span>
          <span className="hidden md:inline">CDM Stats</span>
        </span>
      </div>
      <ul className="flex flex-col gap-0.5 p-2">
        {LINKS.map(({ to, label, icon: Icon }) => (
          <li key={to}>
            <NavLink
              to={{ pathname: to, search }}
              title={label}
              className={({ isActive }) =>
                cn(
                  'relative flex h-9 items-center justify-center gap-3 rounded-md text-[13px] font-medium transition-colors md:justify-start md:px-3',
                  isActive
                    ? 'bg-gold/10 text-gold'
                    : 'text-muted-foreground hover:bg-white/[0.04] hover:text-foreground',
                )
              }
            >
              <Icon className="size-4 shrink-0" aria-hidden />
              <span className="sr-only md:not-sr-only">{label}</span>
            </NavLink>
          </li>
        ))}
      </ul>
    </nav>
  )
}

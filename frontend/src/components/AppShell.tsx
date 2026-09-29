import { Outlet } from 'react-router'
import { Sidebar } from './Sidebar'

export function AppShell() {
  return (
    <div className="flex min-h-dvh">
      <Sidebar />
      <main className="min-w-0 flex-1">
        <Outlet />
      </main>
    </div>
  )
}

/** Standard page body under the FilterBar. */
export function PageBody({ children }: { children: React.ReactNode }) {
  return <div className="mx-auto flex w-full max-w-[1280px] flex-col gap-4 px-4 py-5 md:px-6 md:py-6">{children}</div>
}

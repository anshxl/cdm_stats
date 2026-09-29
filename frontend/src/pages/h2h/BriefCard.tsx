import { Loader2 } from 'lucide-react'
import { useState } from 'react'
import type { Brief } from '@/api/models'
import { Card } from '@/components/Card'
import { ErrorCard } from '@/components/ErrorCard'
import { Skeleton } from '@/components/Skeleton'
import { type ApiError, readError, toQuery } from '@/hooks/useApi'

type State =
  | { kind: 'idle' }
  | { kind: 'loading' }
  | { kind: 'done'; text: string }
  | { kind: 'error'; error: ApiError }

/** Level 3: Claude narrates the tags on this page. On demand only; the server caches per view. */
export function BriefCard({ team, opp, event }: { team: string; opp: string; event: string }) {
  const [state, setState] = useState<State>({ kind: 'idle' })

  async function generate() {
    setState({ kind: 'loading' })
    try {
      const res = await fetch(`/api/h2h/brief${toQuery({ team, opp, event })}`, {
        method: 'POST',
        headers: { Accept: 'application/json' },
      })
      if (!res.ok) throw await readError(res)
      setState({ kind: 'done', text: ((await res.json()) as Brief).text })
    } catch (err: unknown) {
      const error: ApiError =
        err && typeof err === 'object' && 'message' in err && 'status' in err
          ? (err as ApiError)
          : { status: null, message: 'Could not reach the API. Check that the server is running.' }
      setState({ kind: 'error', error })
    }
  }

  const idle = state.kind === 'idle'
  // The brief is cached, so there is no button once it is written.
  const action = state.kind !== 'done' && (
    <button
      type="button"
      onClick={generate}
      disabled={state.kind === 'loading'}
      className="inline-flex h-8 items-center gap-1.5 rounded-md bg-gold px-3 text-[13px] font-medium text-page transition-opacity hover:opacity-90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gold/40 disabled:opacity-60"
    >
      {state.kind === 'loading' && <Loader2 className="size-3.5 animate-spin" aria-hidden />}
      {state.kind === 'error' ? 'Try again' : 'Generate brief'}
    </button>
  )

  return (
    // Idle: header only (title + button), so no empty body and no double bottom border.
    <Card
      title="Pre-match brief"
      action={action}
      className={idle ? '[&>header]:border-b-0' : undefined}
      bodyClassName={idle ? 'hidden' : undefined}
    >
      <div aria-live="polite">
        {state.kind === 'loading' && (
          <div className="flex flex-col gap-2">
            <Skeleton className="h-3.5 w-full" />
            <Skeleton className="h-3.5 w-11/12" />
            <Skeleton className="h-3.5 w-2/3" />
          </div>
        )}
        {state.kind === 'done' && <p className="text-sm leading-6 text-foreground">{state.text}</p>}
        {state.kind === 'error' && <ErrorCard error={state.error} title="Brief" />}
      </div>
    </Card>
  )
}

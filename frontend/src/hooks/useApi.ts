import { useEffect, useState } from 'react'

export type ParamValue = string | number | boolean | null | undefined
export type ApiParams = { [key: string]: ParamValue }

export interface ApiError {
  /** HTTP status, or null for a network failure. */
  status: number | null
  message: string
}

export interface ApiState<T> {
  /** Latest data. Kept while a new request loads, so the view does not jump. */
  data: T | null
  error: ApiError | null
  /** True while a request for the current inputs is in flight. */
  loading: boolean
}

export interface UseApiOptions {
  /** Param names that must be set (non-empty) before the request runs. */
  required?: string[]
}

export function toQuery(params: ApiParams): string {
  const q = new URLSearchParams()
  for (const k of Object.keys(params).sort()) {
    const v = params[k]
    if (v !== null && v !== undefined && v !== '') q.set(k, String(v))
  }
  const s = q.toString()
  return s ? `?${s}` : ''
}

export async function readError(res: Response): Promise<ApiError> {
  let message = `${res.status} ${res.statusText}`.trim()
  try {
    const body: unknown = await res.json()
    if (body && typeof body === 'object' && 'detail' in body) {
      const detail = (body as { detail: unknown }).detail
      if (typeof detail === 'string') message = detail
      else if (Array.isArray(detail)) {
        message = detail
          .map((d) => (d && typeof d === 'object' && 'msg' in d ? String(d.msg) : String(d)))
          .join('; ')
      }
    }
  } catch {
    // Body is not JSON: keep the status text.
  }
  return { status: res.status, message }
}

/**
 * GET `path` with `params` as the query string.
 * Skips the request (data null, loading false) when `path` is null or a
 * `required` param is empty. Aborts the old request when inputs change.
 */
export function useApi<T>(path: string | null, params: ApiParams = {}, options: UseApiOptions = {}): ApiState<T> {
  const ready = path !== null && (options.required ?? []).every((k) => {
    const v = params[k]
    return v !== null && v !== undefined && v !== ''
  })
  const url = ready ? `${path}${toQuery(params)}` : null

  const [state, setState] = useState<{ url: string | null; data: T | null; error: ApiError | null }>({
    url: null, data: null, error: null,
  })

  useEffect(() => {
    if (url === null) return
    const controller = new AbortController()
    fetch(url, { signal: controller.signal, headers: { Accept: 'application/json' } })
      .then(async (res) => {
        if (!res.ok) throw await readError(res)
        return (await res.json()) as T
      })
      .then((data) => setState({ url, data, error: null }))
      .catch((err: unknown) => {
        if (controller.signal.aborted) return
        const error: ApiError =
          err && typeof err === 'object' && 'message' in err && 'status' in err
            ? (err as ApiError)
            : { status: null, message: 'Could not reach the API. Check that the server is running.' }
        setState((s) => ({ url, data: s.data, error }))
      })
    return () => controller.abort()
  }, [url])

  if (url === null) return { data: null, error: null, loading: false }
  const current = state.url === url
  return {
    data: state.data,
    error: current ? state.error : null,
    loading: !current,
  }
}

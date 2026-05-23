/**
 * useSSE — connects to /api/events (Server-Sent Events) and invalidates
 * React Query caches whenever the backend pushes a resource change.
 *
 * Mount once at the App level; all pages benefit automatically.
 */
import { useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'

interface SSEEvent {
  resource: string   // "odags" | "odagtemplates"
  eventType: string  // "ADDED" | "MODIFIED" | "DELETED"
  name: string
  namespace: string
}

export function useSSE() {
  const qc = useQueryClient()

  useEffect(() => {
    const es = new EventSource('/api/events')

    es.onmessage = (e) => {
      try {
        const ev: SSEEvent = JSON.parse(e.data)
        if (ev.resource === 'odags') {
          qc.invalidateQueries({ queryKey: ['odags'] })
          qc.invalidateQueries({ queryKey: ['odag', ev.namespace, ev.name] })
          qc.invalidateQueries({ queryKey: ['odag-history', ev.namespace, ev.name] })
        } else if (ev.resource === 'odagtemplates') {
          qc.invalidateQueries({ queryKey: ['templates'] })
          qc.invalidateQueries({ queryKey: ['template', ev.namespace, ev.name] })
          qc.invalidateQueries({ queryKey: ['template-runs', ev.namespace, ev.name] })
        }
      } catch {
        // ignore malformed events
      }
    }

    es.onerror = () => {
      // EventSource auto-reconnects; no action needed
    }

    return () => es.close()
  }, [qc])
}

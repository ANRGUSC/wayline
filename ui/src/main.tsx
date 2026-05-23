import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import App from './App'
import './index.css'

console.log('[Wayline UI] build v2')
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // SSE pushes invalidations instantly; fall back to polling every 30s
      // in case the SSE connection drops.
      refetchInterval: 30_000,
      staleTime: 10_000,
    },
  },
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </React.StrictMode>,
)


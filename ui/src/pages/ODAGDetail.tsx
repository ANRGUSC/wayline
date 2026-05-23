import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import { api } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'
import DAGGraph from '@/components/DAGGraph'
import UnifiedGantt from '@/components/UnifiedGantt'
import UtilizationView from '@/components/UtilizationView'

type Tab = 'graph' | 'tasks' | 'history' | 'schedule' | 'utilization' | 'spec'

function fmtTime(s?: string): string {
  if (!s) return '—'
  const d = new Date(s)
  return isNaN(d.getTime()) ? '—' : d.toLocaleString()
}

export default function ODAGDetail() {
  const { namespace, name } = useParams<{ namespace: string; name: string }>()
  const [tab, setTab] = useState<Tab>('graph')
  const { data: dag, isLoading, error } = useQuery({
    queryKey: ['odag', namespace, name],
    queryFn: () => api.getODAG(namespace!, name!),
    staleTime: 0,
    refetchInterval: (query) => {
      const phase = (query.state.data as { phase?: string } | undefined)?.phase
      return phase === 'Running' || phase === 'Scheduling' || phase === 'Pending' ? 500 : 30000
    },
  })

  const { data: history } = useQuery({
    queryKey: ['odag-history', namespace, name],
    queryFn: () => api.getODAGHistory(namespace!, name!),
    enabled: tab === 'history',
  })

  if (isLoading) return <p className="text-on-muted">Loading...</p>
  if (error || !dag) return <p className="text-red-500 dark:text-red-400">Error: {String(error)}</p>

  return (
    <div>
      {/* Breadcrumb */}
      <div className="text-sm text-on-faint mb-4">
        <Link to="/" className="hover:text-on-secondary">ODAGs</Link>
        <span className="mx-2">/</span>
        <span className="text-on-secondary">{dag.namespace}/{dag.name}</span>
      </div>

      {/* Header */}
      <div className="flex items-center gap-4 mb-6">
        <h1 className="text-lg font-semibold">{dag.name}</h1>
        <StatusBadge phase={dag.phase} />
{(() => {
          const predMs = dag.predictedTasks && dag.predictedTasks.length > 0
            ? Math.max(...dag.predictedTasks.map(p => p.estEnd))
            : null
          const actMs = dag.makespan ?? null
          const diff = predMs != null && actMs != null ? actMs - predMs : null
          const diffPct = diff != null && predMs! > 0 ? (diff / predMs!) * 100 : null
          return (
            <span className="text-on-faint text-sm flex items-center gap-3">
              <span>predicted: <span className="text-on-secondary">{predMs != null ? `${predMs.toFixed(1)}s` : '—'}</span></span>
              <span>actual: <span className="text-on-secondary">{actMs != null ? `${actMs.toFixed(1)}s` : '—'}</span></span>
              {diff != null && (
                <span className={diff > 0 ? 'text-red-500 dark:text-red-400' : diff < 0 ? 'text-green-600 dark:text-green-400' : ''}>
                  Δ {diff > 0 ? '+' : ''}{diff.toFixed(1)}s{diffPct != null ? ` (${diff > 0 ? '+' : ''}${diffPct.toFixed(0)}%)` : ''}
                </span>
              )}
            </span>
          )
        })()}
      </div>

      {/* Tabs */}
      <div className="flex gap-4 border-b border-line mb-6 text-sm">
        {(['graph', 'tasks', 'schedule', 'utilization', 'spec', 'history'] as Tab[]).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`pb-2 capitalize ${tab === t ? 'text-on border-b-2 border-on' : 'text-on-faint hover:text-on-secondary'}`}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Graph tab */}
      {tab === 'graph' && <DAGGraph dag={dag} />}

      {/* Schedule tab */}
      {tab === 'schedule' && <UnifiedGantt dag={dag} />}

      {/* Tasks tab */}
      {tab === 'tasks' && (() => {
        const constraintMap = new Map(
          dag.spec.tasks.map(t => [t.name, t.constraints?.nodeNames ?? []])
        )
        const specMap = new Map(
          dag.spec.tasks.map(t => [t.name, t])
        )
        function fmtDuration(start?: string, end?: string): string {
          if (!start) return '—'
          const s = new Date(start).getTime()
          const e = end ? new Date(end).getTime() : Date.now()
          const sec = Math.round((e - s) / 1000)
          return `${sec}s`
        }
        function fmtBytes(b?: string): string {
          if (!b) return '—'
          const n = parseInt(b, 10)
          if (isNaN(n) || n === 0) return '0'
          if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)} GB`
          if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(0)} MB`
          if (n >= 1_000) return `${(n / 1_000).toFixed(0)} KB`
          return `${n} B`
        }
        return (
          <table className="w-full text-sm border-collapse">
            <thead>
              <tr className="text-left text-on-muted border-b border-line">
                <th className="pb-2 pr-4">Task</th>
                <th className="pb-2 pr-4">Phase</th>
                <th className="pb-2 pr-4">State</th>
                <th className="pb-2 pr-4">Node</th>
                <th className="pb-2 pr-4">Allowed Nodes</th>
                <th className="pb-2 pr-4">Duration</th>
                <th className="pb-2 pr-4">Data Size</th>
                <th className="pb-2 pr-4">Spec Runtime</th>
                <th className="pb-2 pr-4">Pod</th>
                <th className="pb-2 pr-4">Retries</th>
                <th className="pb-2">Message</th>
              </tr>
            </thead>
            <tbody>
              {dag.tasks.map(task => {
                const allowed = constraintMap.get(task.name) ?? []
                const spec = specMap.get(task.name)
                return (
                  <tr key={task.name} className="border-b border-line-soft">
                    <td className="py-2 pr-4 font-medium">{task.name}</td>
                    <td className="py-2 pr-4"><StatusBadge phase={task.phase} /></td>
                    <td className="py-2 pr-4 text-on-muted text-xs">{task.state ?? '—'}</td>
                    <td className="py-2 pr-4 text-on-muted">{task.node ?? '—'}</td>
                    <td className="py-2 pr-4 text-on-faint text-xs">
                      {allowed.length > 0 ? allowed.join(', ') : <span className="text-on-faint">any</span>}
                    </td>
                    <td className="py-2 pr-4 text-on-muted text-xs">{fmtDuration(task.startTime, task.completionTime)}</td>
                    <td className="py-2 pr-4 text-on-muted text-xs">{fmtBytes(task.dataSize)}</td>
                    <td className="py-2 pr-4 text-on-faint text-xs">{spec?.runtime != null ? `${spec.runtime}s` : '—'}</td>
                    <td className="py-2 pr-4 text-on-faint text-xs">{task.podName ?? '—'}</td>
                    <td className="py-2 pr-4 text-on-muted">{task.retries ?? 0}</td>
                    <td className="py-2 text-on-faint text-xs">{task.message ?? ''}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )
      })()}

      {/* Utilization tab */}
      {tab === 'utilization' && <UtilizationView dag={dag} />}

      {/* Spec tab */}
      {tab === 'spec' && (
        <table className="w-full text-sm border-collapse">
          <thead>
            <tr className="text-left text-on-muted border-b border-line">
              <th className="pb-2 pr-4">Task</th>
              <th className="pb-2 pr-4">Image</th>
              <th className="pb-2 pr-4">CPU</th>
              <th className="pb-2 pr-4">Memory</th>
              <th className="pb-2 pr-4">Runtime</th>
              <th className="pb-2 pr-4">Data Size</th>
              <th className="pb-2 pr-4">Dependencies</th>
              <th className="pb-2">Allowed Nodes</th>
            </tr>
          </thead>
          <tbody>
            {dag.spec.tasks.map(t => {
              const allowed = t.constraints?.nodeNames ?? []
              return (
                <tr key={t.name} className="border-b border-line-soft">
                  <td className="py-2 pr-4 font-medium">{t.name}</td>
                  <td className="py-2 pr-4 text-on-muted text-xs font-mono">{t.image}</td>
                  <td className="py-2 pr-4 text-on-muted">{t.resources?.cpu ?? '—'}</td>
                  <td className="py-2 pr-4 text-on-muted">{t.resources?.memory ?? '—'}</td>
                  <td className="py-2 pr-4 text-on-muted">{t.runtime != null ? `${t.runtime}s` : '—'}</td>
                  <td className="py-2 pr-4 text-on-muted">{t.dataSize ?? '—'}</td>
                  <td className="py-2 pr-4 text-on-faint text-xs">
                    {t.dependencies.length ? t.dependencies.join(', ') : '—'}
                  </td>
                  <td className="py-2 text-on-faint text-xs">
                    {allowed.length > 0 ? allowed.join(', ') : <span className="text-on-faint">any</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}

      {/* History tab */}
      {tab === 'history' && (
        <div>
          {history && history.length > 0 ? (
            <>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={history.map((h, i) => ({ run: i + 1, makespan: h.makespan }))}>
                  <XAxis dataKey="run" stroke="#6b7280" tick={{ fill: '#9ca3af' }} />
                  <YAxis stroke="#6b7280" tick={{ fill: '#9ca3af' }} unit="s" />
                  <Tooltip
                    contentStyle={{ background: 'var(--surface)', border: '1px solid var(--line)', color: 'var(--on-surface)' }}
                    formatter={(v: number) => [`${v.toFixed(1)}s`, 'Makespan']}
                  />
                  <Line type="monotone" dataKey="makespan" stroke="#60a5fa" dot={false} />
                </LineChart>
              </ResponsiveContainer>
              <table className="w-full text-sm border-collapse mt-6">
                <thead>
                  <tr className="text-left text-on-muted border-b border-line">
                    <th className="pb-2 pr-4">Run</th>
                    <th className="pb-2 pr-4">Phase</th>
                    <th className="pb-2 pr-4">Makespan</th>
                    <th className="pb-2">Started</th>
                  </tr>
                </thead>
                <tbody>
                  {history.map((h, i) => (
                    <tr key={h.runId} className="border-b border-line-soft">
                      <td className="py-2 pr-4 text-on-muted">#{i + 1}</td>
                      <td className="py-2 pr-4"><StatusBadge phase={h.phase} /></td>
                      <td className="py-2 pr-4 text-on-muted">
                        {h.makespan != null ? `${h.makespan.toFixed(1)}s` : '—'}
                      </td>
                      <td className="py-2 text-on-faint text-xs">{fmtTime(h.startTime)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          ) : (
            <p className="text-on-faint">No historical runs recorded yet.</p>
          )}
        </div>
      )}
    </div>
  )
}

import { useState } from 'react'
import { useQuery, useQueries } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '@/api/client'
import type { ODAGDetail } from '@/api/client'
import { BATCH_PRESETS, toBatchEntries, ODAG_COLORS } from '@/data/batchPresets'
import StatusBadge from '@/components/StatusBadge'
import BatchGanttChart from '@/components/BatchGanttChart'

const BATCH_NAMES = BATCH_PRESETS.map(p => p.name)
const NS = 'wl-system'

function formatDuration(sec: number | undefined): string {
  if (sec == null || sec === 0) return '-'
  return `${sec.toFixed(1)}s`
}

export default function BatchExecution() {
  const [submitting, setSubmitting] = useState(false)

  // Fetch the ODAG list to know which batch ODAGs exist.
  // SSE invalidates ['odags'] on every change, so this stays fresh.
  const { data: allOdags } = useQuery({
    queryKey: ['odags'],
    queryFn: api.listODAGs,
    refetchInterval: 5000,
  })

  const batchSummaries = (allOdags ?? []).filter(o => BATCH_NAMES.includes(o.name))
  const existing = new Set(batchSummaries.map(o => o.name))
  const summaryMap = new Map(batchSummaries.map(o => [o.name, o]))

  const isAnyActive = batchSummaries.some(
    s => !['Succeeded', 'Failed'].includes(s.phase),
  )

  // Fetch detail for each existing batch ODAG (for Gantt chart).
  // SSE handles live pushes; polling is just a fallback.
  const detailQueries = useQueries({
    queries: BATCH_NAMES.map(name => ({
      queryKey: ['odag', NS, name],
      queryFn: () => api.getODAG(NS, name),
      enabled: existing.has(name),
      retry: false,
      refetchInterval: isAnyActive ? 3000 : 30000,
    })),
  })

  const details: ODAGDetail[] = detailQueries
    .filter(q => q.isSuccess && q.data != null)
    .map(q => q.data as ODAGDetail)

  const succeededCount = batchSummaries.filter(s => s.phase === 'Succeeded').length
  const failedCount = batchSummaries.filter(s => s.phase === 'Failed').length

  async function handleRun() {
    setSubmitting(true)
    try {
      await api.submitBatch(NS, toBatchEntries())
    } catch (e) {
      console.error('Batch submit failed:', e)
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-6">
        <h1 className="text-xl font-bold text-on">Batch Execution</h1>
        <button
          onClick={handleRun}
          disabled={submitting || isAnyActive}
          className={`px-4 py-1.5 rounded text-sm font-medium transition-colors ${
            submitting || isAnyActive
              ? 'bg-surface-card text-on-muted cursor-not-allowed'
              : 'bg-blue-600 text-white hover:bg-blue-500'
          }`}
        >
          {submitting ? 'Submitting...' : isAnyActive ? 'Running...' : 'Run Batch'}
        </button>
        {existing.size > 0 && (
          <span className="text-sm text-on-muted">
            {succeededCount}/{BATCH_NAMES.length} succeeded
            {failedCount > 0 && <span className="text-red-500 dark:text-red-400 ml-2">{failedCount} failed</span>}
          </span>
        )}
      </div>

      {/* Description */}
      <p className="text-sm text-on-faint">
        Submits 5 ODAGs with HEFT scheduling, staggered over ~60s,
        to test multi-DAG scheduling across the cluster.
      </p>

      {/* Overview table */}
      <div className="border border-line rounded overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-surface-alt text-on-muted text-left">
              <th className="px-4 py-2 w-5"></th>
              <th className="px-4 py-2">Name</th>
              <th className="px-4 py-2">Topology</th>
              <th className="px-4 py-2">Delay</th>
              <th className="px-4 py-2">Phase</th>
              <th className="px-4 py-2">Tasks</th>
              <th className="px-4 py-2">Makespan</th>
              <th className="px-4 py-2">Scheduler</th>
            </tr>
          </thead>
          <tbody>
            {BATCH_PRESETS.map((preset, i) => {
              const summary = summaryMap.get(preset.name)
              return (
                <tr
                  key={preset.name}
                  className="border-t border-line hover:bg-surface-alt/50"
                >
                  <td className="px-4 py-2">
                    <span
                      className="inline-block w-3 h-3 rounded-sm"
                      style={{ backgroundColor: ODAG_COLORS[i] }}
                    />
                  </td>
                  <td className="px-4 py-2 font-medium">
                    {summary ? (
                      <Link
                        to={`/odags/${NS}/${preset.name}`}
                        className="text-accent hover:text-accent-hover"
                      >
                        {preset.name}
                      </Link>
                    ) : (
                      <span className="text-on-secondary">{preset.name}</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-on-muted">{preset.topology}</td>
                  <td className="px-4 py-2 text-on-muted">+{preset.delay}s</td>
                  <td className="px-4 py-2">
                    {summary ? (
                      <StatusBadge phase={summary.phase} />
                    ) : (
                      <span className="text-xs text-on-faint">not submitted</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-on-muted">{preset.taskCount}</td>
                  <td className="px-4 py-2 text-on-secondary">
                    {formatDuration(summary?.makespan)}
                  </td>
                  <td className="px-4 py-2 text-on-muted">
                    {summary?.scheduler ?? '-'}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* Combined Gantt Chart */}
      <div>
        <h2 className="text-sm font-semibold text-on-muted mb-3 uppercase tracking-wider">
          Combined Schedule
        </h2>
        <div className="border border-line rounded p-4 bg-surface-alt/30">
          <BatchGanttChart odags={details} />
        </div>
      </div>
    </div>
  )
}

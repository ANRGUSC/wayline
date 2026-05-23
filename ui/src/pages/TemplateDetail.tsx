import { useMemo, useState } from 'react'
import { useParams, Link, useLocation } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api, TemplateRun, TemplateHistoryEntry } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'
import TemplateGraph from '@/components/TemplateGraph'
import MakespanHistogram from '@/components/MakespanHistogram'

type Tab = 'graph' | 'tasks' | 'runs' | 'stats' | 'profile' | 'spec'
const VALID_TABS: Tab[] = ['graph', 'tasks', 'runs', 'stats', 'profile', 'spec']

export default function TemplateDetail() {
  const { namespace, name } = useParams<{ namespace: string; name: string }>()
  const { hash } = useLocation()
  const hashTab = hash.replace('#', '') as Tab
  const initialTab: Tab = VALID_TABS.includes(hashTab) ? hashTab : 'graph'
  const [tab, setTab] = useState<Tab>(initialTab)
  const queryClient = useQueryClient()

  const { data: template, isLoading, error } = useQuery({
    queryKey: ['template', namespace, name],
    queryFn: () => api.getTemplate(namespace!, name!),
    enabled: !!namespace && !!name,
  })

  const { data: runs } = useQuery({
    queryKey: ['template-runs', namespace, name],
    queryFn: () => api.getTemplateRuns(namespace!, name!),
    enabled: !!namespace && !!name,
  })

  const { data: history } = useQuery({
    queryKey: ['template-history', namespace, name],
    queryFn: () => api.getTemplateHistory(namespace!, name!),
    enabled: !!namespace && !!name && tab === 'stats',
  })

  const runMutation = useMutation({
    mutationFn: () => api.runTemplate(namespace!, name!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['template', namespace, name] })
      queryClient.invalidateQueries({ queryKey: ['template-runs', namespace, name] })
      queryClient.invalidateQueries({ queryKey: ['odags'] })
    },
  })

  if (isLoading) return <p className="text-on-muted">Loading...</p>
  if (error) return <p className="text-red-500 dark:text-red-400">Error: {String(error)}</p>
  if (!template) return <p className="text-on-muted">Not found</p>

  const tabs: { key: Tab; label: string }[] = [
    { key: 'graph', label: 'Graph' },
    { key: 'tasks', label: 'Tasks' },
    { key: 'runs', label: `Runs (${template.runCount})` },
    { key: 'stats', label: 'Stats' },
    { key: 'profile', label: 'Profile' },
    { key: 'spec', label: 'Spec' },
  ]

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <Link to="/templates" className="text-on-muted text-sm hover:text-on-secondary">
            Templates
          </Link>
          <span className="text-on-faint mx-1">/</span>
          <span className="text-on-muted text-sm">ODAG</span>
          <span className="text-on-faint mx-2">/</span>
          <h1 className="text-lg font-semibold inline">{template.name}</h1>
          {template.description && (
            <p className="text-on-muted text-sm mt-1">{template.description}</p>
          )}
        </div>
        <button
          onClick={() => runMutation.mutate()}
          disabled={runMutation.isPending}
          className="px-4 py-2 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-400 dark:disabled:bg-gray-600 text-white text-sm rounded"
        >
          {runMutation.isPending ? 'Creating...' : 'New Run'}
        </button>
      </div>

      {/* Summary bar */}
      <div className="flex gap-6 text-sm mb-4 text-on-muted">
        <span>Scheduler: <span className="text-on">{template.scheduler}</span></span>
        <span>Tasks: <span className="text-on">{template.taskCount}</span></span>
        <span>Runs: <span className="text-on">{template.runCount}</span></span>
        {template.lastRunMakespan != null && template.lastRunMakespan > 0 && (
          <span>Last makespan: <span className="text-on">{template.lastRunMakespan.toFixed(1)}s</span></span>
        )}
        <span>Profiling: <span className={template.profilingEnabled ? 'text-green-600 dark:text-green-400' : 'text-on-faint'}>
          {template.profilingEnabled ? 'on' : 'off'}
        </span></span>
      </div>

      {/* Run result toast */}
      {runMutation.isSuccess && (
        <div className="mb-4 p-2 bg-green-100/30 dark:bg-green-900/30 border border-green-300 dark:border-green-800 rounded text-green-700 dark:text-green-300 text-sm">
          {runMutation.data.message}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 border-b border-line mb-4">
        {tabs.map(t => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={`px-4 py-2 text-sm border-b-2 -mb-px ${
              tab === t.key
                ? 'border-blue-500 text-on'
                : 'border-transparent text-on-muted hover:text-on-secondary'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === 'graph' && <TemplateGraph tasks={template.spec.tasks} />}
      {tab === 'tasks' && <TasksTab spec={template.spec} />}
      {tab === 'runs' && <RunsTab runs={runs ?? []} namespace={namespace!} />}
      {tab === 'stats' && <StatsTab history={history ?? []} />}
      {tab === 'profile' && <ProfileTab profileSummary={template.profileSummary} />}
      {tab === 'spec' && <SpecTab spec={template.spec} />}
    </div>
  )
}

/* ---------- Tasks Tab ---------- */

function TasksTab({ spec }: { spec: { tasks: Array<{ name: string; image: string; dependencies: string[]; dataSize?: string; runtime?: number; constraints?: { nodeNames?: string[] } }> } }) {
  return (
    <table className="w-full text-sm border-collapse">
      <thead>
        <tr className="text-left text-on-muted border-b border-line">
          <th className="pb-2 pr-4">Name</th>
          <th className="pb-2 pr-4">Image</th>
          <th className="pb-2 pr-4">Runtime</th>
          <th className="pb-2 pr-4">Data Size</th>
          <th className="pb-2 pr-4">Dependencies</th>
          <th className="pb-2">Constraints</th>
        </tr>
      </thead>
      <tbody>
        {spec.tasks.map(t => (
          <tr key={t.name} className="border-b border-line-soft">
            <td className="py-2 pr-4 text-on">{t.name}</td>
            <td className="py-2 pr-4 text-on-muted text-xs">{t.image.split('/').pop()}</td>
            <td className="py-2 pr-4 text-on-muted">{t.runtime != null ? `${t.runtime}s` : '—'}</td>
            <td className="py-2 pr-4 text-on-muted">{t.dataSize || '—'}</td>
            <td className="py-2 pr-4 text-on-muted">{t.dependencies?.length ? t.dependencies.join(', ') : '—'}</td>
            <td className="py-2 text-on-muted">{t.constraints?.nodeNames?.join(', ') || '—'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

/* ---------- Stats Tab ---------- */

function StatsTab({ history }: { history: TemplateHistoryEntry[] }) {
  if (history.length === 0) {
    return <p className="text-on-faint">No completed runs yet.</p>
  }
  const ms = history.map(h => h.makespan).filter(x => x > 0).sort((a, b) => a - b)
  const n = ms.length
  const mean = ms.reduce((a, b) => a + b, 0) / n
  const median = n % 2 ? ms[(n - 1) / 2] : (ms[n / 2 - 1] + ms[n / 2]) / 2
  const min = ms[0]
  const max = ms[n - 1]
  const variance = ms.reduce((s, x) => s + (x - mean) ** 2, 0) / n
  const stdev = Math.sqrt(variance)
  const p95 = ms[Math.min(n - 1, Math.floor(0.95 * n))]
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 md:grid-cols-6 gap-4 text-sm">
        <Stat label="Runs" value={String(n)} />
        <Stat label="Mean" value={`${mean.toFixed(1)}s`} />
        <Stat label="Median" value={`${median.toFixed(1)}s`} />
        <Stat label="Min" value={`${min.toFixed(1)}s`} />
        <Stat label="Max" value={`${max.toFixed(1)}s`} />
        <Stat label="P95" value={`${p95.toFixed(1)}s`} />
      </div>
      <div>
        <div className="text-xs text-on-faint mb-2">Makespan distribution (stdev {stdev.toFixed(2)}s)</div>
        <MakespanHistogram values={ms} />
      </div>
      <div>
        <div className="text-xs text-on-faint mb-2">Makespan trend (over run order)</div>
        <MakespanTrend history={history} />
      </div>
    </div>
  )
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-surface-alt border border-line rounded p-2">
      <div className="text-xs text-on-faint">{label}</div>
      <div className="text-on font-mono">{value}</div>
    </div>
  )
}

function MakespanTrend({ history }: { history: TemplateHistoryEntry[] }) {
  if (history.length === 0) return null
  const values = history.map(h => h.makespan)
  const maxV = Math.max(...values, 1)
  const W = 720, H = 140, ML = 40, MR = 10, MT = 10, MB = 24
  const innerW = W - ML - MR
  const innerH = H - MT - MB
  const xs = (i: number) => ML + (history.length <= 1 ? innerW / 2 : (i / (history.length - 1)) * innerW)
  const ys = (v: number) => MT + innerH - (v / maxV) * innerH
  const points = history.map((h, i) => `${xs(i)},${ys(h.makespan)}`).join(' ')
  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block' }}>
      <line x1={ML} y1={MT + innerH} x2={ML + innerW} y2={MT + innerH} stroke="#9ca3af" strokeOpacity={0.4} />
      <line x1={ML} y1={MT} x2={ML} y2={MT + innerH} stroke="#9ca3af" strokeOpacity={0.4} />
      <polyline points={points} fill="none" stroke="#60a5fa" strokeWidth={1.5} />
      {history.map((h, i) => (
        <circle key={i} cx={xs(i)} cy={ys(h.makespan)} r={3} fill="#60a5fa">
          <title>{`${h.name}: ${h.makespan.toFixed(1)}s`}</title>
        </circle>
      ))}
      <text x={ML - 4} y={ys(maxV) + 4} textAnchor="end" fontSize={10} fill="#9ca3af">{maxV.toFixed(0)}s</text>
      <text x={ML - 4} y={ys(0) + 4} textAnchor="end" fontSize={10} fill="#9ca3af">0</text>
      <text x={ML} y={H - 6} fontSize={10} fill="#9ca3af">run 1</text>
      <text x={ML + innerW} y={H - 6} textAnchor="end" fontSize={10} fill="#9ca3af">run {history.length}</text>
    </svg>
  )
}

/* ---------- Runs Tab ---------- */

type RunSortKey = 'name' | 'run' | 'phase' | 'makespan' | 'age'
type SortDir = 'asc' | 'desc'

function RunsTab({ runs, namespace }: { runs: TemplateRun[]; namespace: string }) {
  const [query, setQuery] = useState('')
  const [sortKey, setSortKey] = useState<RunSortKey>('age')
  const [sortDir, setSortDir] = useState<SortDir>('asc') // asc on age = newest first

  const visible = useMemo(() => {
    const q = query.trim().toLowerCase()
    const filtered = q
      ? runs.filter(r =>
          r.name.toLowerCase().includes(q) ||
          String(r.run).includes(q) ||
          r.phase.toLowerCase().includes(q))
      : [...runs]
    const cmp = (a: TemplateRun, b: TemplateRun): number => {
      switch (sortKey) {
        case 'name':     return a.name.localeCompare(b.name)
        case 'run':      return Number(a.run) - Number(b.run)
        case 'phase':    return a.phase.localeCompare(b.phase)
        case 'makespan': return (a.makespan ?? Infinity) - (b.makespan ?? Infinity)
        case 'age':      return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
      }
    }
    filtered.sort(cmp)
    if (sortDir === 'desc') filtered.reverse()
    return filtered
  }, [runs, query, sortKey, sortDir])

  function toggleSort(k: RunSortKey) {
    if (sortKey === k) setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    else { setSortKey(k); setSortDir('asc') }
  }

  if (runs.length === 0) {
    return <p className="text-on-faint">No runs yet. Click "New Run" to create one.</p>
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <div className="text-xs text-on-faint">{visible.length} of {runs.length}</div>
        <input
          type="text"
          value={query}
          onChange={e => setQuery(e.target.value)}
          placeholder="Filter by name, run #, phase…"
          className="px-3 py-1 text-sm bg-surface-alt border border-line rounded w-64 focus:outline-none focus:border-accent"
        />
      </div>
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="text-left text-on-muted border-b border-line">
            <RunSortHeader label="Name"     k="name"     sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <RunSortHeader label="Run"      k="run"      sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <RunSortHeader label="Phase"    k="phase"    sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <RunSortHeader label="Makespan" k="makespan" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <RunSortHeader label="Age"      k="age"      sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
          </tr>
        </thead>
        <tbody>
          {visible.map(r => (
            <tr key={r.name} className="border-b border-line-soft hover:bg-surface-alt">
              <td className="py-2 pr-4">
                <Link to={`/odags/${namespace}/${r.name}`} className="text-accent hover:text-accent-hover">{r.name}</Link>
              </td>
              <td className="py-2 pr-4 text-on-muted">#{r.run}</td>
              <td className="py-2 pr-4"><StatusBadge phase={r.phase} /></td>
              <td className="py-2 pr-4 text-on-muted">
                {r.makespan != null && r.makespan > 0 ? `${r.makespan.toFixed(1)}s` : '—'}
              </td>
              <td className="py-2 text-on-faint">{formatAge(r.createdAt)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function RunSortHeader({ label, k, sortKey, sortDir, onClick }: {
  label: string; k: RunSortKey; sortKey: RunSortKey; sortDir: SortDir; onClick: (k: RunSortKey) => void
}) {
  const active = k === sortKey
  const arrow = !active ? '' : sortDir === 'asc' ? ' ▲' : ' ▼'
  return (
    <th className={`pb-2 pr-4 cursor-pointer select-none ${active ? 'text-on-secondary' : 'hover:text-on-secondary'}`}
      onClick={() => onClick(k)}>
      {label}{arrow}
    </th>
  )
}

/* ---------- Profile Tab ---------- */

function ProfileTab({ profileSummary }: { profileSummary?: Record<string, Record<string, number>> }) {
  if (!profileSummary || Object.keys(profileSummary).length === 0) {
    return <p className="text-on-faint">No profiler data yet. Run the template a few times to build up profiles.</p>
  }

  // Collect all unique nodes across all tasks.
  const allNodes = new Set<string>()
  for (const nodeMap of Object.values(profileSummary)) {
    for (const node of Object.keys(nodeMap)) {
      allNodes.add(node)
    }
  }
  const nodes = [...allNodes].sort()
  const tasks = Object.keys(profileSummary).sort()

  // Find min/max for heatmap coloring.
  let min = Infinity, max = 0
  for (const nodeMap of Object.values(profileSummary)) {
    for (const val of Object.values(nodeMap)) {
      if (val < min) min = val
      if (val > max) max = val
    }
  }

  const heatColor = (val: number) => {
    if (max === min) return 'bg-blue-100/50 dark:bg-blue-900/50'
    const ratio = (val - min) / (max - min)
    if (ratio < 0.33) return 'bg-green-100/60 dark:bg-green-900/60 text-green-700 dark:text-green-300'
    if (ratio < 0.66) return 'bg-yellow-100/60 dark:bg-yellow-900/60 text-yellow-700 dark:text-yellow-300'
    return 'bg-red-100/60 dark:bg-red-900/60 text-red-700 dark:text-red-300'
  }

  return (
    <div>
      <h3 className="text-sm text-on-muted mb-3">Task x Node Runtime Matrix (EMA seconds)</h3>
      <div className="overflow-x-auto">
        <table className="text-sm border-collapse">
          <thead>
            <tr className="text-on-muted">
              <th className="pb-2 pr-4 text-left">Task</th>
              {nodes.map(n => (
                <th key={n} className="pb-2 px-3 text-center">{n}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {tasks.map(task => (
              <tr key={task} className="border-t border-line">
                <td className="py-2 pr-4 text-on">{task}</td>
                {nodes.map(node => {
                  const val = profileSummary[task]?.[node]
                  return (
                    <td key={node} className={`py-2 px-3 text-center ${val != null ? heatColor(val) : ''}`}>
                      {val != null ? val.toFixed(1) : <span className="text-on-faint">—</span>}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

/* ---------- Spec Tab ---------- */

function SpecTab({ spec }: { spec: Record<string, unknown> }) {
  return (
    <pre className="text-xs text-on-secondary bg-surface-alt p-4 rounded overflow-auto max-h-[600px]">
      {JSON.stringify(spec, null, 2)}
    </pre>
  )
}

/* ---------- Helpers ---------- */

function formatAge(iso: string): string {
  const seconds = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`
  return `${Math.floor(seconds / 86400)}d`
}

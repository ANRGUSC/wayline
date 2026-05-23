import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'

type SortKey = 'name' | 'namespace' | 'scheduler' | 'taskCount' | 'count' | 'lastPhase' | 'age'
type SortDir = 'asc' | 'desc'

interface UnifiedTemplate {
  name: string
  namespace: string
  description: string
  scheduler: string
  taskCount: number
  count: number
  lastPhase?: string
  createdAt: string
  detailPath: string
}

export default function TemplateList() {
  const [query, setQuery] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('name')
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  const { data: odagTemplates, isLoading, error } = useQuery({
    queryKey: ['templates'],
    queryFn: api.listTemplates,
  })

  const allTemplates = useMemo<UnifiedTemplate[]>(() => {
    const out: UnifiedTemplate[] = []
    for (const t of odagTemplates ?? []) {
      out.push({
        name: t.name, namespace: t.namespace, description: t.description,
        scheduler: t.scheduler, taskCount: t.taskCount, count: t.runCount,
        lastPhase: t.lastRunPhase, createdAt: t.createdAt,
        detailPath: `/templates/odag/${t.namespace}/${t.name}`,
      })
    }
    return out
  }, [odagTemplates])

  const visibleTemplates = useMemo(() => {
    const q = query.trim().toLowerCase()
    const searched = q
      ? allTemplates.filter(t =>
          t.name.toLowerCase().includes(q) ||
          t.namespace.toLowerCase().includes(q) ||
          t.description.toLowerCase().includes(q) ||
          t.scheduler.toLowerCase().includes(q))
      : [...allTemplates]

    const cmp = (a: UnifiedTemplate, b: UnifiedTemplate): number => {
      switch (sortKey) {
        case 'name':      return a.name.localeCompare(b.name)
        case 'namespace': return a.namespace.localeCompare(b.namespace)
        case 'scheduler': return a.scheduler.localeCompare(b.scheduler)
        case 'taskCount': return a.taskCount - b.taskCount
        case 'count':     return a.count - b.count
        case 'lastPhase': return (a.lastPhase ?? '').localeCompare(b.lastPhase ?? '')
        case 'age':       return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
      }
    }
    searched.sort(cmp)
    if (sortDir === 'desc') searched.reverse()
    return searched
  }, [allTemplates, query, sortKey, sortDir])

  function toggleSort(k: SortKey) {
    if (sortKey === k) setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    else { setSortKey(k); setSortDir('asc') }
  }

  if (isLoading) return <p className="text-on-muted">Loading...</p>
  if (error) return <p className="text-red-500 dark:text-red-400">Error: {String(error)}</p>

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-lg font-semibold">ODAG Templates</h1>
        <input
          type="text"
          value={query}
          onChange={e => setQuery(e.target.value)}
          placeholder="Filter by name, namespace, scheduler…"
          className="px-3 py-1 text-sm bg-surface-alt border border-line rounded w-72 focus:outline-none focus:border-accent"
        />
      </div>

      <div className="text-xs text-on-faint mb-2">{visibleTemplates.length} of {allTemplates.length}</div>
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="text-left text-on-muted border-b border-line">
            <SortHeader label="Name"        k="name"      sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Namespace"   k="namespace" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Scheduler"   k="scheduler" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Tasks"       k="taskCount" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Runs"        k="count"     sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Last Phase"  k="lastPhase" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Age"         k="age"       sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
          </tr>
        </thead>
        <tbody>
          {visibleTemplates.length === 0 && (
            <tr>
              <td colSpan={7} className="pt-4 text-on-faint text-center">
                {allTemplates.length ? 'No templates match the filter.' : (
                  <>No templates found. Create one with <code>wayline template apply -f template.yml</code></>
                )}
              </td>
            </tr>
          )}
          {visibleTemplates.map(t => (
            <tr key={`${t.namespace}/${t.name}`} className="border-b border-line-soft hover:bg-surface-alt">
              <td className="py-2 pr-4">
                <Link to={t.detailPath} className="text-accent hover:text-accent-hover">{t.name}</Link>
              </td>
              <td className="py-2 pr-4 text-on-muted">{t.namespace}</td>
              <td className="py-2 pr-4 text-on-muted">{t.scheduler}</td>
              <td className="py-2 pr-4 text-on-muted">{t.taskCount}</td>
              <td className="py-2 pr-4 text-on-muted">
                {t.count > 0 ? (
                  <Link to={`${t.detailPath}#runs`} className="text-accent hover:text-accent-hover">
                    {t.count}
                  </Link>
                ) : t.count}
              </td>
              <td className="py-2 pr-4">
                {t.lastPhase ? <StatusBadge phase={t.lastPhase} /> : <span className="text-on-faint">—</span>}
              </td>
              <td className="py-2 text-on-faint">{formatAge(t.createdAt)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function SortHeader({ label, k, sortKey, sortDir, onClick }: {
  label: string; k: SortKey; sortKey: SortKey; sortDir: SortDir; onClick: (k: SortKey) => void
}) {
  const active = k === sortKey
  const arrow = !active ? '' : sortDir === 'asc' ? ' ▲' : ' ▼'
  return (
    <th
      className={`pb-2 pr-4 cursor-pointer select-none ${active ? 'text-on-secondary' : 'hover:text-on-secondary'}`}
      onClick={() => onClick(k)}
    >
      {label}{arrow}
    </th>
  )
}

function formatAge(iso: string): string {
  const seconds = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`
  return `${Math.floor(seconds / 86400)}d`
}

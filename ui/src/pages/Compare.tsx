import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, ODAGSummary } from '@/api/client'
import UnifiedGantt from '@/components/UnifiedGantt'
import UtilizationView from '@/components/UtilizationView'

export default function Compare() {
  const { data: odags } = useQuery({
    queryKey: ['odags'],
    queryFn: api.listODAGs,
  })

  const [key1, setKey1] = useState<string>('')
  const [key2, setKey2] = useState<string>('')

  const options = useMemo(() => (odags ?? [])
    .filter(d => d.phase === 'Succeeded' || d.phase === 'Failed')
    .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()),
    [odags])

  const [ns1, name1] = key1 ? key1.split('/') : ['', '']
  const [ns2, name2] = key2 ? key2.split('/') : ['', '']

  const { data: dag1 } = useQuery({
    queryKey: ['odag', ns1, name1],
    queryFn: () => api.getODAG(ns1, name1),
    enabled: !!ns1 && !!name1,
  })
  const { data: dag2 } = useQuery({
    queryKey: ['odag', ns2, name2],
    queryFn: () => api.getODAG(ns2, name2),
    enabled: !!ns2 && !!name2,
  })

  return (
    <div>
      <h1 className="text-lg font-semibold mb-4">Compare ODAG runs</h1>

      <div className="grid grid-cols-2 gap-4 mb-6">
        <RunPicker label="Left" value={key1} onChange={setKey1} options={options} />
        <RunPicker label="Right" value={key2} onChange={setKey2} options={options} />
      </div>

      {dag1 && dag2 && (
        <>
          <SummaryDiff left={dag1} right={dag2} />

          <Section title="Schedule (execution + network)">
            <div className="grid grid-cols-2 gap-4">
              <Pane title={key1}><UnifiedGantt dag={dag1} /></Pane>
              <Pane title={key2}><UnifiedGantt dag={dag2} /></Pane>
            </div>
          </Section>

          <Section title="Utilization">
            <div className="grid grid-cols-2 gap-4">
              <Pane title={key1}><UtilizationView dag={dag1} /></Pane>
              <Pane title={key2}><UtilizationView dag={dag2} /></Pane>
            </div>
          </Section>
        </>
      )}

      {(!key1 || !key2) && (
        <p className="text-on-faint text-sm">Pick two completed runs above to compare.</p>
      )}
    </div>
  )
}

function RunPicker({ label, value, onChange, options }: {
  label: string
  value: string
  onChange: (v: string) => void
  options: ODAGSummary[]
}) {
  return (
    <div className="flex items-center gap-2">
      <label className="text-xs text-on-faint w-12">{label}</label>
      <select
        value={value}
        onChange={e => onChange(e.target.value)}
        className="flex-1 px-3 py-1 text-sm bg-surface-alt border border-line rounded focus:outline-none focus:border-accent"
      >
        <option value="">— pick a run —</option>
        {options.map(d => (
          <option key={`${d.namespace}/${d.name}`} value={`${d.namespace}/${d.name}`}>
            {d.name} ({d.phase}{d.makespan != null ? `, ${d.makespan.toFixed(1)}s` : ''})
          </option>
        ))}
      </select>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="mb-8">
      <h2 className="text-xs uppercase tracking-wide text-on-faint mb-2">{title}</h2>
      {children}
    </div>
  )
}

function Pane({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-xs text-on-faint mb-1 font-mono">{title}</div>
      <div className="border border-line rounded p-2">{children}</div>
    </div>
  )
}

function SummaryDiff({ left, right }: { left: any; right: any }) {
  const lms = left.makespan
  const rms = right.makespan
  const diff = lms != null && rms != null ? rms - lms : null
  const pct = diff != null && lms > 0 ? (diff / lms) * 100 : null
  return (
    <div className="grid grid-cols-3 gap-4 mb-6 text-sm">
      <Card label="Makespan (left)" value={lms != null ? `${lms.toFixed(1)}s` : '—'} />
      <Card label="Makespan (right)" value={rms != null ? `${rms.toFixed(1)}s` : '—'} />
      <Card
        label="Diff (right − left)"
        value={diff != null ? `${diff > 0 ? '+' : ''}${diff.toFixed(1)}s${pct != null ? ` (${pct > 0 ? '+' : ''}${pct.toFixed(1)}%)` : ''}` : '—'}
        highlight={diff != null ? (diff > 0 ? 'worse' : diff < 0 ? 'better' : 'same') : undefined}
      />
    </div>
  )
}

function Card({ label, value, highlight }: { label: string; value: string; highlight?: 'better' | 'worse' | 'same' }) {
  const cls =
    highlight === 'better' ? 'text-green-600 dark:text-green-400' :
    highlight === 'worse'  ? 'text-red-500 dark:text-red-400' :
    'text-on'
  return (
    <div className="bg-surface-alt border border-line rounded p-3">
      <div className="text-xs text-on-faint">{label}</div>
      <div className={`font-mono text-base ${cls}`}>{value}</div>
    </div>
  )
}

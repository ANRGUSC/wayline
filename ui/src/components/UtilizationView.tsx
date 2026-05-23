import type { ODAGDetail } from '@/api/client'

interface Props {
  dag: ODAGDetail
}

function fmtBytes(bytes: number): string {
  if (bytes >= 1_000_000_000) return `${(bytes / 1_000_000_000).toFixed(1)} GB`
  if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`
  if (bytes >= 1_000) return `${(bytes / 1_000).toFixed(1)} KB`
  return `${bytes} B`
}

// Derive per-node compute utilization (busy-time / makespan) from actual task
// start/completion timestamps. Overlapping tasks on the same node count as one
// contiguous busy window (via sweep-line union), so the ratio is wall-clock,
// not CPU-seconds.
export default function UtilizationView({ dag }: Props) {
  const tasks = dag.tasks.filter(t => t.startTime && t.completionTime && t.node)

  // ODAG span = min(startTime) → max(completionTime). If the ODAG has no makespan
  // (still running), fall back to max so bars still render.
  const starts = tasks.map(t => new Date(t.startTime!).getTime())
  const ends = tasks.map(t => new Date(t.completionTime!).getTime())
  const t0 = starts.length ? Math.min(...starts) : 0
  const tEnd = ends.length ? Math.max(...ends) : t0 + 1
  const span = Math.max((tEnd - t0) / 1000, 0.001) // seconds

  // Per-node busy time via interval union.
  const byNode: Record<string, Array<{ s: number; e: number }>> = {}
  for (const t of tasks) {
    const s = (new Date(t.startTime!).getTime() - t0) / 1000
    const e = (new Date(t.completionTime!).getTime() - t0) / 1000
    ;(byNode[t.node!] ??= []).push({ s, e })
  }
  const busyByNode: Record<string, number> = {}
  for (const [node, ivs] of Object.entries(byNode)) {
    ivs.sort((a, b) => a.s - b.s)
    let total = 0
    let cs = ivs[0].s, ce = ivs[0].e
    for (let i = 1; i < ivs.length; i++) {
      if (ivs[i].s <= ce) ce = Math.max(ce, ivs[i].e)
      else { total += ce - cs; cs = ivs[i].s; ce = ivs[i].e }
    }
    total += ce - cs
    busyByNode[node] = total
  }
  const nodes = Object.keys(busyByNode).sort()

  // Per-link bytes — prefer actual, fall back to predicted.
  const linkSource = (dag.actualNetworkFlows && dag.actualNetworkFlows.length > 0)
    ? { flows: dag.actualNetworkFlows, kind: 'actual' as const }
    : { flows: dag.predictedNetworkFlows ?? [], kind: 'predicted' as const }
  const byLink: Record<string, { bytes: number; count: number }> = {}
  for (const f of linkSource.flows) {
    const key = `${f.srcNode} → ${f.dstNode}`
    const entry = byLink[key] ?? { bytes: 0, count: 0 }
    entry.bytes += f.dataSize
    entry.count++
    byLink[key] = entry
  }
  const links = Object.entries(byLink).sort((a, b) => b[1].bytes - a[1].bytes)
  const linkMax = links.length ? links[0][1].bytes : 0

  if (nodes.length === 0 && links.length === 0) {
    return <p className="text-on-faint text-sm">No utilization data yet (tasks haven't run).</p>
  }

  return (
    <div className="space-y-8">
      {nodes.length > 0 && (
        <div>
          <div className="text-xs uppercase tracking-wide text-on-faint mb-2">
            Compute utilization per node (busy wall-time / {span.toFixed(1)}s span)
          </div>
          <table className="text-sm w-full">
            <thead>
              <tr className="text-left text-on-muted border-b border-line">
                <th className="pb-2 pr-4">Node</th>
                <th className="pb-2 pr-4 w-1/2">Utilization</th>
                <th className="pb-2 pr-4">Busy</th>
                <th className="pb-2">Idle</th>
              </tr>
            </thead>
            <tbody>
              {nodes.map(n => {
                const busy = busyByNode[n]
                const pct = Math.min(100, (busy / span) * 100)
                return (
                  <tr key={n} className="border-b border-line-soft">
                    <td className="py-2 pr-4 font-mono text-xs">{n}</td>
                    <td className="py-2 pr-4">
                      <div className="w-full bg-surface-alt rounded h-3 overflow-hidden">
                        <div
                          className="h-3 bg-blue-500 dark:bg-blue-400"
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                      <div className="text-xs text-on-faint mt-0.5">{pct.toFixed(1)}%</div>
                    </td>
                    <td className="py-2 pr-4 text-on-muted">{busy.toFixed(2)}s</td>
                    <td className="py-2 text-on-faint">{(span - busy).toFixed(2)}s</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {links.length > 0 && (
        <div>
          <div className="text-xs uppercase tracking-wide text-on-faint mb-2">
            Network traffic per link ({linkSource.kind})
          </div>
          <table className="text-sm w-full">
            <thead>
              <tr className="text-left text-on-muted border-b border-line">
                <th className="pb-2 pr-4">Link</th>
                <th className="pb-2 pr-4 w-1/2">Volume</th>
                <th className="pb-2 pr-4">Bytes</th>
                <th className="pb-2">Flows</th>
              </tr>
            </thead>
            <tbody>
              {links.map(([name, v]) => {
                const pct = linkMax > 0 ? (v.bytes / linkMax) * 100 : 0
                return (
                  <tr key={name} className="border-b border-line-soft">
                    <td className="py-2 pr-4 font-mono text-xs">{name}</td>
                    <td className="py-2 pr-4">
                      <div className="w-full bg-surface-alt rounded h-3 overflow-hidden">
                        <div
                          className="h-3 bg-emerald-500 dark:bg-emerald-400"
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                    </td>
                    <td className="py-2 pr-4 text-on-muted">{fmtBytes(v.bytes)}</td>
                    <td className="py-2 text-on-faint">{v.count}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

import type { ODAGDetail, PredictedNetworkFlow, ActualNetworkFlow } from '@/api/client'

function isDark() { return document.documentElement.classList.contains('dark') }
function rowEven() { return isDark() ? '#0f172a' : '#f9fafb' }
function rowOdd() { return isDark() ? '#111827' : '#f3f4f6' }
function gridStroke() { return isDark() ? '#1f2937' : '#e5e7eb' }
function axisStroke() { return isDark() ? '#374151' : '#d1d5db' }
function labelFill() { return isDark() ? '#9ca3af' : '#6b7280' }
function tickFill() { return isDark() ? '#6b7280' : '#9ca3af' }
function barTextDark() { return isDark() ? '#0f172a' : '#ffffff' }
function legendFill() { return isDark() ? '#6b7280' : '#9ca3af' }
function bandSep() { return isDark() ? '#1f2937' : '#e5e7eb' }

// Red dashed line dividing execution and network bands within each node row.
const EXEC_NET_DIVIDER = '#ef4444'

const TASK_COLORS = [
  '#60a5fa', '#34d399', '#f59e0b', '#f87171',
  '#a78bfa', '#fb923c', '#e879f9', '#2dd4bf',
]

function taskColor(name: string, names: string[]): string {
  return TASK_COLORS[names.indexOf(name) % TASK_COLORS.length]
}

// TEMP: keep this around — used by the commented-out network rendering blocks.
function fmtBytes(bytes: number): string {
  if (bytes >= 1_000_000_000) return `${(bytes / 1_000_000_000).toFixed(1)} GB`
  if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(0)} MB`
  if (bytes >= 1_000) return `${(bytes / 1_000).toFixed(0)} KB`
  return `${bytes} B`
}
void fmtBytes

function maxOverlap(rows: Array<{ s: number; e: number }>): number {
  if (rows.length <= 1) return rows.length
  const events: Array<{ t: number; d: number }> = []
  for (const r of rows) { events.push({ t: r.s, d: 1 }); events.push({ t: r.e, d: -1 }) }
  events.sort((a, b) => a.t - b.t || a.d - b.d)
  let cur = 0, mx = 0
  for (const e of events) { cur += e.d; mx = Math.max(mx, cur) }
  return mx
}

function assignSubRows(items: Array<{ key: string; start: number; end: number }>): Record<string, number> {
  const sorted = [...items].sort((a, b) => a.start - b.start)
  const rowEnds: number[] = []
  const out: Record<string, number> = {}
  for (const it of sorted) {
    let placed = false
    for (let r = 0; r < rowEnds.length; r++) {
      if (it.start >= rowEnds[r]) { rowEnds[r] = it.end; out[it.key] = r; placed = true; break }
    }
    if (!placed) { out[it.key] = rowEnds.length; rowEnds.push(it.end) }
  }
  return out
}

type BandKey = 'execPred' | 'execAct' | 'netPred' | 'netAct'
const BAND_ORDER: BandKey[] = ['execPred', 'execAct', 'netPred', 'netAct']

interface Props {
  dag: ODAGDetail
}

export default function UnifiedGantt({ dag }: Props) {
  const taskNames = dag.spec.tasks.map(t => t.name)

  const predictedTasks = dag.predictedTasks ?? []
  const activeTasks = dag.tasks.filter(t => t.startTime && t.node)
  const refMs = activeTasks.length > 0
    ? Math.min(...activeTasks.map(t => new Date(t.startTime!).getTime()))
    : null
  const actualTaskBars = activeTasks.map(t => ({
    name: t.name,
    node: t.node!,
    start: (new Date(t.startTime!).getTime() - refMs!) / 1000,
    end: t.completionTime
      ? (new Date(t.completionTime).getTime() - refMs!) / 1000
      : (Date.now() - refMs!) / 1000,
  }))

  // TEMP: networking bars disabled — restore by reverting this block + the
  // two commented JSX rendering blocks below + the two commented legend
  // entries + LEGEND_ITEMS = 4.
  const _predictedFlows: PredictedNetworkFlow[] = dag.predictedNetworkFlows ?? []
  const _actualFlows: ActualNetworkFlow[] = dag.actualNetworkFlows ?? []
  void _predictedFlows; void _actualFlows
  const predictedFlows: PredictedNetworkFlow[] = []
  const actualFlows: ActualNetworkFlow[] = []

  // Union of nodes.
  const nodeSet = new Set<string>()
  predictedTasks.forEach(p => nodeSet.add(p.node))
  actualTaskBars.forEach(b => nodeSet.add(b.node))
  predictedFlows.forEach(f => { nodeSet.add(f.srcNode); nodeSet.add(f.dstNode) })
  actualFlows.forEach(f => { nodeSet.add(f.srcNode); nodeSet.add(f.dstNode) })
  const nodes = Array.from(nodeSet).sort()

  if (nodes.length === 0) {
    return <p className="text-on-faint text-sm">No schedule data yet.</p>
  }

  const slots: Record<BandKey, Record<string, number>> = {
    execPred: {}, execAct: {}, netPred: {}, netAct: {},
  }
  const subRows: Record<BandKey, Record<string, Record<string, number>>> = {
    execPred: {}, execAct: {}, netPred: {}, netAct: {},
  }
  for (const node of nodes) {
    const pt = predictedTasks.filter(p => p.node === node)
    const at = actualTaskBars.filter(b => b.node === node)
    slots.execPred[node] = pt.length ? Math.max(maxOverlap(pt.map(p => ({ s: p.estStart, e: p.estEnd }))), 1) : 0
    slots.execAct[node]  = at.length ? Math.max(maxOverlap(at.map(b => ({ s: b.start, e: b.end }))), 1) : 0

    const npOnNode = predictedFlows.flatMap(f => {
      const xs: Array<{ s: number; e: number }> = []
      if (f.srcNode === node) xs.push({ s: f.start, e: f.end })
      if (f.dstNode === node) xs.push({ s: f.start, e: f.end })
      return xs
    })
    const naOnNode = actualFlows.flatMap(f => {
      const xs: Array<{ s: number; e: number }> = []
      if (f.srcNode === node) xs.push({ s: f.start, e: f.end })
      if (f.dstNode === node) xs.push({ s: f.start, e: f.end })
      return xs
    })
    slots.netPred[node] = npOnNode.length ? Math.max(maxOverlap(npOnNode), 1) : 0
    slots.netAct[node]  = naOnNode.length ? Math.max(maxOverlap(naOnNode), 1) : 0

    subRows.execPred[node] = assignSubRows(pt.map(p => ({ key: p.name, start: p.estStart, end: p.estEnd })))
    subRows.execAct[node]  = assignSubRows(at.map(b => ({ key: b.name, start: b.start, end: b.end })))
    subRows.netPred[node]  = assignSubRows(
      predictedFlows.flatMap((f, i) => {
        const out = []
        const key = `p${i}`
        if (f.srcNode === node) out.push({ key: `${key}/src`, start: f.start, end: f.end })
        if (f.dstNode === node) out.push({ key: `${key}/dst`, start: f.start, end: f.end })
        return out
      })
    )
    subRows.netAct[node] = assignSubRows(
      actualFlows.flatMap((f, i) => {
        const out = []
        const key = `a${i}`
        if (f.srcNode === node) out.push({ key: `${key}/src`, start: f.start, end: f.end })
        if (f.dstNode === node) out.push({ key: `${key}/dst`, start: f.start, end: f.end })
        return out
      })
    )
  }

  const maxTime = Math.max(
    1,
    ...actualTaskBars.map(b => b.end),
    ...predictedTasks.map(p => p.estEnd),
    ...predictedFlows.map(f => f.end),
    ...actualFlows.map(f => f.end),
  )

  const ML = 90, MR = 20, MT = 16
  const BAR = 13, BAR_GAP = 2, NODE_PAD = 6, BAND_GAP = 4
  const W = 960
  const innerW = W - ML - MR

  // Legend: vertically stacked swatches below the axis.
  const LEGEND_ROW_H = 16
  const LEGEND_ITEMS = 2 // TEMP: was 4 — networking legend entries commented out below.
  const AXIS_LABEL_H = 22
  const MB = AXIS_LABEL_H + 12 + LEGEND_ITEMS * LEGEND_ROW_H + 10

  function bandsPerNode(node: string): Array<{ key: BandKey; slotCount: number }> {
    const result: Array<{ key: BandKey; slotCount: number }> = []
    for (const key of BAND_ORDER) {
      const s = slots[key][node]
      if (s > 0) result.push({ key, slotCount: s })
    }
    return result
  }

  function nodeHeight(node: string): number {
    const bands = bandsPerNode(node)
    if (bands.length === 0) return NODE_PAD * 2 + BAR
    let h = NODE_PAD * 2
    for (let i = 0; i < bands.length; i++) {
      h += bands[i].slotCount * (BAR + BAR_GAP)
      if (i < bands.length - 1) h += BAND_GAP
    }
    return h
  }

  const nodeYOffset: Record<string, number> = {}
  let running = 0
  for (const node of nodes) {
    nodeYOffset[node] = MT + running
    running += nodeHeight(node)
  }
  const totalInnerH = running
  const totalH = totalInnerH + MT + MB

  function bandY(node: string, which: BandKey): number | null {
    const bands = bandsPerNode(node)
    let y = nodeYOffset[node] + NODE_PAD
    for (const b of bands) {
      if (b.key === which) return y
      y += b.slotCount * (BAR + BAR_GAP) + BAND_GAP
    }
    return null
  }

  const xs = (t: number) => ML + (t / maxTime) * innerW
  const tickCount = 7
  const step = Math.ceil(maxTime / tickCount)
  const ticks: number[] = []
  for (let t = 0; t <= maxTime + step; t += step) ticks.push(Math.round(t))

  return (
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${W} ${totalH}`} width="100%" style={{ display: 'block', fontFamily: 'inherit' }}>
        {/* Row backgrounds */}
        {nodes.map((node, i) => (
          <rect
            key={node}
            x={ML} y={nodeYOffset[node]}
            width={innerW} height={nodeHeight(node)}
            fill={i % 2 === 0 ? rowEven() : rowOdd()}
          />
        ))}

        {/* Row separators between nodes */}
        {nodes.map((node, i) => {
          if (i === 0) return null
          return (
            <line
              key={`sep-${node}`}
              x1={ML} y1={nodeYOffset[node]}
              x2={ML + innerW} y2={nodeYOffset[node]}
              stroke={isDark() ? '#f3f4f6' : '#000000'} strokeWidth={1}
            />
          )
        })}

        {/* Band separators — red dashed between exec and net, faint otherwise */}
        {nodes.map(node => {
          const bands = bandsPerNode(node)
          const lines: JSX.Element[] = []
          let y = nodeYOffset[node] + NODE_PAD
          for (let i = 0; i < bands.length - 1; i++) {
            y += bands[i].slotCount * (BAR + BAR_GAP) + BAND_GAP / 2
            const isExecNetBoundary =
              (bands[i].key.startsWith('exec') && bands[i + 1].key.startsWith('net')) ||
              (bands[i].key.startsWith('net') && bands[i + 1].key.startsWith('exec'))
            lines.push(
              <line
                key={`bs-${node}-${i}`}
                x1={ML} y1={y}
                x2={ML + innerW} y2={y}
                stroke={isExecNetBoundary ? EXEC_NET_DIVIDER : bandSep()}
                strokeWidth={isExecNetBoundary ? 1.2 : 0.5}
                strokeDasharray={isExecNetBoundary ? '6 4' : undefined}
                strokeOpacity={isExecNetBoundary ? 0.75 : 0.3}
              />
            )
            y += BAND_GAP / 2
          }
          return lines
        })}

        {/* Vertical grid */}
        {ticks.filter(t => t <= maxTime).map(t => (
          <line
            key={t}
            x1={xs(t)} y1={MT}
            x2={xs(t)} y2={MT + totalInnerH}
            stroke={gridStroke()} strokeWidth={1}
          />
        ))}

        {/* Node labels */}
        {nodes.map(node => (
          <text
            key={node}
            x={ML - 8}
            y={nodeYOffset[node] + nodeHeight(node) / 2}
            textAnchor="end"
            dominantBaseline="middle"
            fill={labelFill()}
            fontSize={11}
          >
            {node}
          </text>
        ))}

        {/* Exec predicted */}
        {predictedTasks.map(p => {
          const y0 = bandY(p.node, 'execPred'); if (y0 == null) return null
          const color = taskColor(p.name, taskNames)
          const x = xs(p.estStart)
          const w = Math.max(xs(p.estEnd) - xs(p.estStart), 3)
          const sub = subRows.execPred[p.node]?.[p.name] ?? 0
          const y = y0 + sub * (BAR + BAR_GAP)
          return (
            <g key={`ep-${p.name}`}>
              <rect x={x} y={y} width={w} height={BAR} fill={color} fillOpacity={0.18}
                stroke={color} strokeWidth={1.5} strokeDasharray="5 3" rx={2} />
              <text x={x + 4} y={y + BAR / 2} dominantBaseline="middle" fill={color} fillOpacity={0.8} fontSize={9} style={{ pointerEvents: 'none' }}>
                {p.name}
              </text>
              <title>{`${p.name} predicted: ${p.estStart.toFixed(1)}s – ${p.estEnd.toFixed(1)}s (${p.node})`}</title>
            </g>
          )
        })}

        {/* Exec actual */}
        {actualTaskBars.map(b => {
          const y0 = bandY(b.node, 'execAct'); if (y0 == null) return null
          const color = taskColor(b.name, taskNames)
          const x = xs(b.start)
          const w = Math.max(xs(b.end) - xs(b.start), 3)
          const sub = subRows.execAct[b.node]?.[b.name] ?? 0
          const y = y0 + sub * (BAR + BAR_GAP)
          return (
            <g key={`ea-${b.name}`}>
              <rect x={x} y={y} width={w} height={BAR} fill={color} fillOpacity={0.9} rx={2} />
              <text x={x + 4} y={y + BAR / 2} dominantBaseline="middle" fill={barTextDark()} fontWeight="bold" fontSize={9} style={{ pointerEvents: 'none' }}>
                {b.name}
              </text>
              <title>{`${b.name} actual: ${b.start.toFixed(2)}s – ${b.end.toFixed(2)}s (${b.node})`}</title>
            </g>
          )
        })}

        {/* TEMP: networking bars disabled. Restore the two blocks below to bring them back. */}
        {/*
        {predictedFlows.flatMap((f, i) => {
          const color = taskColor(f.fromTask, taskNames)
          const x = xs(f.start)
          const w = Math.max(xs(f.end) - xs(f.start), 3)
          const key = `p${i}`
          const render = (role: 'src' | 'dst', node: string): JSX.Element | null => {
            const y0 = bandY(node, 'netPred'); if (y0 == null) return null
            const sub = subRows.netPred[node]?.[`${key}/${role}`] ?? 0
            const y = y0 + sub * (BAR + BAR_GAP)
            const isSrc = role === 'src'
            return (
              <g key={`np-${i}-${role}`}>
                <rect x={x} y={y} width={w} height={BAR}
                  fill={color} fillOpacity={isSrc ? 0.15 : 0}
                  stroke={color} strokeWidth={1.2} strokeDasharray="5 3" rx={2} />
                <title>{`predicted ${isSrc ? 'egress' : 'ingress'} on ${node}: ${f.fromTask}→${f.toTask}\n${fmtBytes(f.dataSize)}\n${f.start.toFixed(2)}s – ${f.end.toFixed(2)}s`}</title>
              </g>
            )
          }
          return [render('src', f.srcNode), render('dst', f.dstNode)].filter(Boolean) as JSX.Element[]
        })}

        {actualFlows.flatMap((f, i) => {
          const color = f.ok ? taskColor(f.fromTask, taskNames) : '#ef4444'
          const x = xs(f.start)
          const w = Math.max(xs(f.end) - xs(f.start), 3)
          const key = `a${i}`
          const label = `${f.srcNode}→${f.dstNode} · ${f.fromTask}→${f.toTask}`
          const showInline = w > 80
          const render = (role: 'src' | 'dst', node: string): JSX.Element | null => {
            const y0 = bandY(node, 'netAct'); if (y0 == null) return null
            const sub = subRows.netAct[node]?.[`${key}/${role}`] ?? 0
            const y = y0 + sub * (BAR + BAR_GAP)
            const isSrc = role === 'src'
            return (
              <g key={`na-${i}-${role}`}>
                <rect x={x} y={y} width={w} height={BAR}
                  fill={color}
                  fillOpacity={isSrc ? 0.9 : 0.35}
                  stroke={color}
                  strokeWidth={isSrc ? 0 : 1}
                  rx={2} />
                {showInline && isSrc && (
                  <text x={x + 4} y={y + BAR / 2} dominantBaseline="middle" fill={barTextDark()} fontWeight="bold" fontSize={9} style={{ pointerEvents: 'none' }}>
                    {label}
                  </text>
                )}
                <title>{`actual ${isSrc ? 'egress' : 'ingress'} on ${node}: ${f.fromTask}→${f.toTask}\n${fmtBytes(f.dataSize)}\n${f.start.toFixed(2)}s – ${f.end.toFixed(2)}s${f.ok ? '' : '\nFAILED'}`}</title>
              </g>
            )
          }
          return [render('src', f.srcNode), render('dst', f.dstNode)].filter(Boolean) as JSX.Element[]
        })}
        */}

        {/* X axis */}
        <line x1={ML} y1={MT + totalInnerH} x2={ML + innerW} y2={MT + totalInnerH} stroke={axisStroke()} strokeWidth={1} />
        {ticks.filter(t => t <= maxTime + 1e-6).map(t => (
          <g key={`xtick-${t}`}>
            <line x1={xs(t)} y1={MT + totalInnerH} x2={xs(t)} y2={MT + totalInnerH + 5} stroke={axisStroke()} />
            <text x={xs(t)} y={MT + totalInnerH + 16} textAnchor="middle" fill={tickFill()} fontSize={10}>{t}s</text>
          </g>
        ))}

        {/* Legend — vertically stacked */}
        <g transform={`translate(${ML}, ${MT + totalInnerH + AXIS_LABEL_H + 8})`}>
          <g transform="translate(0, 0)">
            <rect x={0} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.18} stroke="#9ca3af" strokeWidth={1.2} strokeDasharray="5 3" rx={1} />
            <text x={20} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Exec predicted (dashed outline, light fill)</text>
          </g>
          <g transform={`translate(0, ${LEGEND_ROW_H})`}>
            <rect x={0} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.9} rx={1} />
            <text x={20} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Exec actual (solid fill)</text>
          </g>
          {/* TEMP: networking legend entries disabled. Restore below to bring them back. */}
          {/*
          <g transform={`translate(0, ${LEGEND_ROW_H * 2})`}>
            <rect x={0} y={0} width={14} height={10} fill="transparent" stroke="#9ca3af" strokeWidth={1.2} strokeDasharray="5 3" rx={1} />
            <text x={20} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Net predicted (dashed outline — src light fill, dst hollow)</text>
          </g>
          <g transform={`translate(0, ${LEGEND_ROW_H * 3})`}>
            <rect x={0} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.9} rx={1} />
            <rect x={18} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.35} stroke="#9ca3af" strokeWidth={1} rx={1} />
            <text x={38} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Net actual (src solid / dst faded+outlined)</text>
          </g>
          */}
        </g>
      </svg>
    </div>
  )
}
